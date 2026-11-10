import json
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, min, max, max_by, min_by,
    concat_ws, stddev, when, udf,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.appName("CryptoTrain").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", LongType()),
])

# Legge TUTTA la storia da Kafka (modalità batch)
raw = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "crypto-prices")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))
)

stats = (
    parsed.groupBy(window("event_time", "30 seconds"), "symbol")
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        max_by("price", "timestamp").alias("last_price"),
        min_by("price", "timestamp").alias("first_price"),
    )
    .withColumn("volatility_pct", (col("max_price") - col("min_price")) / col("avg_price") * 100)
    .withColumn("drift_pct", (col("last_price") - col("first_price")) / col("avg_price") * 100)
    .withColumn("window_start", col("window.start"))
)

print("Finestre usate per l'addestramento:", stats.count())

pipeline = Pipeline(stages=[
    VectorAssembler(inputCols=["volatility_pct", "drift_pct"], outputCol="raw"),
    StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True),
    KMeans(k=3, seed=42, featuresCol="features"),
])
model = pipeline.fit(stats)

centers = model.stages[-1].clusterCenters()


def dist(features, cluster):
    c = centers[cluster]
    return float(math.sqrt(sum((float(a) - float(b)) ** 2 for a, b in zip(features, c))))


dist_udf = udf(dist, DoubleType())

scored = model.transform(stats).withColumn("anomaly_score", dist_udf(col("features"), col("prediction")))

s = scored.agg(avg("anomaly_score").alias("m"), stddev("anomaly_score").alias("s")).first()
threshold = float(s["m"] + 2 * s["s"])
print("Soglia anomalia:", threshold)

# Salva modello e soglia nella cartella del progetto
model.write().overwrite().save("/app/model")
with open("/app/model_threshold.json", "w") as f:
    json.dump({"threshold": threshold}, f)
print("Modello salvato in /app/model")

# Riempie anche lo storico delle anomalie in Elasticsearch
out = (
    scored.withColumn("is_anomaly", when(col("anomaly_score") > threshold, 1).otherwise(0))
    .withColumn("doc_id", concat_ws("_", col("symbol"), col("window_start").cast("string")))
    .select("doc_id", "symbol", "window_start", "avg_price", "volatility_pct",
            "drift_pct", "anomaly_score", "is_anomaly")
)
print("Anomalie trovate nello storico:", out.filter(col("is_anomaly") == 1).count())

(
    out.write.format("org.elasticsearch.spark.sql")
    .option("es.nodes", "elasticsearch")
    .option("es.port", "9200")
    .option("es.nodes.wan.only", "true")
    .option("es.mapping.id", "doc_id")
    .option("es.write.operation", "upsert")
    .mode("append")
    .save("crypto-anomalies")
)
print("Fatto: storico scritto in crypto-anomalies")