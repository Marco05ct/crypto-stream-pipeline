import json
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, min, max, max_by, min_by,
    concat_ws, when, udf,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName("CryptoStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Modello addestrato da ml_train.py e relativa soglia
model = PipelineModel.load("/app/model")
centers = model.stages[-1].clusterCenters()
with open("/app/model_threshold.json") as f:
    threshold = json.load(f)["threshold"]


def dist(features, cluster):
    c = centers[cluster]
    return float(math.sqrt(sum((float(a) - float(b)) ** 2 for a, b in zip(features, c))))


dist_udf = udf(dist, DoubleType())

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", LongType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "crypto-prices")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))
)

stats = (
    parsed.withWatermark("event_time", "30 seconds")
    .groupBy(window("event_time", "30 seconds"), "symbol")
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        max_by("price", "timestamp").alias("last_price"),
        min_by("price", "timestamp").alias("first_price"),
    )
)


def es_write(df, index):
    (
        df.write.format("org.elasticsearch.spark.sql")
        .option("es.nodes", "elasticsearch")
        .option("es.port", "9200")
        .option("es.nodes.wan.only", "true")
        .option("es.mapping.id", "doc_id")
        .option("es.write.operation", "upsert")
        .mode("append")
        .save(index)
    )


def process_batch(df, batch_id):
    base = (
        df.withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .withColumn("doc_id", concat_ws("_", col("symbol"), col("window_start").cast("string")))
        .withColumn("volatility_pct", (col("max_price") - col("min_price")) / col("avg_price") * 100)
        .withColumn("drift_pct", (col("last_price") - col("first_price")) / col("avg_price") * 100)
        .persist()
    )

    # 1) statistiche per i grafici dei prezzi
    es_write(
        base.select("doc_id", "symbol", "window_start", "window_end",
                    "avg_price", "min_price", "max_price"),
        "crypto-stats",
    )

    # 2) anomalie calcolate dal modello di Machine Learning
    scored = model.transform(base).withColumn(
        "anomaly_score", dist_udf(col("features"), col("prediction"))
    )
    es_write(
        scored.withColumn("is_anomaly", when(col("anomaly_score") > threshold, 1).otherwise(0))
        .select("doc_id", "symbol", "window_start", "avg_price", "volatility_pct",
                "drift_pct", "anomaly_score", "is_anomaly"),
        "crypto-anomalies",
    )
    base.unpersist()


query = (
    stats.writeStream.outputMode("update")
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/checkpoint")
    .start()
)
query.awaitTermination()