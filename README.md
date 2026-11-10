\# Crypto Stream Pipeline



Progetto conclusivo del corso \*\*Technologies for Advanced Programming\*\* (A.A. 2025-2026).



Pipeline di stream processing che acquisisce in tempo reale i prezzi di Bitcoin, Ethereum e Solana da Binance, li elabora con Spark, rileva le anomalie con un modello di Machine Learning e mostra tutto in una dashboard Kibana.



\## Architettura



```

Binance API -> producer.py -> Kafka -> Spark Structured Streaming -> Elasticsearch -> Kibana

&#x20;                                             |

&#x20;                                 modello KMeans (Spark MLlib)

```



| Componente | Ruolo |

|---|---|

| `producer.py` | Ingestion: ogni 2 secondi legge i prezzi da Binance e li invia al topic Kafka `crypto-prices` |

| Apache Kafka | Coda di messaggi che separa chi produce i dati da chi li elabora |

| `spark\_job.py` | Spark Structured Streaming: finestre da 30 secondi per ogni crypto (media, minimo, massimo, volatilità, deriva) e applicazione del modello ML |

| `ml\_train.py` | Addestra il modello KMeans sullo storico letto da Kafka, salva modello e soglia |

| Elasticsearch | Indici `crypto-stats` (statistiche) e `crypto-anomalies` (anomalie) |

| Kibana | Dashboard con i prezzi e le anomalie |



\## Machine Learning



Per ogni finestra da 30 secondi si calcolano due caratteristiche:



\- \*\*volatilità %\*\*: (massimo - minimo) / media

\- \*\*deriva %\*\*: (ultimo prezzo - primo prezzo) / media



`ml\_train.py` addestra un modello \*\*KMeans\*\* (Spark MLlib, k=3) su tutto lo storico. Una finestra è \*\*anomala\*\* se la distanza dal centro del suo cluster supera la soglia `media + 2 x deviazione standard`. Il modello viene addestrato in batch, poi applicato in streaming a ogni nuova finestra da `spark\_job.py`.



\## Requisiti



\- Docker Desktop

\- Python 3.12 con le librerie `kafka-python-ng` e `requests`



```

pip install kafka-python-ng requests

```



\## Come avviare il progetto



1\. \*\*Avviare Kafka, Elasticsearch e Kibana\*\*



```

docker compose up -d

```



2\. \*\*Creare il topic Kafka\*\* (solo la prima volta)



```

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic crypto-prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

```



3\. \*\*Creare l'immagine Spark con numpy\*\* (solo la prima volta)



```

docker build -t spark-ml -f Dockerfile.spark .

```



4\. \*\*Creare gli indici Elasticsearch con i tipi corretti\*\* (solo la prima volta)



```

curl.exe -X PUT http://localhost:9200/crypto-stats -H "Content-Type: application/json" -d "@mapping.json"

curl.exe -X PUT http://localhost:9200/crypto-anomalies -H "Content-Type: application/json" -d "@mapping\_anomalies.json"

```



5\. \*\*Avviare il producer\*\* (lasciarlo acceso per raccogliere dati)



```

python producer.py

```



6\. \*\*Addestrare il modello\*\* (dopo aver raccolto un po' di dati, almeno qualche decina di minuti)



```

docker run --rm --network crypto-pipeline\_default -v "${PWD}:/app" spark-ml /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30\_2.12:8.15.3 /app/ml\_train.py

```



7\. \*\*Avviare Spark in streaming\*\* (lasciarlo acceso)



```

docker run --rm --network crypto-pipeline\_default -v "${PWD}:/app" spark-ml /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30\_2.12:8.15.3 /app/spark\_job.py

```



8\. \*\*Kibana\*\*: aprire http://localhost:5601, creare le data view `crypto-stats` e `crypto-anomalies` (campo temporale `window\_start`) e costruire i grafici con Lens.



Nota: il nome della rete Docker (`crypto-pipeline\_default`) dipende dal nome della cartella del progetto.



\## Per fermare tutto



```

docker compose stop

```



Non usare `docker compose down`, che elimina i container e i dati.

