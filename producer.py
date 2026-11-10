import json
import time
import requests
from kafka import KafkaProducer

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
TOPIC = "crypto-prices"
URL = "https://api.binance.com/api/v3/ticker/price"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Producer avviato. Premi Ctrl+C per fermarlo.")

while True:
    for symbol in SYMBOLS:
        try:
            r = requests.get(URL, params={"symbol": symbol}, timeout=5)
            price = float(r.json()["price"])
            message = {
                "symbol": symbol,
                "price": price,
                "timestamp": int(time.time() * 1000),
            }
            producer.send(TOPIC, message)
            print("Inviato:", message)
        except Exception as e:
            print("Errore:", e)
    time.sleep(2)