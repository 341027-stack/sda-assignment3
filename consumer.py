"""
Assignment 3 — Kafka Consumer
Reads from three Kafka topics and writes to MongoDB Atlas:
  raw.sales      → fmcg_streaming.sales
  raw.orders     → fmcg_streaming.orders
  raw.inventory  → fmcg_streaming.inventory

Usage:
  python consumer.py

Prerequisites:
  pip install kafka-python pymongo
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ── Configuration — FILL IN YOUR PASSWORD BELOW ──────────────────────────────
MONGO_URI = "mongodb+srv://341027_db_user:uU0tucCEm69Wg9jG@cluster0.91u4wwb.mongodb.net/?appName=Cluster0"
DB_NAME   = "fmcg_streaming"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = {
    "raw.sales":     "sales",
    "raw.orders":    "orders",
    "raw.inventory": "inventory",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("consumer")


# ── MongoDB connection ────────────────────────────────────────────────────────
def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        log.info("✅ Connected to MongoDB Atlas — database: %s", DB_NAME)
    except ConnectionFailure as e:
        log.error("❌ MongoDB connection failed: %s", e)
        log.error("Check your password in MONGO_URI and try again.")
        raise
    return client[DB_NAME]


# ── Consumer thread ───────────────────────────────────────────────────────────
def consume_topic(topic: str, collection_name: str, db) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"consumer-{collection_name}",
    )
    collection = db[collection_name]
    log.info("Listening on %-16s → MongoDB collection: %s", topic, collection_name)

    for message in consumer:
        record = message.value
        record["consumed_at"] = datetime.now(timezone.utc).isoformat()
        collection.insert_one(record)
        log.info("← %-16s  saved to %-12s  offset=%d",
                 topic, collection_name, message.offset)


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    db = get_db()

    threads = []
    for topic, collection_name in TOPICS.items():
        t = threading.Thread(
            target=consume_topic,
            args=(topic, collection_name, db),
            name=collection_name,
            daemon=True,
        )
        t.start()
        threads.append(t)

    log.info("Consumer running — press Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping consumer.")


if __name__ == "__main__":
    main()
