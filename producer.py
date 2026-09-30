"""
Assignment 2 — Kafka Producer
Streams three FMCG data feeds to separate Kafka topics:
  raw.sales      ← data/pos_sales.csv
  raw.orders     ← data/qcom_orders.json
  raw.inventory  ← data/wms_inventory.json

Usage:
  python producer.py                    # runs all three feeds concurrently
  python producer.py --feed sales       # run only the POS feed
  python producer.py --feed orders      # run only the quick-commerce feed
  python producer.py --feed inventory   # run only the warehouse feed
  python producer.py --delay 0.5        # set inter-message delay in seconds (default: 0.2)

Prerequisites:
  pip install kafka-python
  Kafka broker running at localhost:9092 (see docker-compose.yml)
"""

import argparse
import csv
import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Configuration ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = {
    "sales":     "raw.sales",
    "orders":    "raw.orders",
    "inventory": "raw.inventory",
}
DATA_FILES = {
    "sales":     "data/pos_sales.csv",
    "orders":    "data/qcom_orders.json",
    "inventory": "data/wms_inventory.json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")


# ── Producer factory ──────────────────────────────────────────────────────────
def make_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=5,
    )
    log.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
    return producer


# ── Feed functions ────────────────────────────────────────────────────────────
def stream_sales(producer: KafkaProducer, delay: float) -> None:
    """Stream POS sales CSV rows to raw.sales, cycling through the file."""
    topic = TOPICS["sales"]
    path  = Path(DATA_FILES["sales"])
    cycle = 0
    while True:
        cycle += 1
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # cast numeric fields
                row["customer_age"] = int(row["customer_age"])
                row["quantity"]     = int(row["quantity"])
                row["unit_price"]   = float(row["unit_price"])
                row["total_price"]  = float(row["total_price"])
                # stamp ingestion time (event time stays in row["timestamp"])
                row["ingested_at"]  = datetime.now(timezone.utc).isoformat()
                producer.send(topic, value=row)
                log.info("→ %-16s  txn=%-12s  cat=%-15s  £%.2f",
                         topic, row["transaction_id"],
                         row["product_category"], row["total_price"])
                time.sleep(delay)
        log.info("Sales file cycled (pass %d) — replaying from start", cycle)


def stream_orders(producer: KafkaProducer, delay: float) -> None:
    """Stream quick-commerce order events to raw.orders, cycling."""
    topic  = TOPICS["orders"]
    events = json.loads(Path(DATA_FILES["orders"]).read_text())
    cycle  = 0
    while True:
        cycle += 1
        for event in events:
            event["ingested_at"] = datetime.now(timezone.utc).isoformat()
            producer.send(topic, value=event)
            on_time = "✓" if event["on_time"] else "✗"
            log.info("→ %-16s  order=%-36s  zone=%-8s  £%.2f  %s",
                     topic, event["order_id"],
                     event["delivery_zone"], event["basket_value"], on_time)
            time.sleep(delay)
        log.info("Orders file cycled (pass %d) — replaying from start", cycle)


def stream_inventory(producer: KafkaProducer, delay: float) -> None:
    """Stream warehouse inventory events to raw.inventory, cycling."""
    topic  = TOPICS["inventory"]
    events = json.loads(Path(DATA_FILES["inventory"]).read_text())
    cycle  = 0
    while True:
        cycle += 1
        for event in events:
            event["ingested_at"] = datetime.now(timezone.utc).isoformat()
            producer.send(topic, value=event)
            flag = "⚠ RESTOCK" if event["restock_needed"] else "ok"
            log.info("→ %-16s  sku=%-10s  wh=%-10s  avail=%-5d  %s",
                     topic, event["sku"], event["warehouse_id"],
                     event["qty_available"], flag)
            time.sleep(delay)
        log.info("Inventory file cycled (pass %d) — replaying from start", cycle)


# ── Entry point ───────────────────────────────────────────────────────────────
FEED_FUNCS = {
    "sales":     stream_sales,
    "orders":    stream_orders,
    "inventory": stream_inventory,
}

def main():
    parser = argparse.ArgumentParser(description="FMCG Kafka Producer — Assignment 2")
    parser.add_argument("--feed",  choices=list(FEED_FUNCS), default=None,
                        help="Run a single feed only (default: all three)")
    parser.add_argument("--delay", type=float, default=0.2,
                        help="Seconds between messages per feed (default: 0.2)")
    args = parser.parse_args()

    producer = make_producer()

    feeds_to_run = [args.feed] if args.feed else list(FEED_FUNCS)
    log.info("Starting %d feed(s): %s  (delay=%.2fs each)",
             len(feeds_to_run), feeds_to_run, args.delay)
    log.info("Press Ctrl+C to stop.\n")

    threads = []
    for name in feeds_to_run:
        t = threading.Thread(
            target=FEED_FUNCS[name],
            args=(producer, args.delay),
            name=name,
            daemon=True,
        )
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Interrupted — flushing and closing producer…")
    finally:
        producer.flush()
        producer.close()
        log.info("Done.")


if __name__ == "__main__":
    main()
