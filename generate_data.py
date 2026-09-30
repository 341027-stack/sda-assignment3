"""
Assignment 2 - Sample Data Generator
Generates three sample datasets matching the A1 pipeline schemas:
  1. data/pos_sales.csv         — Retail POS transactions
  2. data/qcom_orders.json      — Quick-commerce order events
  3. data/wms_inventory.json    — Warehouse inventory / pick events
Run: python generate_data.py
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
Path("data").mkdir(exist_ok=True)

# ── Shared reference data ────────────────────────────────────────────────────
CATEGORIES   = ["beverages", "snacks", "dairy", "personal_care", "home_goods", "frozen"]
SKUS         = [f"SKU-{i:04d}" for i in range(1, 51)]          # 50 products
STORES       = [f"STORE-{i:02d}" for i in range(1, 6)]         # 5 stores
WAREHOUSES   = ["WH-NORTH", "WH-SOUTH", "WH-EAST"]
ZONES        = ["zone-A", "zone-B", "zone-C", "zone-D", "zone-E"]
RIDERS       = [f"RIDER-{i:03d}" for i in range(1, 21)]

start_ts = datetime(2026, 8, 25, 8, 0, 0)


# ── 1. POS Sales CSV (500 rows) ──────────────────────────────────────────────
pos_rows = []
for i in range(1, 501):
    ts = start_ts + timedelta(seconds=random.randint(0, 86400))
    pos_rows.append({
        "transaction_id": f"TXN-{i:05d}",
        "timestamp":      ts.isoformat(),
        "store_id":       random.choice(STORES),
        "customer_age":   random.randint(18, 72),
        "customer_gender": random.choice(["M", "F", "Other"]),
        "product_category": random.choice(CATEGORIES),
        "sku":            random.choice(SKUS),
        "quantity":       random.randint(1, 8),
        "unit_price":     round(random.uniform(0.99, 24.99), 2),
        "total_price":    None,   # filled below
    })
    row = pos_rows[-1]
    row["total_price"] = round(row["quantity"] * row["unit_price"], 2)

with open("data/pos_sales.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=pos_rows[0].keys())
    writer.writeheader()
    writer.writerows(pos_rows)
print(f"✅  data/pos_sales.csv          — {len(pos_rows)} rows")


# ── 2. Quick-commerce Orders JSON (200 events) ───────────────────────────────
qcom_events = []
for i in range(1, 201):
    ts = start_ts + timedelta(seconds=random.randint(0, 86400))
    promised = random.randint(20, 45)          # minutes
    actual   = promised + random.randint(-5, 15)
    items = [
        {"sku": random.choice(SKUS), "qty": random.randint(1, 4),
         "price": round(random.uniform(0.99, 19.99), 2)}
        for _ in range(random.randint(1, 5))
    ]
    basket_value = round(sum(it["qty"] * it["price"] for it in items), 2)
    qcom_events.append({
        "order_id":             str(uuid.uuid4()),
        "timestamp":            ts.isoformat(),
        "items":                items,
        "basket_value":         basket_value,
        "delivery_zone":        random.choice(ZONES),
        "rider_id":             random.choice(RIDERS),
        "promised_delivery_min": promised,
        "actual_delivery_min":   actual,
        "on_time":              actual <= promised,
    })

with open("data/qcom_orders.json", "w") as f:
    json.dump(qcom_events, f, indent=2)
print(f"✅  data/qcom_orders.json       — {len(qcom_events)} events")


# ── 3. Warehouse Inventory JSON (100 events) ─────────────────────────────────
wms_events = []
for i in range(1, 101):
    ts = start_ts + timedelta(seconds=random.randint(0, 86400))
    qty_on_hand  = random.randint(0, 500)
    qty_reserved = random.randint(0, min(qty_on_hand, 100))
    wms_events.append({
        "event_id":        str(uuid.uuid4()),
        "timestamp":       ts.isoformat(),
        "warehouse_id":    random.choice(WAREHOUSES),
        "sku":             random.choice(SKUS),
        "event_type":      random.choice(["stock_update", "pick", "restock_trigger"]),
        "qty_on_hand":     qty_on_hand,
        "qty_reserved":    qty_reserved,
        "qty_available":   qty_on_hand - qty_reserved,
        "restock_needed":  (qty_on_hand - qty_reserved) < 50,
    })

with open("data/wms_inventory.json", "w") as f:
    json.dump(wms_events, f, indent=2)
print(f"✅  data/wms_inventory.json     — {len(wms_events)} events")

print("\nAll sample data written to ./data/")
