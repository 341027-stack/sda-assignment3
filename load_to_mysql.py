"""
Assignment 3 — MongoDB to MySQL loader for Grafana
Pulls aggregated data from MongoDB Atlas and loads into MySQL
so Grafana can chart it using the MySQL data source.

Run: python load_to_mysql.py
"""

import mysql.connector
from pymongo import MongoClient

# ── MongoDB Atlas config ───────────────────────────────────────────────────────
MONGO_URI = "mongodb+srv://341027_db_user:HSTBbXUaPe1SufH6@cluster0.91u4wwb.mongodb.net/?appName=Cluster0"
DB_NAME   = "fmcg_streaming"

# ── MySQL config ───────────────────────────────────────────────────────────────
MYSQL_HOST     = "localhost"
MYSQL_PORT     = 3306
MYSQL_USER     = "root"
MYSQL_PASSWORD = "root"
MYSQL_DB       = "fmcg_grafana"


def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]


def get_mysql_conn():
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
    cursor.execute(f"USE {MYSQL_DB}")
    return conn, cursor


def load_sales_by_category(mongo_db, cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_by_category (
            category VARCHAR(50) PRIMARY KEY,
            total_revenue FLOAT,
            tx_count INT
        )
    """)
    cursor.execute("DELETE FROM sales_by_category")

    pipeline = [
        {"$group": {
            "_id": "$product_category",
            "total_revenue": {"$sum": "$total_price"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"total_revenue": -1}}
    ]
    rows = list(mongo_db["sales"].aggregate(pipeline))
    for r in rows:
        cursor.execute(
            "INSERT INTO sales_by_category VALUES (%s, %s, %s)",
            (r["_id"], round(r["total_revenue"], 2), r["count"])
        )
    print(f"✅ sales_by_category — {len(rows)} rows loaded")


def load_delivery_performance(mongo_db, cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS delivery_performance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            zone VARCHAR(20),
            on_time TINYINT(1),
            order_count INT
        )
    """)
    cursor.execute("DELETE FROM delivery_performance")

    pipeline = [
        {"$group": {
            "_id": {"zone": "$delivery_zone", "on_time": "$on_time"},
            "count": {"$sum": 1}
        }}
    ]
    rows = list(mongo_db["orders"].aggregate(pipeline))
    for r in rows:
        cursor.execute(
            "INSERT INTO delivery_performance (zone, on_time, order_count) VALUES (%s, %s, %s)",
            (r["_id"]["zone"], int(r["_id"]["on_time"]), r["count"])
        )
    print(f"✅ delivery_performance — {len(rows)} rows loaded")


def load_restock_alerts(mongo_db, cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS restock_alerts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            warehouse VARCHAR(20),
            restock_needed TINYINT(1),
            item_count INT,
            avg_qty_available FLOAT
        )
    """)
    cursor.execute("DELETE FROM restock_alerts")

    pipeline = [
        {"$group": {
            "_id": {"warehouse": "$warehouse_id", "restock": "$restock_needed"},
            "count": {"$sum": 1},
            "avg_available": {"$avg": "$qty_available"}
        }}
    ]
    rows = list(mongo_db["inventory"].aggregate(pipeline))
    for r in rows:
        cursor.execute(
            "INSERT INTO restock_alerts (warehouse, restock_needed, item_count, avg_qty_available) VALUES (%s, %s, %s, %s)",
            (r["_id"]["warehouse"], int(r["_id"]["restock"]), r["count"], round(r["avg_available"], 1))
        )
    print(f"✅ restock_alerts — {len(rows)} rows loaded")


def main():
    print("Connecting to MongoDB Atlas...")
    mongo_db = get_mongo_db()

    print("Connecting to MySQL...")
    conn, cursor = get_mysql_conn()

    load_sales_by_category(mongo_db, cursor)
    load_delivery_performance(mongo_db, cursor)
    load_restock_alerts(mongo_db, cursor)

    conn.commit()
    cursor.close()
    conn.close()
    print("\n✅ All data loaded into MySQL database: fmcg_grafana")
    print("Now go to Grafana and create your charts!")


if __name__ == "__main__":
    main()
