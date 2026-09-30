"""
Assignment 3 — Flask API bridge for Grafana
Queries MongoDB Atlas and serves aggregated data as JSON
Run: python api.py
"""
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient

MONGO_URI = "mongodb+srv://341027_db_user:HSTBbXUaPe1SufH6@cluster0.91u4wwb.mongodb.net/?appName=Cluster0"
DB_NAME = "fmcg_streaming"

app = Flask(__name__)
CORS(app)
client = MongoClient(MONGO_URI)
db = client[DB_NAME]


@app.route("/sales/by-category")
def sales_by_category():
    pipeline = [
        {"$group": {"_id": "$product_category", "total_revenue": {"$sum": "$total_price"}, "count": {"$sum": 1}}},
        {"$sort": {"total_revenue": -1}}
    ]
    result = list(db["sales"].aggregate(pipeline))
    return jsonify([{"category": r["_id"], "total_revenue": round(r["total_revenue"], 2), "count": r["count"]} for r in result])


@app.route("/orders/delivery-performance")
def delivery_performance():
    pipeline = [
        {"$group": {
            "_id": {"zone": "$delivery_zone", "on_time": "$on_time"},
            "count": {"$sum": 1}
        }}
    ]
    result = list(db["orders"].aggregate(pipeline))
    return jsonify([{"zone": r["_id"]["zone"], "on_time": r["_id"]["on_time"], "count": r["count"]} for r in result])


@app.route("/inventory/restock-alerts")
def restock_alerts():
    pipeline = [
        {"$group": {
            "_id": {"warehouse": "$warehouse_id", "restock": "$restock_needed"},
            "count": {"$sum": 1},
            "avg_available": {"$avg": "$qty_available"}
        }}
    ]
    result = list(db["inventory"].aggregate(pipeline))
    return jsonify([{
        "warehouse": r["_id"]["warehouse"],
        "restock_needed": r["_id"]["restock"],
        "count": r["count"],
        "avg_qty_available": round(r["avg_available"], 1)
    } for r in result])


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
