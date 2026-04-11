"""
POS Transaction Generator
Produces realistic JSON POS events to a Kafka topic at a configurable TPS.

Usage:
    python pos_generator.py [--tps 10]

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS  e.g. kafka:9092
    KAFKA_TOPIC              e.g. raw_pos_transactions
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

# ---------------------------------------------------------------------------
# Product catalog — ~20 SKUs across realistic grocery/retail categories
# ---------------------------------------------------------------------------
PRODUCT_CATALOG = [
    {"sku": "SKU-10001", "name": "Organic Whole Milk 1L",        "category": "Dairy",    "unit_price": 4.99},
    {"sku": "SKU-10002", "name": "Free Range Eggs 12pk",         "category": "Dairy",    "unit_price": 5.49},
    {"sku": "SKU-10003", "name": "Sourdough Bread Loaf",         "category": "Bakery",   "unit_price": 3.99},
    {"sku": "SKU-10004", "name": "Butter Croissant",             "category": "Bakery",   "unit_price": 2.49},
    {"sku": "SKU-10005", "name": "Atlantic Salmon Fillet 400g",  "category": "Seafood",  "unit_price": 12.99},
    {"sku": "SKU-10006", "name": "Chicken Breast 500g",          "category": "Meat",     "unit_price": 8.99},
    {"sku": "SKU-10007", "name": "Beef Mince 500g",              "category": "Meat",     "unit_price": 7.49},
    {"sku": "SKU-10008", "name": "Baby Spinach 200g",            "category": "Produce",  "unit_price": 3.29},
    {"sku": "SKU-10009", "name": "Cherry Tomatoes 250g",         "category": "Produce",  "unit_price": 2.99},
    {"sku": "SKU-10010", "name": "Bananas 1kg",                  "category": "Produce",  "unit_price": 1.99},
    {"sku": "SKU-10011", "name": "Avocado Each",                 "category": "Produce",  "unit_price": 1.79},
    {"sku": "SKU-10012", "name": "Orange Juice 1L",              "category": "Beverages","unit_price": 4.29},
    {"sku": "SKU-10013", "name": "Sparkling Water 6pk",          "category": "Beverages","unit_price": 5.99},
    {"sku": "SKU-10014", "name": "Arabica Coffee Beans 250g",    "category": "Beverages","unit_price": 9.99},
    {"sku": "SKU-10015", "name": "Greek Yogurt 500g",            "category": "Dairy",    "unit_price": 4.49},
    {"sku": "SKU-10016", "name": "Cheddar Cheese 400g",          "category": "Dairy",    "unit_price": 6.99},
    {"sku": "SKU-10017", "name": "Pasta Penne 500g",             "category": "Pantry",   "unit_price": 2.19},
    {"sku": "SKU-10018", "name": "Olive Oil Extra Virgin 500ml", "category": "Pantry",   "unit_price": 8.49},
    {"sku": "SKU-10019", "name": "Dark Chocolate 100g",          "category": "Snacks",   "unit_price": 3.49},
    {"sku": "SKU-10020", "name": "Mixed Nuts 200g",              "category": "Snacks",   "unit_price": 5.79},
]

STORES = ["STORE-001", "STORE-002", "STORE-003"]
REGISTERS = ["REG-01", "REG-02", "REG-03", "REG-04"]
PAYMENT_METHODS = ["credit_card", "debit_card", "cash", "mobile_pay"]
PAYMENT_STATUSES = ["completed", "failed", "refunded"]
PAYMENT_STATUS_WEIGHTS = [90, 5, 5]

TAX_RATE = 0.07  # 7% flat tax

fake = Faker()
Faker.seed(42)  # Reproducible for demo consistency


def build_transaction() -> dict:
    """Generate one realistic POS transaction payload."""
    num_items = random.randint(1, 5)
    items = []
    for product in random.sample(PRODUCT_CATALOG, num_items):
        quantity = random.randint(1, 4)
        items.append({
            "sku": product["sku"],
            "name": product["name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": product["unit_price"],
        })

    subtotal = round(sum(i["unit_price"] * i["quantity"] for i in items), 2)
    tax = round(subtotal * TAX_RATE, 2)
    total = round(subtotal + tax, 2)

    customer_id = (
        f"CUST-{random.randint(1000, 9999)}"
        if random.random() < 0.70
        else None
    )

    return {
        "transaction_id": str(uuid.uuid4()),
        "store_id": random.choice(STORES),
        "register_id": random.choice(REGISTERS),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "items": items,
        "subtotal": subtotal,
        "tax": tax,
        "total": total,
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": random.choices(PAYMENT_STATUSES, weights=PAYMENT_STATUS_WEIGHTS)[0],
        "customer_id": customer_id,
    }


def delivery_report(err, msg):
    """Callback invoked by confluent-kafka after each produce attempt."""
    if err is not None:
        print(f"[ERROR] Delivery failed for {msg.key()}: {err}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="POS transaction Kafka producer")
    parser.add_argument("--tps", type=float, default=10.0,
                        help="Target transactions per second (default: 10)")
    args = parser.parse_args()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "raw_pos_transactions")
    sleep_interval = 1.0 / args.tps

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    # Graceful shutdown on SIGTERM (Docker stop) or Ctrl+C
    shutdown = {"flag": False}

    def _handle_shutdown(signum, frame):
        print("\n[INFO] Shutdown signal received. Flushing producer...", flush=True)
        shutdown["flag"] = True

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    print(f"[INFO] Starting POS generator → topic '{topic}' @ {bootstrap_servers} ({args.tps} TPS)", flush=True)

    count = 0
    try:
        while not shutdown["flag"]:
            payload = build_transaction()
            producer.produce(
                topic=topic,
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report,
            )
            # Trigger delivery callbacks without blocking
            producer.poll(0)

            count += 1
            if count % 50 == 0:
                print(f"[INFO] {count} transactions produced", flush=True)

            time.sleep(sleep_interval)
    finally:
        print(f"[INFO] Flushing {producer.flush(timeout=10)} remaining messages...", flush=True)
        print(f"[INFO] Generator stopped after {count} transactions.", flush=True)


if __name__ == "__main__":
    main()
