#!/usr/bin/env python3
"""generate_data.py

Generates synthetic e-commerce CSV data files inside `data/`:
- customers.csv
- products.csv
- orders.csv
- payments.csv
- shipments.csv

Requirements: pandas, faker, numpy
Run: python generate_data.py
"""
from faker import Faker
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

OUT_DIR = "data"

def generate_customers(n=250):
    customers = []
    start = datetime.now() - timedelta(days=365 * 3)
    for i in range(1, n + 1):
        created_at = start + timedelta(days=random.randint(0, 365 * 3), seconds=random.randint(0, 86400))
        customers.append({
            "customer_id": i,
            "name": fake.name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "created_at": created_at.isoformat(sep=" ", timespec="seconds"),
        })
    return pd.DataFrame(customers)

def generate_products(n=300):
    categories = ["Books", "Electronics", "Home", "Clothing", "Toys", "Sporting Goods", "Beauty"]
    products = []
    for i in range(1, n + 1):
        price = round(random.uniform(5, 500), 2)
        products.append({
            "product_id": i,
            "product_name": fake.sentence(nb_words=3).rstrip('.'),
            "category": random.choice(categories),
            "price": price,
        })
    return pd.DataFrame(products)

def generate_orders(customers_df, products_df, n=500):
    orders = []
    order_start = datetime.now() - timedelta(days=365 * 2)
    for i in range(1, n + 1):
        customer_id = int(customers_df.sample(1).iloc[0]["customer_id"])
        order_date = order_start + timedelta(days=random.randint(0, 365 * 2), seconds=random.randint(0,86400))
        # pick 1-5 products, compute total
        items = products_df.sample(random.randint(1, 5))
        total_amount = float(items["price"].sum())
        orders.append({
            "order_id": i,
            "customer_id": customer_id,
            "order_date": order_date.isoformat(sep=" ", timespec="seconds"),
            "total_amount": round(total_amount, 2),
        })
    return pd.DataFrame(orders)

def generate_payments(orders_df, n=None):
    payments = []
    n = n or len(orders_df)
    methods = ["credit_card", "paypal", "bank_transfer", "gift_card"]
    payment_id = 1
    for idx, row in orders_df.iterrows():
        order_id = int(row["order_id"])
        # payment date is on or after order_date (0-5 days)
        od = datetime.fromisoformat(row["order_date"])
        payment_date = od + timedelta(days=random.randint(0, 5), seconds=random.randint(0,86400))
        payments.append({
            "payment_id": payment_id,
            "order_id": order_id,
            "amount": float(row["total_amount"]),
            "method": random.choice(methods),
            "payment_date": payment_date.isoformat(sep=" ", timespec="seconds"),
        })
        payment_id += 1
    return pd.DataFrame(payments)

def generate_shipments(orders_df):
    shipments = []
    statuses = ["pending", "shipped", "delivered", "returned"]
    shipment_id = 1
    for idx, row in orders_df.iterrows():
        order_id = int(row["order_id"])
        od = datetime.fromisoformat(row["order_date"])
        # shipment happens 0-7 days after order
        shipment_date = od + timedelta(days=random.randint(0, 7), seconds=random.randint(0,86400))
        # pick status — delivered more likely if older
        age_days = (datetime.now() - shipment_date).days
        if age_days > 30:
            status = random.choices(statuses, weights=[5, 15, 70, 10])[0]
        else:
            status = random.choices(statuses, weights=[40, 40, 15, 5])[0]
        shipments.append({
            "shipment_id": shipment_id,
            "order_id": order_id,
            "shipment_date": shipment_date.isoformat(sep=" ", timespec="seconds"),
            "status": status,
        })
        shipment_id += 1
    return pd.DataFrame(shipments)

def main():
    customers_df = generate_customers(250)
    products_df = generate_products(300)
    orders_df = generate_orders(customers_df, products_df, 500)
    payments_df = generate_payments(orders_df)
    shipments_df = generate_shipments(orders_df)

    customers_df.to_csv(f"{OUT_DIR}/customers.csv", index=False)
    products_df.to_csv(f"{OUT_DIR}/products.csv", index=False)
    orders_df.to_csv(f"{OUT_DIR}/orders.csv", index=False)
    payments_df.to_csv(f"{OUT_DIR}/payments.csv", index=False)
    shipments_df.to_csv(f"{OUT_DIR}/shipments.csv", index=False)

    print("Generated CSV files in data/ (customers, products, orders, payments, shipments)")

if __name__ == "__main__":
    main()
