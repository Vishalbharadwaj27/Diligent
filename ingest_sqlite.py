#!/usr/bin/env python3
"""ingest_sqlite.py

Reads CSV files from `data/` and ingests them into a SQLite database `ecom.db`.
Uses pandas and sqlite3. Creates tables with proper types and foreign keys,
then inserts all rows.
"""
import sqlite3
import pandas as pd
import os

DB_PATH = "ecom.db"
DATA_DIR = "data"

def create_tables(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            phone TEXT,
            created_at TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            price REAL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TEXT,
            total_amount REAL,
            FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            payment_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            amount REAL,
            method TEXT,
            payment_date TEXT,
            FOREIGN KEY(order_id) REFERENCES orders(order_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shipments (
            shipment_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            shipment_date TEXT,
            status TEXT,
            FOREIGN KEY(order_id) REFERENCES orders(order_id)
        );
        """
    )
    conn.commit()

def ingest_csv_to_table(conn, csv_path, table_name, columns):
    df = pd.read_csv(csv_path)
    # Ensure dataframe columns match expected
    df = df.loc[:, columns]
    placeholders = ",".join(["?" for _ in columns])
    insert_sql = f"INSERT OR REPLACE INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
    cur = conn.cursor()
    cur.executemany(insert_sql, df.values.tolist())
    conn.commit()

def main():
    if not os.path.isdir(DATA_DIR):
        raise SystemExit(f"Data directory '{DATA_DIR}' not found. Run generate_data.py first.")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    create_tables(conn)

    ingest_csv_to_table(conn, os.path.join(DATA_DIR, "customers.csv"), "customers", [
        "customer_id", "name", "email", "phone", "created_at",
    ])

    ingest_csv_to_table(conn, os.path.join(DATA_DIR, "products.csv"), "products", [
        "product_id", "product_name", "category", "price",
    ])

    ingest_csv_to_table(conn, os.path.join(DATA_DIR, "orders.csv"), "orders", [
        "order_id", "customer_id", "order_date", "total_amount",
    ])

    ingest_csv_to_table(conn, os.path.join(DATA_DIR, "payments.csv"), "payments", [
        "payment_id", "order_id", "amount", "method", "payment_date",
    ])

    ingest_csv_to_table(conn, os.path.join(DATA_DIR, "shipments.csv"), "shipments", [
        "shipment_id", "order_id", "shipment_date", "status",
    ])

    conn.close()
    print("Data successfully ingested into ecom.db")

if __name__ == "__main__":
    main()
