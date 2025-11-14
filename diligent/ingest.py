import sqlite3
from pathlib import Path

import pandas as pd


DATA_DIR = Path("data")
DB_PATH = Path("ecommerce.db")

TABLE_SCHEMAS = {
    "customers": """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            country TEXT NOT NULL
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL NOT NULL
        )
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL
        )
    """,
    "payments": """
        CREATE TABLE IF NOT EXISTS payments (
            payment_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            payment_method TEXT NOT NULL,
            payment_status TEXT NOT NULL
        )
    """,
}

CSV_TABLE_MAP = [
    ("customers.csv", "customers"),
    ("products.csv", "products"),
    ("orders.csv", "orders"),
    ("order_items.csv", "order_items"),
    ("payments.csv", "payments"),
]


def main() -> None:
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Data directory not found: {DATA_DIR.resolve()}")

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        for ddl in TABLE_SCHEMAS.values():
            cursor.execute(ddl)

        for csv_name, table in CSV_TABLE_MAP:
            csv_path = DATA_DIR / csv_name
            if not csv_path.exists():
                raise FileNotFoundError(f"Missing required CSV file: {csv_path}")

            df = pd.read_csv(csv_path)
            df.to_sql(table, conn, if_exists="append", index=False)

    print("Data Ingested Successfully")


if __name__ == "__main__":
    main()

