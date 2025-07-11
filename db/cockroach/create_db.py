#!/usr/bin/env python3

import subprocess
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
import sys
import random
from datetime import datetime, timedelta

# Docker and DB Config
CONTAINER_NAME = "cockroach-node1"
DB_NAME = "defaultdb"
DB_USER = "root"
DB_HOST = "localhost"
DB_PORT = 26257

# SQL connection URI
CONN_STRING = f"postgresql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=disable"

def run_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {command}")
        print(e.stderr)
        return None

def start_container():
    print("Starting CockroachDB Docker container...")
    run_command(f"docker stop {CONTAINER_NAME} || true")
    run_command(f"docker rm {CONTAINER_NAME} || true")

    cmd = (
        f"docker run -d --name={CONTAINER_NAME} "
        f"-p 26257:26257 -p 8080:8080 "
        f"cockroachdb/cockroach start-single-node --insecure "
        f"--store=node1 --listen-addr=0.0.0.0:26257 --http-addr=0.0.0.0:8080"
    )
    return run_command(cmd)

def wait_for_cockroach():
    print("Waiting for CockroachDB to be ready...")
    for _ in range(30):
        try:
            with psycopg2.connect(CONN_STRING) as conn:
                return True
        except:
            time.sleep(2)
    print("CockroachDB did not start in time.")
    return False

def execute_sql(sql, values=None, many=False):
    with psycopg2.connect(CONN_STRING) as conn:
        with conn.cursor() as cur:
            if many:
                execute_values(cur, sql, values)
            elif values:
                cur.execute(sql, values)
            else:
                cur.execute(sql)

def create_tables():
    print("Creating tables...")
    create_queries = [
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id SERIAL PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            country TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id SERIAL PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            brand TEXT,
            price DECIMAL,
            cost DECIMAL,
            stock_quantity INT,
            description TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id SERIAL PRIMARY KEY,
            customer_id INT REFERENCES customers(customer_id),
            order_date TIMESTAMP,
            total_amount DECIMAL,
            status TEXT,
            shipping_address TEXT,
            shipping_city TEXT,
            shipping_state TEXT,
            shipping_zip TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id SERIAL PRIMARY KEY,
            order_id INT REFERENCES orders(order_id),
            product_id INT REFERENCES products(product_id),
            quantity INT,
            unit_price DECIMAL,
            total_price DECIMAL,
            created_at TIMESTAMP
        )
        """
    ]
    for q in create_queries:
        execute_sql(q)
    print("Tables created successfully.")

def generate_data():
    now = datetime.now()
    first_names = ["John", "Jane", "Mike", "Sara"]
    last_names = ["Smith", "Johnson", "Lee", "Garcia"]
    cities = ["NY", "LA", "Chicago", "Houston"]
    states = ["NY", "CA", "IL", "TX"]
    brands = ["Apple", "Nike", "Samsung", "Sony"]
    categories = ["Electronics", "Clothing", "Books"]

    customers = [
        (
            f"{random.choice(first_names)}",
            f"{random.choice(last_names)}",
            f"user{i}@test.com",
            f"555-000-{i:04d}",
            f"{i} Main St",
            random.choice(cities),
            random.choice(states),
            f"{random.randint(10000,99999)}",
            "USA",
            now, now
        ) for i in range(1, 51)
    ]

    products = [
        (
            f"Product {i}",
            random.choice(categories),
            random.choice(brands),
            round(random.uniform(10, 500), 2),
            round(random.uniform(5, 250), 2),
            random.randint(0, 1000),
            f"Description {i}",
            now, now
        ) for i in range(1, 21)
    ]

    execute_sql("""
        INSERT INTO customers (
            first_name, last_name, email, phone, address,
            city, state, zip_code, country, created_at, updated_at
        ) VALUES %s
    """, customers, many=True)

    execute_sql("""
        INSERT INTO products (
            product_name, category, brand, price, cost, stock_quantity,
            description, created_at, updated_at
        ) VALUES %s
    """, products, many=True)

    orders = []
    order_items = []

    for i in range(1, 101):
        cust_id = random.randint(1, len(customers))
        order_date = now - timedelta(days=random.randint(0, 30))
        total = 0
        num_items = random.randint(1, 3)

        order = [cust_id, order_date, 0, "processing", f"{i} Shipping Rd", random.choice(cities),
                 random.choice(states), f"{random.randint(10000,99999)}", order_date, order_date]
        orders.append(order)

        for _ in range(num_items):
            prod_id = random.randint(1, len(products))
            qty = random.randint(1, 5)
            unit_price = round(random.uniform(10, 100), 2)
            total_price = qty * unit_price
            total += total_price
            order_items.append((i, prod_id, qty, unit_price, total_price, order_date))

        order[2] = round(total, 2)  # total_amount

    execute_sql("""
        INSERT INTO orders (
            customer_id, order_date, total_amount, status,
            shipping_address, shipping_city, shipping_state,
            shipping_zip, created_at, updated_at
        ) VALUES %s
    """, orders, many=True)

    execute_sql("""
        INSERT INTO order_items (
            order_id, product_id, quantity, unit_price,
            total_price, created_at
        ) VALUES %s
    """, order_items, many=True)

    print(f"Inserted {len(customers)} customers, {len(products)} products, {len(orders)} orders, {len(order_items)} order items.")

def main():
    if not start_container():
        print("Container start failed")
        return
    if not wait_for_cockroach():
        print("DB not ready")
        return
    create_tables()
    generate_data()
    print("\nCockroachDB setup complete. Sample data loaded.")

if __name__ == "__main__":
    main()
