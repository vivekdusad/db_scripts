#!/usr/bin/env python3
"""
Microsoft SQL Server Setup Script using Docker (rapidfort/microsoft-sql-server-2019-ib)
This script creates a SQL Server Docker container, sets up tables, and inserts sample data.
"""

import subprocess
import time
import pyodbc
import sys
import random
from datetime import datetime, timedelta

# Configuration
CONTAINER_NAME = "mssql-server"
MSSQL_PASSWORD = "MyS3cureP@ss"
MSSQL_USER = "sa"
MSSQL_PORT = 1433
MSSQL_IMAGE = "rapidfort/microsoft-sql-server-2019-ib"

# Sample DB and table names
DATABASE_NAME = "SampleDB"
DRIVER = "ODBC Driver 17 for SQL Server"
CONN_STR = f"DRIVER={{{DRIVER}}};SERVER=localhost,{MSSQL_PORT};UID={MSSQL_USER};PWD={MSSQL_PASSWORD}"


def run_command(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(1)
    return result.stdout.strip()


def start_docker():
    print("Starting SQL Server Docker container...")
    subprocess.run(f"docker stop {CONTAINER_NAME}", shell=True)
    subprocess.run(f"docker rm {CONTAINER_NAME}", shell=True)
    run_command(f"docker run -d --name {CONTAINER_NAME} -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD={MSSQL_PASSWORD}' \
        -p {MSSQL_PORT}:1433 {MSSQL_IMAGE}")


def wait_for_sql_server():
    print("Waiting for SQL Server to be ready...")
    for _ in range(30):
        try:
            with pyodbc.connect(CONN_STR, timeout=2) as conn:
                return True
        except:
            time.sleep(2)
    print("SQL Server did not start in time.")
    return False


def setup_database():
    with pyodbc.connect(CONN_STR, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{DATABASE_NAME}') CREATE DATABASE {DATABASE_NAME}")
        print("Database created.")


def create_tables():
    with pyodbc.connect(CONN_STR + f";DATABASE={DATABASE_NAME}") as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE customers (
            customer_id INT PRIMARY KEY,
            first_name NVARCHAR(100),
            last_name NVARCHAR(100),
            email NVARCHAR(100),
            phone NVARCHAR(50),
            address NVARCHAR(255),
            city NVARCHAR(100),
            state NVARCHAR(50),
            zip_code NVARCHAR(20),
            created_at DATETIME
        )""")

        cursor.execute("""
        CREATE TABLE products (
            product_id INT PRIMARY KEY,
            product_name NVARCHAR(100),
            category NVARCHAR(100),
            price DECIMAL(10, 2),
            stock_quantity INT
        )""")

        cursor.execute("""
        CREATE TABLE orders (
            order_id INT PRIMARY KEY,
            customer_id INT,
            order_date DATETIME,
            total_amount DECIMAL(10, 2),
            FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
        )""")
        print("Tables created.")


def insert_sample_data():
    with pyodbc.connect(CONN_STR + f";DATABASE={DATABASE_NAME}") as conn:
        cursor = conn.cursor()

        # Customers
        for i in range(1, 21):
            cursor.execute("""
                INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, i, f"First{i}", f"Last{i}", f"user{i}@example.com", f"555-000{i}",
                   f"{i} Main St", "CityX", "StateY", f"1000{i}", datetime.now())

        # Products
        for i in range(1, 11):
            cursor.execute("""
                INSERT INTO products VALUES (?, ?, ?, ?, ?)
            """, i, f"Product{i}", "CategoryA", round(random.uniform(10, 100), 2), random.randint(10, 100))

        # Orders
        for i in range(1, 31):
            cust_id = random.randint(1, 20)
            total = round(random.uniform(20, 200), 2)
            cursor.execute("""
                INSERT INTO orders VALUES (?, ?, ?, ?)
            """, i, cust_id, datetime.now() - timedelta(days=random.randint(0, 30)), total)

        print("Sample data inserted.")


def main():
    start_docker()
    if not wait_for_sql_server():
        sys.exit(1)
    setup_database()
    create_tables()
    insert_sample_data()
    print("\n✅ SQL Server setup complete. Connect with:")
    print(f"  Server: localhost,{MSSQL_PORT}")
    print(f"  User: {MSSQL_USER}")
    print(f"  Password: {MSSQL_PASSWORD}")
    print(f"  Database: {DATABASE_NAME}")


if __name__ == "__main__":
    main()
