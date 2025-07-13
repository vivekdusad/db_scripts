#!/usr/bin/env python3
import subprocess
import time
import os
import sys

# Config
CONTAINER_NAME = "mssql-server"
IMAGE_NAME = "rapidfort/microsoft-sql-server-2019-ib"
SA_PASSWORD = "MyS3cureP@ss"
PORT = 1433
SQLCMD_PATH = "/opt/mssql-tools/bin/sqlcmd"

# SQL setup script (written locally, then copied into container)
SETUP_SQL = "setup.sql"
SQL_SCRIPT = f"""
CREATE DATABASE SampleDB;
GO
USE SampleDB;
GO

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
);
GO

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name NVARCHAR(100),
    category NVARCHAR(100),
    price DECIMAL(10, 2),
    stock_quantity INT
);
GO

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATETIME,
    total_amount DECIMAL(10, 2),
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);
GO

INSERT INTO customers VALUES (1, 'John', 'Doe', 'john@example.com', '555-0001', '123 Main St', 'City', 'State', '12345', GETDATE());
INSERT INTO products VALUES (1, 'Widget', 'Gadget', 19.99, 100);
INSERT INTO orders VALUES (1, 1, GETDATE(), 19.99);
GO
"""


def run(cmd, check=True, capture=True):
    print(f"🧾 Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=capture, text=True)
    if result.returncode != 0 and check:
        print("❌ Error:", result.stderr)
        sys.exit(1)
    return result.stdout.strip() if capture else ""


def write_sql_script():
    with open(SETUP_SQL, "w") as f:
        f.write(SQL_SCRIPT)


def start_container():
    print("🚀 Starting SQL Server container...")
    run(f"docker rm -f {CONTAINER_NAME}", check=False)
    run(f"docker run -d --name {CONTAINER_NAME} -e ACCEPT_EULA=Y -e SA_PASSWORD={SA_PASSWORD} -p {PORT}:1433 {IMAGE_NAME}")


def wait_for_sql():
    print("⏳ Waiting for SQL Server to be ready...")
    for _ in range(30):
        result = run(
            f"docker exec {CONTAINER_NAME} {SQLCMD_PATH} -S localhost -U sa -P {SA_PASSWORD} -Q \"SELECT 1\"",
            check=False,
        )
        if "1" in result:
            print("✅ SQL Server is ready!")
            return True
        time.sleep(2)
    print("❌ SQL Server did not start in time.")
    return False


def copy_sql_script():
    print("📁 Copying SQL script into container...")
    run(f"docker cp {SETUP_SQL} {CONTAINER_NAME}:/setup.sql")


def run_sql_script():
    print("⚙️ Running SQL setup script...")
    run(f"docker exec {CONTAINER_NAME} {SQLCMD_PATH} -S localhost -U sa -P {SA_PASSWORD} -i /setup.sql")


def cleanup():
    if os.path.exists(SETUP_SQL):
        os.remove(SETUP_SQL)


def main():
    write_sql_script()
    start_container()
    if wait_for_sql():
        copy_sql_script()
        run_sql_script()
        print("\n🎉 SQL Server setup complete!")
        print(f"➡️ Connect with: Server=localhost,{PORT}; User=sa; Password={SA_PASSWORD}")
    cleanup()


if __name__ == "__main__":
    main()
