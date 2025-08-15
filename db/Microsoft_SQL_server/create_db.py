#!/usr/bin/env python3
import subprocess
import time
import os
import sys

# --- Configuration ---
CONTAINER_NAME = "sqlpreview"
IMAGE_NAME = "mcr.microsoft.com/mssql/server:2022-preview-ubuntu-22.04"
SA_PASSWORD = "yourStrong(!)Password"
PORT = 1433
SQLCMD_PATH = "/opt/mssql-tools/bin/sqlcmd"
SETUP_SQL = "ecommerce.sql"

# --- SQL Script Content ---
SQL_SCRIPT = """
CREATE DATABASE ecommerce;
GO

USE ecommerce;
GO

CREATE TABLE users (
    user_id INT PRIMARY KEY,
    username NVARCHAR(50),
    email NVARCHAR(100),
    created_at DATETIME DEFAULT GETDATE()
);
GO

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name NVARCHAR(100),
    description NVARCHAR(255),
    price DECIMAL(10, 2),
    stock INT
);
GO

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    user_id INT,
    order_date DATETIME DEFAULT GETDATE(),
    total_amount DECIMAL(10, 2),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
GO

CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10, 2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
GO

INSERT INTO users VALUES (1, 'alice', 'alice@example.com', GETDATE());
INSERT INTO users VALUES (2, 'bob', 'bob@example.com', GETDATE());
GO

INSERT INTO products VALUES (1, 'Laptop', '14-inch gaming laptop', 899.99, 10);
INSERT INTO products VALUES (2, 'Phone', '5G smartphone', 499.99, 25);
GO

INSERT INTO orders VALUES (1, 1, GETDATE(), 1399.98);
GO

INSERT INTO order_items VALUES (1, 1, 1, 1, 899.99);
INSERT INTO order_items VALUES (2, 1, 2, 1, 499.99);
GO
"""


def run(cmd, check=True, capture=True):
    print(f"🧾 Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=capture, text=True)
    if result.returncode != 0 and check:
        print("❌ Error:", result.stderr.strip())
        sys.exit(1)
    return result.stdout.strip() if capture else ""


def write_sql_script():
    with open(SETUP_SQL, "w") as f:
        f.write(SQL_SCRIPT)


def start_container():
    print("🚀 Starting SQL Server container...")
    run(f"docker rm -f {CONTAINER_NAME}", check=False)
    run(
        f'docker run -e "ACCEPT_EULA=Y" '
        f'-e "MSSQL_SA_PASSWORD={SA_PASSWORD}" '
        f'-e "MSSQL_PID=Evaluation" '
        f'-p {PORT}:1433 '
        f'--name {CONTAINER_NAME} '
        f'--hostname {CONTAINER_NAME} '
        f'-d {IMAGE_NAME}'
    )


def wait_for_sql():
    print("⏳ Waiting for SQL Server to be ready...")
    for i in range(60):
        cmd = (
            f'docker exec {CONTAINER_NAME} /bin/bash -c '
            f'"{SQLCMD_PATH} -S localhost -U sa -P \\"{SA_PASSWORD}\\" -Q \\"SELECT 1; GO\\""'
        )
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if "1" in result.stdout:
            print("✅ SQL Server is ready!")
            return True
        print(f"🔄 Attempt {i+1}/60: Waiting for SQL Server...")
        time.sleep(2)
    print("❌ SQL Server did not start in time.")
    sys.exit(1)


def copy_sql_script():
    print("📁 Copying SQL script into container...")
    run(f"docker cp {SETUP_SQL} {CONTAINER_NAME}:/ecommerce.sql")


def run_sql_script_shell_wrapper():
    print("⚙️ Running SQL setup script via shell wrapper...")
    cmd = (
        f'docker exec {CONTAINER_NAME} /bin/bash -c '
        f'"{SQLCMD_PATH} -S localhost -U sa -P \\"{SA_PASSWORD}\\" -i /ecommerce.sql"'
    )
    run(cmd)


def cleanup():
    if os.path.exists(SETUP_SQL):
        os.remove(SETUP_SQL)
        print("🧹 Cleaned up local SQL script.")


def main():
    write_sql_script()
    start_container()
    wait_for_sql()
    copy_sql_script()
    run_sql_script_shell_wrapper()
    print("\n🎉 SQL Server 'ecommerce' DB setup complete!")
    print(f"➡️ Connect manually:")
    print(f"   docker exec -it {CONTAINER_NAME} {SQLCMD_PATH} -S localhost -U sa -P \"{SA_PASSWORD}\"")
    cleanup()


if __name__ == "__main__":
    main()
