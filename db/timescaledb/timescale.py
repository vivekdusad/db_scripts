import subprocess
import time
import os

# Configuration
TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg17"
CONTAINER_NAME = "timescale_db_6"
DB_NAME = "ecommerce"
DB_USER = "postgres"
DB_PASSWORD = ""  # Leave blank or update if needed
DB_PORT = "5432"

# Step 1: Pull TimescaleDB Docker image
def pull_timescale_image():
    print(f"Pulling TimescaleDB Docker image: {TIMESCALE_IMAGE}")
    subprocess.run(["docker", "pull", TIMESCALE_IMAGE], check=True)

# Step 2: Run TimescaleDB container
def run_timescale_container():
    print(f"Running TimescaleDB container: {CONTAINER_NAME}")
    subprocess.run([
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-e", f"""POSTGRES_PASSWORD="{DB_PASSWORD}" """,
        "-p", f"{DB_PORT}:5432",
        TIMESCALE_IMAGE
    ], check=True)

# Step 3: Wait for TimescaleDB to be ready
def wait_for_timescale_ready(container_name, retries=10, delay=5):
    for i in range(retries):
        try:
            print(f"Checking if TimescaleDB is ready (attempt {i+1})...")
            subprocess.run([
                "docker", "exec", "-i", container_name,
                "pg_isready", "-U", DB_USER
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("TimescaleDB is ready!")
            return
        except subprocess.CalledProcessError:
            time.sleep(delay)
    raise TimeoutError("TimescaleDB did not become ready in time.")

# Step 4: Create DB schema and insert data
def create_ecommerce_schema_and_data():
    print("Creating ecommerce database, tables, and inserting sample data...")

    sql_script = f"""
    CREATE DATABASE {DB_NAME};
    \\connect {DB_NAME}

    CREATE EXTENSION IF NOT EXISTS timescaledb;

    CREATE TABLE IF NOT EXISTS users (
        user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        username TEXT,
        email TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name TEXT,
        description TEXT,
        price NUMERIC,
        in_stock INT
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(user_id),
        order_date TIMESTAMPTZ NOT NULL,
        total NUMERIC
    );

    SELECT create_hypertable('orders', 'order_date', if_not_exists => TRUE);

    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id SERIAL PRIMARY KEY,
        order_id UUID REFERENCES orders(order_id),
        product_id UUID REFERENCES products(product_id),
        quantity INT,
        price NUMERIC
    );

    INSERT INTO users (username, email) VALUES 
      ('alice', 'alice@example.com'),
      ('bob', 'bob@example.com'),
      ('carol', 'carol@example.com'),
      ('dave', 'dave@example.com'),
      ('eve', 'eve@example.com'),
      ('frank', 'frank@example.com'),
      ('grace', 'grace@example.com'),
      ('heidi', 'heidi@example.com'),
      ('ivan', 'ivan@example.com'),
      ('judy', 'judy@example.com');

    INSERT INTO products (name, description, price, in_stock) VALUES
      ('Laptop', 'A powerful laptop', 1200.00, 10),
      ('Phone', 'A smart phone', 800.00, 25),
      ('Tablet', 'A handy tablet', 500.00, 15),
      ('Headphones', 'Noise-cancelling headphones', 200.00, 30),
      ('Monitor', '4K monitor', 350.00, 12),
      ('Keyboard', 'Mechanical keyboard', 100.00, 40),
      ('Mouse', 'Wireless mouse', 50.00, 50),
      ('Webcam', 'HD webcam', 80.00, 20),
      ('Printer', 'Color printer', 150.00, 8),
      ('Speaker', 'Bluetooth speaker', 60.00, 35);

    INSERT INTO orders (user_id, order_date, total)
    SELECT user_id, NOW() - INTERVAL '1 day' * s.a, (RANDOM()*1500)::NUMERIC(10,2)
    FROM users CROSS JOIN generate_series(1,10) AS s(a)
    LIMIT 10;

    INSERT INTO order_items (order_id, product_id, quantity, price)
    SELECT o.order_id, p.product_id, (1 + (RANDOM()*3)::INT), p.price
    FROM orders o
    CROSS JOIN LATERAL (
        SELECT product_id, price FROM products ORDER BY RANDOM() LIMIT 1
    ) p
    LIMIT 10;
    """

    filename = "ecommerce_setup.sql"
    with open(filename, "w") as f:
        f.write(sql_script)

    subprocess.run(["docker", "cp", filename, f"{CONTAINER_NAME}:/setup.sql"], check=True)

    subprocess.run([
        "docker", "exec", "-i", CONTAINER_NAME,
        "psql", "-U", DB_USER, "-f", "/setup.sql"
    ], check=True)

    os.remove(filename)
    print("Ecommerce database, tables, and sample data created successfully.")

# Main execution
if __name__ == "__main__":
    pull_timescale_image()
    run_timescale_container()
    wait_for_timescale_ready(CONTAINER_NAME)
    create_ecommerce_schema_and_data()
    print("✅ TimescaleDB ecommerce DB is up and initialized.")
    print("💡 Connect using: psql -U postgres -d ecommerce -h localhost")
