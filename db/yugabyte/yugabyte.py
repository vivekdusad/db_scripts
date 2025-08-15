import subprocess
import time
import os

# Configuration
YUGABYTE_IMAGE = "yugabytedb/yugabyte:latest"
CONTAINER_NAME = "yugabyte_5"
DB_NAME = "ecommerce"
DB_USER = "yugabyte"
DB_PORT = "5433"
YSQL_BIN_PATH = "/home/yugabyte/bin/ysqlsh"

# Step 1: Pull the YugabyteDB image
def pull_yugabyte_image():
    print(f"📦 Pulling YugabyteDB Docker image: {YUGABYTE_IMAGE}")
    subprocess.run(["docker", "pull", YUGABYTE_IMAGE], check=True)

# Step 2: Run the Yugabyte container (non-persistent, single-node RF=1)
def run_yugabyte_container():
    print(f"🚀 Starting YugabyteDB container: {CONTAINER_NAME}")
    subprocess.run([
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-p", "7000:7000",  # Web UI
        "-p", "9000:9000",  # YB-Master UI
        "-p", "15433:15433",  # Web UI secure
        "-p", f"{DB_PORT}:5433",  # YSQL
        "-p", "9042:9042",  # YCQL
        YUGABYTE_IMAGE,
        "bin/yugabyted", "start", "--background=false"
    ], check=True)

# Step 3: Wait until YugaByte is ready
def wait_for_yugabyte_ready(container_name, retries=10, delay=5):
    for i in range(retries):
        try:
            print(f"⌛ Waiting for YugabyteDB to be ready (attempt {i+1})...")
            cmd = f'docker exec -i {container_name} {YSQL_BIN_PATH} -h 172.17.0.2 -U {DB_USER} -c "SELECT 1;"'

            subprocess.run(cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            print("✅ YugabyteDB is ready!")
            return
        except subprocess.CalledProcessError:
            time.sleep(delay)
    raise TimeoutError("❌ YugabyteDB did not become ready in time.")


# Step 4: Create the ecommerce schema and sample data
def create_ecommerce_schema_and_data():
    print("🛠️ Creating ecommerce schema and inserting sample data...")

    sql_script = f"""
    CREATE DATABASE {DB_NAME};
    \\c {DB_NAME}

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

    filename = "ecommerce_yugabyte.sql"
    with open(filename, "w") as f:
        f.write(sql_script)

    # Copy SQL script into the container
    subprocess.run(["docker", "cp", filename, f"{CONTAINER_NAME}:/setup.sql"], check=True)

    # Execute the script using ysqlsh inside the container
    subprocess.run([
        "docker", "exec", "-i", CONTAINER_NAME,
        YSQL_BIN_PATH, "-h","172.17.0.2","-U", DB_USER, "-f", "/setup.sql"
    ], check=True)

    os.remove(filename)
    print("🎉 Ecommerce schema and data created successfully.")

# Main entry point
if __name__ == "__main__":
    pull_yugabyte_image()
    run_yugabyte_container()
    wait_for_yugabyte_ready(CONTAINER_NAME)
    create_ecommerce_schema_and_data()

    print("\n✅ YugabyteDB container is up and initialized with ecommerce data.")
    print(f"🔗 Access Yugabyte Web UI: http://localhost:15433")
    print(f"🖥️  Connect via: docker exec -it {CONTAINER_NAME} bash -c '{YSQL_BIN_PATH} -U yugabyte -d {DB_NAME}'")
