import subprocess
import time
import os

# Configuration
CASSANDRA_IMAGE = "cassandra:latest"
CONTAINER_NAME = "cassandra_db_6"
CQL_KEYSPACE = "ecommerce"

# Step 1: Pull Cassandra Docker image
def pull_cassandra_image():
    print(f"Pulling Cassandra Docker image: {CASSANDRA_IMAGE}")
    subprocess.run(["docker", "pull", CASSANDRA_IMAGE], check=True)

# Step 2: Run Cassandra container
def run_cassandra_container():
    print(f"Running Cassandra container: {CONTAINER_NAME}")
    subprocess.run([
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-p", "9042:9042",
        CASSANDRA_IMAGE
    ], check=True)

# Step 3: Wait for Cassandra to be ready
def wait_for_cassandra_ready(container_name, retries=10, delay=10):
    for i in range(retries):
        try:
            print(f"Checking if Cassandra is ready (attempt {i+1})...")
            subprocess.run([
                "docker", "exec", "-i", container_name,
                "cqlsh", "-e", "SELECT now() FROM system.local;"
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("Cassandra is ready!")
            return
        except subprocess.CalledProcessError:
            time.sleep(delay)
    raise TimeoutError("Cassandra did not become ready in time.")

# Step 4: Create everything in one CQL script and execute it
def create_ecommerce_schema_and_data():
    print("Creating keyspace, tables, and inserting sample data in one CQL script...")

    cql_script = f"""
    CREATE KEYSPACE IF NOT EXISTS {CQL_KEYSPACE} 
    WITH replication = {{'class':'SimpleStrategy', 'replication_factor' : 1}};

    USE {CQL_KEYSPACE};

    CREATE TABLE IF NOT EXISTS users (
        user_id UUID PRIMARY KEY,
        username text,
        email text,
        created_at timestamp
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id UUID PRIMARY KEY,
        name text,
        description text,
        price decimal,
        in_stock int
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id UUID PRIMARY KEY,
        user_id UUID,
        order_date timestamp,
        total decimal
    );

    CREATE TABLE IF NOT EXISTS order_items (
        order_id UUID,
        product_id UUID,
        quantity int,
        price decimal,
        PRIMARY KEY (order_id, product_id)
    );

    -- Insert sample users
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'alice', 'alice@example.com', toTimestamp(now()));
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'bob', 'bob@example.com', toTimestamp(now()));
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'carol', 'carol@example.com', toTimestamp(now()));
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'dave', 'dave@example.com', toTimestamp(now()));
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'eve', 'eve@example.com', toTimestamp(now()));
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'frank', 'frank@example.com', toTimestamp(now()));
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'grace', 'grace@example.com', toTimestamp(now()));
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'heidi', 'heidi@example.com', toTimestamp(now()));
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'ivan', 'ivan@example.com', toTimestamp(now()));
    INSERT INTO users (user_id, username, email, created_at) VALUES (uuid(), 'judy', 'judy@example.com', toTimestamp(now()));

    -- Insert sample products
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Laptop', 'A powerful laptop', 1200.00, 10);
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Phone', 'A smart phone', 800.00, 25);
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Tablet', 'A handy tablet', 500.00, 15);
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Headphones', 'Noise-cancelling headphones', 200.00, 30);
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Monitor', '4K monitor', 350.00, 12);
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Keyboard', 'Mechanical keyboard', 100.00, 40);
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Mouse', 'Wireless mouse', 50.00, 50);
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Webcam', 'HD webcam', 80.00, 20);
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Printer', 'Color printer', 150.00, 8);
    INSERT INTO products (product_id, name, description, price, in_stock) VALUES (uuid(), 'Speaker', 'Bluetooth speaker', 60.00, 35);

    -- Insert sample orders
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 1300.00);
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 800.00);
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 500.00);
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 200.00);
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 350.00);
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 100.00);
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 50.00);
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 80.00);
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 150.00);
    INSERT INTO orders (order_id, user_id, order_date, total) VALUES (uuid(), uuid(), toTimestamp(now()), 60.00);

    -- Insert sample order_items
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 1, 1200.00);
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 2, 1600.00);
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 1, 800.00);
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 3, 1500.00);
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 1, 200.00);
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 2, 400.00);
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 1, 350.00);
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 1, 100.00);
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 2, 100.00);
    INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (uuid(), uuid(), 1, 60.00);
    """

    filename = "full_ecommerce_setup.cql"
    with open(filename, "w") as f:
        f.write(cql_script)

    # Copy to container
    subprocess.run(["docker", "cp", filename, f"{CONTAINER_NAME}:/setup.cql"], check=True)

    # Execute script in container
    subprocess.run([
        "docker", "exec", "-i", CONTAINER_NAME,
        "cqlsh", "-f", "/setup.cql"
    ], check=True)

    # Clean up local file
    os.remove(filename)
    print("Ecommerce keyspace, tables and sample data created successfully.")

if __name__ == "__main__":
    pull_cassandra_image()
    run_cassandra_container()
    wait_for_cassandra_ready(CONTAINER_NAME)
    create_ecommerce_schema_and_data()
    print("Cassandra ecommerce DB is up and initialized.")
    print("You can now connect to it using cqlsh or your application.")
