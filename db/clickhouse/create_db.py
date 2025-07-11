#!/usr/bin/env python3
"""
ClickHouse Database Setup Script
This script creates a ClickHouse Docker container and populates it with sample data.
"""

import subprocess
import time
import requests
import json
import sys
from datetime import datetime, timedelta
import random

# Docker configuration
CONTAINER_NAME = "clickhouse-server"
CLICKHOUSE_DB = "mydatabase"
CLICKHOUSE_USER = "myuser"
CLICKHOUSE_PASSWORD = "mypassword"
CLICKHOUSE_HTTP_PORT = 8123
CLICKHOUSE_NATIVE_PORT = 9000

# ClickHouse connection details
CLICKHOUSE_URL = f"http://localhost:{CLICKHOUSE_HTTP_PORT}"
AUTH = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

def run_command(command, check=True):
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=check)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {command}")
        print(f"Error: {e.stderr}")
        if check:
            sys.exit(1)
        return None

def wait_for_clickhouse():
    """Wait for ClickHouse to be ready."""
    print("Waiting for ClickHouse to be ready...")
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            response = requests.get(f"{CLICKHOUSE_URL}/ping", auth=AUTH, timeout=5)
            if response.status_code == 200:
                print("ClickHouse is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"Attempt {attempt + 1}/{max_attempts} - ClickHouse not ready yet...")
        time.sleep(2)
    
    print("ClickHouse failed to start within the expected time")
    return False

def execute_clickhouse_query(query, data=None):
    """Execute a query on ClickHouse."""
    try:
        if data:
            response = requests.post(
                CLICKHOUSE_URL,
                auth=AUTH,
                params={'query': query},
                data=data,
                headers={'Content-Type': 'application/octet-stream'}
            )
        else:
            response = requests.post(
                CLICKHOUSE_URL,
                auth=AUTH,
                data=query
            )
        
        if response.status_code == 200:
            return response.text
        else:
            print(f"Query failed: {query}")
            print(f"Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def create_tables():
    """Create the database tables."""
    print("Creating tables...")
    
    # Create customers table
    customers_table = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id UInt32,
        first_name String,
        last_name String,
        email String,
        phone String,
        address String,
        city String,
        state String,
        zip_code String,
        country String,
        created_at DateTime,
        updated_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY customer_id
    """
    
    # Create products table
    products_table = """
    CREATE TABLE IF NOT EXISTS products (
        product_id UInt32,
        product_name String,
        category String,
        brand String,
        price Decimal(10, 2),
        cost Decimal(10, 2),
        stock_quantity UInt32,
        description String,
        created_at DateTime,
        updated_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY product_id
    """
    
    # Create orders table
    orders_table = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id UInt32,
        customer_id UInt32,
        order_date DateTime,
        total_amount Decimal(10, 2),
        status String,
        shipping_address String,
        shipping_city String,
        shipping_state String,
        shipping_zip String,
        created_at DateTime,
        updated_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY order_id
    """
    
    # Create order_items table
    order_items_table = """
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id UInt32,
        order_id UInt32,
        product_id UInt32,
        quantity UInt32,
        unit_price Decimal(10, 2),
        total_price Decimal(10, 2),
        created_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY order_item_id
    """
    
    tables = [customers_table, products_table, orders_table, order_items_table]
    
    for table in tables:
        result = execute_clickhouse_query(table)
        if result is None:
            print("Failed to create tables")
            return False
    
    print("Tables created successfully!")
    return True

def generate_sample_data():
    """Generate and insert sample data."""
    print("Generating sample data...")
    
    # Sample customers data
    customers_data = []
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Lisa", "Robert", "Emily", "James", "Jessica"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
    
    for i in range(1, 101):  # 100 customers
        customer = {
            'customer_id': i,
            'first_name': random.choice(first_names),
            'last_name': random.choice(last_names),
            'email': f"customer{i}@example.com",
            'phone': f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            'address': f"{random.randint(100, 9999)} Main St",
            'city': random.choice(cities),
            'state': random.choice(states),
            'zip_code': f"{random.randint(10000, 99999)}",
            'country': "USA",
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        customers_data.append(customer)
    
    # Sample products data
    products_data = []
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Health & Beauty"]
    brands = ["Apple", "Samsung", "Nike", "Adidas", "Sony", "LG", "Canon", "Dell", "HP", "Microsoft"]
    
    for i in range(1, 51):  # 50 products
        product = {
            'product_id': i,
            'product_name': f"Product {i}",
            'category': random.choice(categories),
            'brand': random.choice(brands),
            'price': round(random.uniform(10.00, 500.00), 2),
            'cost': round(random.uniform(5.00, 250.00), 2),
            'stock_quantity': random.randint(0, 1000),
            'description': f"Description for product {i}",
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        products_data.append(product)
    
    # Sample orders data
    orders_data = []
    order_items_data = []
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    
    for i in range(1, 201):  # 200 orders
        customer_id = random.randint(1, 100)
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))
        
        order = {
            'order_id': i,
            'customer_id': customer_id,
            'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'total_amount': 0,  # Will be calculated
            'status': random.choice(statuses),
            'shipping_address': f"{random.randint(100, 9999)} Shipping St",
            'shipping_city': random.choice(cities),
            'shipping_state': random.choice(states),
            'shipping_zip': f"{random.randint(10000, 99999)}",
            'created_at': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': order_date.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Generate order items for this order
        num_items = random.randint(1, 5)
        total_amount = 0
        
        for j in range(num_items):
            product_id = random.randint(1, 50)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(10.00, 200.00), 2)
            total_price = round(quantity * unit_price, 2)
            total_amount += total_price
            
            order_item = {
                'order_item_id': len(order_items_data) + 1,
                'order_id': i,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': total_price,
                'created_at': order_date.strftime('%Y-%m-%d %H:%M:%S')
            }
            order_items_data.append(order_item)
        
        order['total_amount'] = round(total_amount, 2)
        orders_data.append(order)
    
    return customers_data, products_data, orders_data, order_items_data

def insert_data(table_name, data):
    """Insert data into a table."""
    if not data:
        return True
    
    print(f"Inserting data into {table_name}...")
    
    # Convert data to TSV format
    headers = list(data[0].keys())
    tsv_data = '\t'.join(headers) + '\n'
    
    for row in data:
        tsv_row = '\t'.join(str(row[header]) for header in headers)
        tsv_data += tsv_row + '\n'
    
    query = f"INSERT INTO {table_name} FORMAT TSV"
    result = execute_clickhouse_query(query, tsv_data)
    
    if result is None:
        print(f"Failed to insert data into {table_name}")
        return False
    
    print(f"Successfully inserted {len(data)} rows into {table_name}")
    return True

def main():
    """Main function to set up ClickHouse and populate with sample data."""
    print("Starting ClickHouse setup...")
    
    # Check if container already exists
    existing_container = run_command(f"docker ps -a --filter name={CONTAINER_NAME} --format '{{{{.Names}}}}'", check=False)
    
    if existing_container:
        print(f"Container {CONTAINER_NAME} already exists. Stopping and removing it...")
        run_command(f"docker stop {CONTAINER_NAME}", check=False)
        run_command(f"docker rm {CONTAINER_NAME}", check=False)
    
    # Start ClickHouse Docker container
    print("Starting ClickHouse Docker container...")
    docker_command = f"""docker run -d \
  --name {CONTAINER_NAME} \
  -e CLICKHOUSE_DB={CLICKHOUSE_DB} \
  -e CLICKHOUSE_USER={CLICKHOUSE_USER} \
  -e CLICKHOUSE_PASSWORD={CLICKHOUSE_PASSWORD} \
  -p {CLICKHOUSE_HTTP_PORT}:{CLICKHOUSE_HTTP_PORT} \
  -p {CLICKHOUSE_NATIVE_PORT}:{CLICKHOUSE_NATIVE_PORT} \
  clickhouse/clickhouse-server:latest"""
    
    result = run_command(docker_command)
    if result:
        print(f"ClickHouse container started successfully: {result}")
    else:
        print("Failed to start ClickHouse container")
        return False
    
    # Wait for ClickHouse to be ready
    if not wait_for_clickhouse():
        return False
    
    # Create tables
    if not create_tables():
        return False
    
    # Generate and insert sample data
    customers_data, products_data, orders_data, order_items_data = generate_sample_data()
    
    # Insert data into tables
    if not insert_data("customers", customers_data):
        return False
    
    if not insert_data("products", products_data):
        return False
    
    if not insert_data("orders", orders_data):
        return False
    
    if not insert_data("order_items", order_items_data):
        return False
    
    print("\n" + "="*50)
    print("ClickHouse setup completed successfully!")
    print(f"Database: {CLICKHOUSE_DB}")
    print(f"User: {CLICKHOUSE_USER}")
    print(f"Password: {CLICKHOUSE_PASSWORD}")
    print(f"HTTP Port: {CLICKHOUSE_HTTP_PORT}")
    print(f"Native Port: {CLICKHOUSE_NATIVE_PORT}")
    print(f"Connection URL: {CLICKHOUSE_URL}")
    print("\nSample data inserted:")
    print(f"- {len(customers_data)} customers")
    print(f"- {len(products_data)} products") 
    print(f"- {len(orders_data)} orders")
    print(f"- {len(order_items_data)} order items")
    print("\nYou can now connect to ClickHouse and start querying the data!")
    print("="*50)
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
