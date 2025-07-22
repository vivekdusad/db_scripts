#!/usr/bin/env python3
"""
Firebird Database Setup Script
This script creates a Firebird Docker container and populates it with sample data.
"""

import subprocess
import time
import sys
from datetime import datetime, timedelta
import random
import os
import tempfile

# Docker configuration
CONTAINER_NAME = "firebird-server"
FIREBIRD_HOST = "localhost"
FIREBIRD_PORT = 3050
FIREBIRD_USER = "SYSDBA"
FIREBIRD_PASSWORD = "masterkey"
FIREBIRD_DATABASE = "sample_db"
FIREBIRD_DATA_PATH = "./firebird_data"

# Database file path inside container
DB_FILE_PATH = f"/firebird/data/{FIREBIRD_DATABASE}.fdb"

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

def wait_for_firebird():
    """Wait for Firebird to be ready."""
    print("Waiting for Firebird to be ready...")
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            # Try to connect using isql through docker exec
            test_command = f"""docker exec {CONTAINER_NAME} /opt/firebird/bin/isql -user {FIREBIRD_USER} -password {FIREBIRD_PASSWORD} {DB_FILE_PATH} -q -o /dev/null <<< "SELECT 1 FROM RDB\\$DATABASE;" """
            result = run_command(test_command, check=False)
            if result is not None:
                print("Firebird is ready!")
                return True
        except Exception:
            pass
        
        print(f"Attempt {attempt + 1}/{max_attempts} - Firebird not ready yet...")
        time.sleep(2)
    
    print("Firebird failed to start within the expected time")
    return False

def execute_firebird_sql(sql_commands, database_path=None):
    """Execute SQL commands on Firebird using isql."""
    if database_path is None:
        database_path = DB_FILE_PATH
    
    try:
        # Create a temporary SQL file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_commands)
            temp_sql_file = f.name
        
        # Copy the SQL file to the container
        copy_command = f"docker cp {temp_sql_file} {CONTAINER_NAME}:/tmp/temp_script.sql"
        run_command(copy_command)
        
        # Execute the SQL file using isql
        isql_command = f"""docker exec {CONTAINER_NAME} /opt/firebird/bin/isql -user {FIREBIRD_USER} -password {FIREBIRD_PASSWORD} {database_path} -i /tmp/temp_script.sql"""
        result = run_command(isql_command, check=False)
        
        # Clean up
        os.unlink(temp_sql_file)
        run_command(f"docker exec {CONTAINER_NAME} rm -f /tmp/temp_script.sql", check=False)
        
        return result
        
    except Exception as e:
        print(f"Failed to execute SQL: {e}")
        return None

def create_database():
    """Create the Firebird database."""
    print("Creating Firebird database...")
    
    try:
        # Create database using isql
        create_db_command = f"""docker exec {CONTAINER_NAME} /opt/firebird/bin/isql -user {FIREBIRD_USER} -password {FIREBIRD_PASSWORD} -q <<< "CREATE DATABASE '{DB_FILE_PATH}';" """
        result = run_command(create_db_command, check=False)
        
        if result is not None:
            print("Database created successfully!")
            return True
        else:
            print("Failed to create database")
            return False
    except Exception as e:
        print(f"Failed to create database: {e}")
        return False

def create_tables():
    """Create sample tables in Firebird."""
    print("Creating Firebird tables...")
    
    sql_script = """
    -- Create employees table
    CREATE TABLE employees (
        id INTEGER NOT NULL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        department VARCHAR(50),
        position VARCHAR(100),
        salary DECIMAL(10,2),
        hire_date DATE,
        is_active CHAR(1) DEFAULT 'Y',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create products table
    CREATE TABLE products (
        id INTEGER NOT NULL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        description BLOB SUB_TYPE TEXT,
        category VARCHAR(50),
        price DECIMAL(10,2),
        stock_quantity INTEGER DEFAULT 0,
        manufacturer VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create customers table
    CREATE TABLE customers (
        id INTEGER NOT NULL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        phone VARCHAR(20),
        address BLOB SUB_TYPE TEXT,
        city VARCHAR(50),
        country VARCHAR(50),
        registration_date DATE DEFAULT CURRENT_DATE,
        is_active CHAR(1) DEFAULT 'Y'
    );

    -- Create orders table
    CREATE TABLE orders (
        id INTEGER NOT NULL PRIMARY KEY,
        customer_name VARCHAR(100) NOT NULL,
        customer_email VARCHAR(100),
        order_date DATE NOT NULL,
        total_amount DECIMAL(12,2),
        status VARCHAR(20) DEFAULT 'PENDING',
        shipping_address BLOB SUB_TYPE TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create order_items table
    CREATE TABLE order_items (
        id INTEGER NOT NULL PRIMARY KEY,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        total_price DECIMAL(12,2) NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    );

    -- Create indexes
    CREATE INDEX idx_employees_department ON employees(department);
    CREATE INDEX idx_products_category ON products(category);
    CREATE INDEX idx_orders_status ON orders(status);
    CREATE INDEX idx_orders_date ON orders(order_date);
    CREATE INDEX idx_customers_email ON customers(email);

    COMMIT;
    """
    
    result = execute_firebird_sql(sql_script)
    if result is not None:
        print("Tables and indexes created successfully!")
        return True
    else:
        print("Failed to create tables")
        return False

def generate_sample_data():
    """Generate sample data for Firebird tables."""
    print("Generating sample data...")
    
    # Sample employees data
    departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'Operations']
    positions = ['Manager', 'Senior Developer', 'Developer', 'Analyst', 'Specialist', 'Coordinator']
    
    employees_data = []
    for i in range(1, 101):
        employees_data.append({
            'id': i,
            'first_name': f"FirstName{i}",
            'last_name': f"LastName{i}",
            'email': f"employee{i}@company.com",
            'department': random.choice(departments),
            'position': random.choice(positions),
            'salary': round(random.uniform(40000, 120000), 2),
            'hire_date': (datetime.now() - timedelta(days=random.randint(30, 1825))).strftime('%Y-%m-%d'),
            'is_active': random.choice(['Y', 'N'])
        })
    
    # Sample products data
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys']
    manufacturers = ['TechCorp', 'StyleBrand', 'QualityMfg', 'Innovation Inc', 'Global Goods']
    
    products_data = []
    for i in range(1, 201):
        products_data.append({
            'id': i,
            'name': f"Product {i}",
            'description': f"This is a detailed description of product {i}. It offers excellent quality and value for customers.",
            'category': random.choice(categories),
            'price': round(random.uniform(10.0, 999.99), 2),
            'stock_quantity': random.randint(0, 1000),
            'manufacturer': random.choice(manufacturers)
        })
    
    # Sample customers data
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']
    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia']
    
    customers_data = []
    for i in range(1, 151):
        customers_data.append({
            'id': i,
            'first_name': f"Customer{i}",
            'last_name': f"LastName{i}",
            'email': f"customer{i}@email.com",
            'phone': f"555-{random.randint(1000, 9999)}",
            'address': f"{random.randint(100, 9999)} Main Street",
            'city': random.choice(cities),
            'country': random.choice(countries),
            'registration_date': (datetime.now() - timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d'),
            'is_active': random.choice(['Y', 'N'])
        })
    
    # Sample orders data
    statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED']
    
    orders_data = []
    for i in range(1, 301):
        orders_data.append({
            'id': i,
            'customer_name': f"Customer {random.randint(1, 150)}",
            'customer_email': f"customer{random.randint(1, 150)}@email.com",
            'order_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
            'total_amount': round(random.uniform(25.0, 2500.0), 2),
            'status': random.choice(statuses),
            'shipping_address': f"{random.randint(100, 9999)} Shipping Street, {random.choice(cities)}"
        })
    
    # Sample order items data
    order_items_data = []
    item_id = 1
    for order in orders_data:
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_id = random.randint(1, 200)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(10.0, 200.0), 2)
            order_items_data.append({
                'id': item_id,
                'order_id': order['id'],
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': round(quantity * unit_price, 2)
            })
            item_id += 1
    
    return employees_data, products_data, customers_data, orders_data, order_items_data

def escape_sql_string(value):
    """Escape single quotes in SQL strings."""
    if isinstance(value, str):
        return value.replace("'", "''")
    return str(value)

def insert_data(table_name, data, columns):
    """Insert data into a Firebird table."""
    if not data:
        return True
    
    print(f"Inserting data into '{table_name}' table...")
    
    # Generate INSERT statements in batches
    batch_size = 50
    total_items = len(data)
    
    for i in range(0, total_items, batch_size):
        batch = data[i:i + batch_size]
        
        # Build INSERT statements for this batch
        sql_statements = []
        for item in batch:
            values = []
            for col in columns:
                value = item[col]
                if value is None:
                    values.append('NULL')
                elif isinstance(value, str):
                    values.append(f"'{escape_sql_string(value)}'")
                else:
                    values.append(str(value))
            
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
            sql_statements.append(sql)
        
        # Add COMMIT at the end of each batch
        sql_statements.append("COMMIT;")
        batch_sql = '\n'.join(sql_statements)
        
        result = execute_firebird_sql(batch_sql)
        if result is None:
            print(f"Failed to insert batch {i//batch_size + 1} into '{table_name}' table")
            return False
        
        print(f"Inserted batch {i//batch_size + 1}/{(total_items + batch_size - 1)//batch_size}")
    
    print(f"Successfully inserted {total_items} records into '{table_name}' table")
    return True

def main():
    """Main function to set up Firebird and populate with sample data."""
    print("Starting Firebird setup...")
    
    # Check if container already exists
    existing_container = run_command(f"docker ps -a --filter name={CONTAINER_NAME} --format '{{{{.Names}}}}'", check=False)
    
    if existing_container:
        print(f"Container {CONTAINER_NAME} already exists. Stopping and removing it...")
        run_command(f"docker stop {CONTAINER_NAME}", check=False)
        run_command(f"docker rm {CONTAINER_NAME}", check=False)
    
    # Create data directory
    run_command(f"mkdir -p {FIREBIRD_DATA_PATH}", check=False)
    
    # Start Firebird Docker container
    print("Starting Firebird Docker container...")
    docker_command = f"""docker run -d \
  --name {CONTAINER_NAME} \
  -p {FIREBIRD_PORT}:{FIREBIRD_PORT} \
  -e ISC_PASSWORD={FIREBIRD_PASSWORD} \
  -e FIREBIRD_DATABASE={FIREBIRD_DATABASE}.fdb \
  -v {os.path.abspath(FIREBIRD_DATA_PATH)}:/firebird/data \
  jacobalberty/firebird:v3.0"""
    
    result = run_command(docker_command)
    if result:
        print(f"Firebird container started successfully: {result}")
    else:
        print("Failed to start Firebird container")
        return False
    
    # Wait for Firebird to be ready
    time.sleep(15)  # Give Firebird some time to initialize
    
    # Create database
    if not create_database():
        return False
    
    # Wait a bit more for database to be fully ready
    if not wait_for_firebird():
        return False
    
    # Create tables
    if not create_tables():
        return False
    
    # Generate sample data
    employees_data, products_data, customers_data, orders_data, order_items_data = generate_sample_data()
    
    # Insert data into tables
    if not insert_data("employees", employees_data, 
                      ['id', 'first_name', 'last_name', 'email', 'department', 'position', 'salary', 'hire_date', 'is_active']):
        return False
    
    if not insert_data("products", products_data,
                      ['id', 'name', 'description', 'category', 'price', 'stock_quantity', 'manufacturer']):
        return False
    
    if not insert_data("customers", customers_data,
                      ['id', 'first_name', 'last_name', 'email', 'phone', 'address', 'city', 'country', 'registration_date', 'is_active']):
        return False
    
    if not insert_data("orders", orders_data,
                      ['id', 'customer_name', 'customer_email', 'order_date', 'total_amount', 'status', 'shipping_address']):
        return False
    
    if not insert_data("order_items", order_items_data,
                      ['id', 'order_id', 'product_id', 'quantity', 'unit_price', 'total_price']):
        return False
    
    print("\n" + "="*50)
    print("Firebird setup completed successfully!")
    print(f"Host: {FIREBIRD_HOST}")
    print(f"Port: {FIREBIRD_PORT}")
    print(f"Database: {FIREBIRD_DATABASE}.fdb")
    print(f"User: {FIREBIRD_USER}")
    print(f"Password: {FIREBIRD_PASSWORD}")
    print(f"Data Path: {FIREBIRD_DATA_PATH}")
    print("\nSample data inserted:")
    print(f"- {len(employees_data)} employees")
    print(f"- {len(products_data)} products")
    print(f"- {len(customers_data)} customers")
    print(f"- {len(orders_data)} orders")
    print(f"- {len(order_items_data)} order items")
    print("\nTables created:")
    print("- employees: Employee information")
    print("- products: Product catalog")
    print("- customers: Customer data")
    print("- orders: Order records")
    print("- order_items: Order line items")
    print("\nYou can now connect to Firebird and start querying!")
    print("Example connection via Docker:")
    print(f"  docker exec -it {CONTAINER_NAME} /opt/firebird/bin/isql -user {FIREBIRD_USER} -password {FIREBIRD_PASSWORD} {DB_FILE_PATH}")
    print("\nExample queries:")
    print("  SELECT * FROM employees WHERE department = 'Engineering';")
    print("  SELECT COUNT(*) FROM products;")
    print("  SELECT * FROM orders WHERE status = 'PENDING';")
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