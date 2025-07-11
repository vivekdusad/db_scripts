import psycopg2

# Replace with your EC2 instance's public IP or DNS
HOST = "YOUR_EC2_PUBLIC_IP"  # or "ec2-XX-XX-XX-XX.compute-1.amazonaws.com"
PORT = 26257
USER = "root"
DATABASE = "defaultdb"

sql_script = """
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    full_name STRING NOT NULL,
    email STRING UNIQUE NOT NULL,
    signup_date DATE NOT NULL
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name STRING NOT NULL,
    price DECIMAL(10,2) NOT NULL
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    amount DECIMAL(10,2),
    order_date DATE NOT NULL
);

INSERT INTO customers (customer_id, full_name, email, signup_date) VALUES
(1, 'Alice Johnson', 'alice@example.com', '2024-01-15'),
(2, 'Bob Smith', 'bob@example.com', '2024-02-20'),
(3, 'Charlie Brown', 'charlie@example.com', '2024-02-25'),
(4, 'David Lee', 'david@example.com', '2024-03-10'),
(5, 'Eva Green', 'eva@example.com', '2024-04-01'),
(6, 'Frank Castle', 'frank@example.com', '2024-04-18');

INSERT INTO products (product_id, product_name, price) VALUES
(1, 'Laptop', 800.00),
(2, 'Mouse', 25.00),
(3, 'Keyboard', 45.50),
(4, 'Monitor', 200.00),
(5, 'Printer', 150.00),
(6, 'Webcam', 60.00);

INSERT INTO orders (order_id, customer_id, product_id, amount, order_date) VALUES
(101, 1, 1, 800.00, '2024-02-01'),
(102, 2, 2, 25.00, '2024-02-25'),
(103, 3, 3, 45.50, '2024-03-05'),
(104, 4, 4, 200.00, '2024-04-12'),
(105, 5, 5, 150.00, '2024-04-20'),
(106, 6, 6, 60.00, '2024-05-01');
"""

def run_sql_script():
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            user=USER,
            dbname=DATABASE,
            sslmode='disable'  # because we're using --insecure mode
        )
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute(sql_script)
        print("✅ SQL script executed successfully!")
        cur.close()
        conn.close()
    except Exception as e:
        print("❌ Error occurred:", e)

if __name__ == "__main__":
    run_sql_script()