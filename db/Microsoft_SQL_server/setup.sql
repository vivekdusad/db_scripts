
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
