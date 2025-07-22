# Firebird Database Setup

This script sets up a Firebird database server using Docker and populates it with sample data for testing and development purposes.

## Prerequisites

- Docker installed and running
- Python 3.7+
- pip (Python package manager)

## Installation

1. Install the required Python packages:
```bash
pip install -r requirements.txt
```

## Usage

Run the setup script:
```bash
python create_db.py
```

The script will:
1. Start a Firebird Docker container
2. Create a sample database
3. Create tables with appropriate schemas
4. Populate the database with sample data

## Database Schema

The script creates the following tables:

### employees
- Employee information including name, email, department, position, salary, and hire date
- 100 sample records

### products
- Product catalog with descriptions, categories, prices, and stock quantities
- 200 sample records

### customers
- Customer data with contact information and addresses
- 150 sample records

### orders
- Order records with customer information, dates, amounts, and status
- 300 sample records

### order_items
- Individual line items for orders with product details and quantities
- Variable number of items per order

## Connection Details

After setup, you can connect to the database using:
- **Host**: localhost
- **Port**: 3050
- **Database**: sample_db.fdb
- **User**: SYSDBA
- **Password**: masterkey

## Example Usage

```python
import fdb

# Connect to the database
con = fdb.connect(
    host='localhost',
    port=3050,
    database='/firebird/data/sample_db.fdb',
    user='SYSDBA',
    password='masterkey'
)

# Execute a query
cur = con.cursor()
cur.execute("SELECT first_name, last_name, department FROM employees WHERE department = 'Engineering'")
results = cur.fetchall()

for row in results:
    print(f"{row[0]} {row[1]} - {row[2]}")

# Close connection
con.close()
```

## Sample Queries

```sql
-- Get all employees in Engineering department
SELECT * FROM employees WHERE department = 'Engineering';

-- Get top 10 most expensive products
SELECT name, price FROM products ORDER BY price DESC ROWS 10;

-- Get orders with their total items count
SELECT o.id, o.customer_name, o.total_amount, COUNT(oi.id) as item_count
FROM orders o
LEFT JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.id, o.customer_name, o.total_amount;

-- Get customers from specific cities
SELECT * FROM customers WHERE city IN ('New York', 'Los Angeles');

-- Get product sales summary
SELECT p.name, p.category, SUM(oi.quantity) as total_sold
FROM products p
JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name, p.category
ORDER BY total_sold DESC;
```

## Docker Container Management

The script automatically manages the Docker container, but you can also control it manually:

```bash
# Stop the container
docker stop firebird-server

# Start the container
docker start firebird-server

# Remove the container
docker rm firebird-server

# View container logs
docker logs firebird-server
```

## Data Persistence

The database files are stored in the `./firebird_data` directory and are persisted between container restarts.

## Troubleshooting

1. **Container fails to start**: Ensure Docker is running and port 3050 is available
2. **Connection refused**: Wait a few seconds after container start for Firebird to initialize
3. **Permission errors**: Ensure the firebird_data directory has proper permissions
4. **Database creation fails**: Check if the container has sufficient resources and the data directory is writable

## Cleanup

To completely remove the setup:
```bash
docker stop firebird-server
docker rm firebird-server
rm -rf firebird_data
``` 