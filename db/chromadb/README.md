# ChromaDB Setup Script

This script sets up a ChromaDB vector database in a Docker container and populates it with sample data.

## Prerequisites

- Docker installed and running
- Python 3.7 or higher
- pip package manager

## Installation

1. Install the required Python packages:
```bash
pip install -r requirements.txt
```

## Usage

Run the script to start ChromaDB and populate it with sample data:

```bash
python3 create_db.py
```

## What the script does

1. **Container Management**: Stops and removes any existing ChromaDB container
2. **Docker Setup**: Starts a new ChromaDB container with:
   - Port 8000 exposed for HTTP API
   - Persistent data storage in `./chromadb_data`
3. **Collections**: Creates three sample collections:
   - `documents`: General document collection (100 items)
   - `products`: Product information collection (50 items)
   - `articles`: Article and research collection (30 items)
4. **Sample Data**: Populates each collection with realistic sample data including:
   - Document text content
   - Metadata (categories, authors, dates, tags, etc.)
   - Various topics and categories

## Connection Details

- **Host**: localhost
- **Port**: 8000
- **URL**: http://localhost:8000
- **Data Path**: ./chromadb_data

## Example Usage

After running the script, you can connect to ChromaDB:

```python
import chromadb

# Connect to ChromaDB
client = chromadb.HttpClient(host='localhost', port=8000)

# Get a collection
collection = client.get_collection('documents')

# Query the collection
results = collection.query(
    query_texts=['artificial intelligence'],
    n_results=5
)

print(results)
```

## Sample Collections

### Documents Collection
- 100 documents about various topics (technology, science, business, health, education)
- Metadata includes: category, author, created_at, tags

### Products Collection
- 50 product descriptions for electronics and accessories
- Metadata includes: name, category, price, brand, rating, in_stock

### Articles Collection
- 30 articles on technology and research topics
- Metadata includes: title, author, publication_date, word_count, category, difficulty

## Troubleshooting

- Ensure Docker is running before executing the script
- Check that port 8000 is available
- If the container fails to start, check Docker logs: `docker logs chromadb-server`
- To reset everything, run the script again (it will clean up and recreate the container)

## Cleanup

To stop and remove the ChromaDB container:

```bash
docker stop chromadb-server
docker rm chromadb-server
```

To remove the data directory:

```bash
rm -rf ./chromadb_data
``` 