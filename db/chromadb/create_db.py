#!/usr/bin/env python3
"""
ChromaDB Database Setup Script
This script creates a ChromaDB Docker container and populates it with sample data.
"""

import subprocess
import time
import requests
import json
import sys
from datetime import datetime, timedelta
import random
import chromadb
from chromadb.config import Settings

# Docker configuration
CONTAINER_NAME = "chromadb-server"
CHROMADB_HOST = "localhost"
CHROMADB_PORT = 8000
CHROMADB_DATA_PATH = "./chromadb_data"

# ChromaDB connection details
CHROMADB_URL = f"http://{CHROMADB_HOST}:{CHROMADB_PORT}"

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

def wait_for_chromadb():
    """Wait for ChromaDB to be ready."""
    print("Waiting for ChromaDB to be ready...")
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            response = requests.get(f"{CHROMADB_URL}/api/v1/heartbeat", timeout=5)
            if response.status_code == 200:
                print("ChromaDB is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"Attempt {attempt + 1}/{max_attempts} - ChromaDB not ready yet...")
        time.sleep(2)
    
    print("ChromaDB failed to start within the expected time")
    return False

def get_chromadb_client():
    """Get ChromaDB client."""
    try:
        client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT)
        return client
    except Exception as e:
        print(f"Failed to connect to ChromaDB: {e}")
        return None

def create_collections():
    """Create sample collections in ChromaDB."""
    print("Creating ChromaDB collections...")
    
    client = get_chromadb_client()
    if not client:
        return False
    
    try:
        # Create collections
        print("Creating 'documents' collection...")
        documents_collection = client.create_collection(
            name="documents",
            metadata={"description": "Sample document collection"}
        )
        
        print("Creating 'products' collection...")
        products_collection = client.create_collection(
            name="products",
            metadata={"description": "Sample product collection"}
        )
        
        print("Creating 'articles' collection...")
        articles_collection = client.create_collection(
            name="articles",
            metadata={"description": "Sample article collection"}
        )
        
        print("Collections created successfully!")
        return True
        
    except Exception as e:
        print(f"Failed to create collections: {e}")
        return False

def generate_sample_data():
    """Generate sample data for ChromaDB collections."""
    print("Generating sample data...")
    
    # Sample documents
    documents_data = {
        "ids": [f"doc_{i}" for i in range(1, 101)],
        "documents": [
            f"This is document {i} containing information about topic {random.choice(['technology', 'science', 'business', 'health', 'education'])}. "
            f"It discusses various aspects of {random.choice(['artificial intelligence', 'machine learning', 'data science', 'cloud computing', 'cybersecurity'])} "
            f"and provides insights into {random.choice(['best practices', 'implementation strategies', 'future trends', 'challenges', 'opportunities'])}."
            for i in range(1, 101)
        ],
        "metadatas": [
            {
                "category": random.choice(['technology', 'science', 'business', 'health', 'education']),
                "author": f"Author {random.randint(1, 20)}",
                "created_at": (datetime.now() - timedelta(days=random.randint(1, 365))).isoformat(),
                "tags": random.sample(['ai', 'ml', 'data', 'cloud', 'security', 'innovation', 'research'], k=random.randint(1, 3))
            }
            for i in range(1, 101)
        ]
    }
    
    # Sample products
    product_names = [
        "Wireless Headphones", "Smart Watch", "Laptop Stand", "Bluetooth Speaker",
        "USB-C Hub", "Mechanical Keyboard", "Ergonomic Mouse", "Monitor Stand",
        "Desk Lamp", "Phone Charger", "Tablet Case", "Webcam", "Microphone",
        "Gaming Chair", "Standing Desk", "Cable Organizer", "Power Bank",
        "Wireless Mouse", "Keyboard Wrist Rest", "Monitor Light Bar"
    ]
    
    products_data = {
        "ids": [f"product_{i}" for i in range(1, 51)],
        "documents": [
            f"{random.choice(product_names)} - {random.choice(['Premium', 'Professional', 'Standard', 'Deluxe', 'Basic'])} "
            f"model with {random.choice(['advanced features', 'ergonomic design', 'high-quality materials', 'innovative technology', 'user-friendly interface'])}. "
            f"Perfect for {random.choice(['office work', 'gaming', 'home use', 'professional tasks', 'creative projects'])}."
            for i in range(1, 51)
        ],
        "metadatas": [
            {
                "name": random.choice(product_names),
                "category": random.choice(['electronics', 'accessories', 'furniture', 'audio', 'computing']),
                "price": round(random.uniform(10.0, 500.0), 2),
                "brand": f"Brand {random.choice(['A', 'B', 'C', 'D', 'E'])}",
                "rating": round(random.uniform(3.0, 5.0), 1),
                "in_stock": random.choice([True, False])
            }
            for i in range(1, 51)
        ]
    }
    
    # Sample articles
    article_topics = [
        "The Future of Artificial Intelligence",
        "Machine Learning in Healthcare",
        "Cybersecurity Best Practices",
        "Cloud Computing Trends",
        "Data Science Applications",
        "Remote Work Technologies",
        "Sustainable Technology Solutions",
        "Digital Transformation Strategies",
        "IoT and Smart Cities",
        "Blockchain Applications"
    ]
    
    articles_data = {
        "ids": [f"article_{i}" for i in range(1, 31)],
        "documents": [
            f"{random.choice(article_topics)}: "
            f"This comprehensive article explores {random.choice(['recent developments', 'key challenges', 'emerging trends', 'practical applications', 'future implications'])} "
            f"in the field. It covers {random.choice(['technical aspects', 'business implications', 'user experiences', 'implementation strategies', 'case studies'])} "
            f"and provides valuable insights for {random.choice(['professionals', 'researchers', 'students', 'decision makers', 'practitioners'])}."
            for i in range(1, 31)
        ],
        "metadatas": [
            {
                "title": random.choice(article_topics),
                "author": f"Dr. {random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis'])}",
                "publication_date": (datetime.now() - timedelta(days=random.randint(1, 180))).isoformat(),
                "word_count": random.randint(800, 3000),
                "category": random.choice(['technology', 'research', 'industry', 'academic', 'news']),
                "difficulty": random.choice(['beginner', 'intermediate', 'advanced'])
            }
            for i in range(1, 31)
        ]
    }
    
    return documents_data, products_data, articles_data

def insert_data(collection_name, data):
    """Insert data into a ChromaDB collection."""
    print(f"Inserting data into '{collection_name}' collection...")
    
    client = get_chromadb_client()
    if not client:
        return False
    
    try:
        collection = client.get_collection(collection_name)
        
        # Insert data in batches to avoid potential issues with large datasets
        batch_size = 10
        total_items = len(data["ids"])
        
        for i in range(0, total_items, batch_size):
            end_idx = min(i + batch_size, total_items)
            batch_ids = data["ids"][i:end_idx]
            batch_documents = data["documents"][i:end_idx]
            batch_metadatas = data["metadatas"][i:end_idx]
            
            collection.add(
                ids=batch_ids,
                documents=batch_documents,
                metadatas=batch_metadatas
            )
            
            print(f"Inserted batch {i//batch_size + 1}/{(total_items + batch_size - 1)//batch_size}")
        
        print(f"Successfully inserted {total_items} items into '{collection_name}' collection")
        return True
        
    except Exception as e:
        print(f"Failed to insert data into '{collection_name}' collection: {e}")
        return False

def main():
    """Main function to set up ChromaDB and populate with sample data."""
    print("Starting ChromaDB setup...")
    
    # Check if container already exists
    existing_container = run_command(f"docker ps -a --filter name={CONTAINER_NAME} --format '{{{{.Names}}}}'", check=False)
    
    if existing_container:
        print(f"Container {CONTAINER_NAME} already exists. Stopping and removing it...")
        run_command(f"docker stop {CONTAINER_NAME}", check=False)
        run_command(f"docker rm {CONTAINER_NAME}", check=False)
    
    # Create data directory
    run_command(f"mkdir -p {CHROMADB_DATA_PATH}", check=False)
    
    # Start ChromaDB Docker container
    print("Starting ChromaDB Docker container...")
    docker_command = f"""docker run -d \
  --name {CONTAINER_NAME} \
  -p {CHROMADB_PORT}:{CHROMADB_PORT} \
  -v {CHROMADB_DATA_PATH}:/chroma/chroma \
  chromadb/chroma:latest"""
    
    result = run_command(docker_command)
    if result:
        print(f"ChromaDB container started successfully: {result}")
    else:
        print("Failed to start ChromaDB container")
        return False
    
    # Wait for ChromaDB to be ready
    if not wait_for_chromadb():
        return False
    
    # Create collections
    if not create_collections():
        return False
    
    # Generate sample data
    documents_data, products_data, articles_data = generate_sample_data()
    
    # Insert data into collections
    if not insert_data("documents", documents_data):
        return False
    
    if not insert_data("products", products_data):
        return False
    
    if not insert_data("articles", articles_data):
        return False
    
    print("\n" + "="*50)
    print("ChromaDB setup completed successfully!")
    print(f"Host: {CHROMADB_HOST}")
    print(f"Port: {CHROMADB_PORT}")
    print(f"URL: {CHROMADB_URL}")
    print(f"Data Path: {CHROMADB_DATA_PATH}")
    print("\nSample data inserted:")
    print(f"- {len(documents_data['ids'])} documents")
    print(f"- {len(products_data['ids'])} products")
    print(f"- {len(articles_data['ids'])} articles")
    print("\nCollections created:")
    print("- documents: General document collection")
    print("- products: Product information collection")
    print("- articles: Article and research collection")
    print("\nYou can now connect to ChromaDB and start querying the vector database!")
    print("Example usage:")
    print("  import chromadb")
    print(f"  client = chromadb.HttpClient(host='{CHROMADB_HOST}', port={CHROMADB_PORT})")
    print("  collection = client.get_collection('documents')")
    print("  results = collection.query(query_texts=['artificial intelligence'], n_results=5)")
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