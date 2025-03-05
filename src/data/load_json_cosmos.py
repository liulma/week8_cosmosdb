from azure.cosmos import CosmosClient
import requests
from config import config

# Fetch the Cosmos DB connection details
def get_cosmos_client():
    # Fetch Cosmos DB connection details using the config function
    db_config = config()

    # Extract the connection details
    endpoint = db_config['endpoint']
    key = db_config['key']
    database_name = db_config['database_name']
    container_name = db_config['container_name']

    # Initialize Cosmos DB client
    client = CosmosClient(endpoint, key)

    # Connect to the existing database and container
    database = client.get_database_client(database_name)
    container = database.get_container_client(container_name)

    return container

# Function to load data from an API and insert into Cosmos DB
def load_data(container):
    for i in range(4, 50):
        response = requests.get(f"https://dummyjson.com/products/{i}")
        
        if response.status_code == 200:
            product = response.json()

            # Ensure that the id is a string
            product['id'] = str(product['id'])

            # Insert into Cosmos DB
            container.create_item(body=product)
            print(f"Inserted product {product['id']} - {product['title']}")
        else:
            print(f"Failed to fetch product {i}.")

def main():
    # Get the Cosmos DB container
    container = get_cosmos_client()

    # Load and insert data
    load_data(container)

    print("Data loaded successfully!")

# Run the script
if __name__ == "__main__":
    main()
