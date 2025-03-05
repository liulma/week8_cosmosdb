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
def query_mean(container):
    avg_query = "SELECT VALUE AVG(c.price) FROM c"

    try:
        # Execute query and fetch results
        results = list(container.query_items(query=avg_query, enable_cross_partition_query=True))

        if results:
            mean_price = results[0] # When you execute a query, the result is a list of items. You need to extract the value properly from the query result.
            print(f"Mean Price: {mean_price}")
        else:
            print("No results found.")

    except Exception as e:
        print(f"Error executing query: {e}")

def query_max_min(container):
    max_query = "SELECT VALUE MAX(c.price) FROM c"
    min_query = "SELECT VALUE MIN(c.price) FROM c"

    try:
        max_price_result = list(container.query_items(query=max_query, enable_cross_partition_query=True))
        min_price_result = list(container.query_items(query=min_query, enable_cross_partition_query=True))

        if max_price_result and min_price_result:
            max_price = max_price_result[0]
            min_price = min_price_result[0]

            print(f"Max price: {max_price}")
            print(f"Min price: {min_price}")
        else:
            print("No results found.")

    except Exception as e:
        print(f"Error executing query: {e}")

def main():
    # Get the Cosmos DB container
    container = get_cosmos_client()

    # Load and insert data
    query_mean(container)
    query_max_min(container)

# Run the script
if __name__ == "__main__":
    main()