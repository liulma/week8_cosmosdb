from azure.cosmos import CosmosClient
from config import config

# Fetch Cosmos DB connection details
def get_cosmos_client():
    db_config = config()
    endpoint = db_config['endpoint']
    key = db_config['key']
    database_name = db_config['database_name']
    container_name = db_config['container_name']
    
    client = CosmosClient(endpoint, key)
    database = client.get_database_client(database_name)
    container = database.get_container_client(container_name)
    
    return container

# Create (Insert) Document
def create_item(container, item):
    try:
        response = container.create_item(body=item)
        print(f"Item created: {response['id']}")
    except Exception as e:
        print(f"Error creating item: {e}")

# Update Document
def update_item(container, item_id, updated_data):
    try:
        # Read the document first (you could fetch by id or some other unique field)
        query = f"SELECT * FROM c WHERE c.id = '{item_id}'"
        results = list(container.query_items(query=query, enable_cross_partition_query=True))

        if results:
            item = results[0]  # Assuming only one document with the same id
            # Update fields
            item.update(updated_data)
            # Replace the document
            response = container.replace_item(item['id'], body=item)
            print(f"Item updated: {response['id']}")
        else:
            print("Item not found for update.")
    
    except Exception as e:
        print(f"Error updating item: {e}")

# Delete Document
def delete_item(container, item_id, partition_key_value):
    try:
        container.delete_item(item_id, partition_key=partition_key_value)  # Here, partition_key can be used if your container is partitioned
        print(f"Item deleted: {item_id}")
    except Exception as e:
        print(f"Error deleting item: {e}")

def main():
    container = get_cosmos_client()

    # Sample item (Product)
    product = {
        "id": "50",
        "title": "Smartphone",
        "category": "Electronics",
        "stock": 150,
        "price": 399.99
    }

    #create_item(container, product)

    item_id = "50"
    updated_data = {
        "price": 350.00,  # Updating the price
        "stock": 100      # Updating the stock
    }
    #update_item(container, item_id, updated_data)

    partition_key_value = "Electronics"

    delete_item(container, item_id, partition_key_value)

# Run the script
if __name__ == "__main__":
    main()
