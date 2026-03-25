from pymongo import MongoClient
from datetime import datetime

# Connect to local MongoDB (default port 27017)
client = MongoClient("mongodb://localhost:27017/")
db = client["social_media_brand_monitor"]

def save_to_mongo(data, collection_name="raw_data", metadata=None):
    """
    Save data to MongoDB collection. Accepts a dict or list of dicts.
    Adds a timestamp and optional metadata to each document.
    """
    col = db[collection_name]
    
    # Prepare the base metadata to be added to each document
    base_metadata = {
        "extraction_timestamp": datetime.utcnow()
    }
    if metadata:
        base_metadata.update(metadata)

    if isinstance(data, list):
        documents_to_insert = []
        for doc in data:
            # Ensure doc is a dictionary to avoid errors with non-dict items
            if isinstance(doc, dict):
                doc.update(base_metadata)
                documents_to_insert.append(doc)
        if documents_to_insert:
            col.insert_many(documents_to_insert)
    elif isinstance(data, dict):
        data.update(base_metadata)
        col.insert_one(data)
    # If data is neither a list nor a dict, we might log an error or handle it as needed
    # For now, we'll just ignore it to prevent crashes

