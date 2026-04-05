from pymongo import MongoClient, UpdateOne
from datetime import datetime
import sys
import os

# Add the project root to the Python path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Connect to local MongoDB (default port 27017)
client = MongoClient("mongodb://localhost:27017/")
db = client["social_media_brand_monitor"]

def save_to_mongo(data, collection_name="brand_mentions", metadata=None):
    """
    Save data to a MongoDB collection using an upsert strategy to prevent duplicates.
    - A unique identifier is created based on the source and page number.
    - If a document with that identifier exists, it's updated.
    - If not, a new document is inserted.
    """
    col = db[collection_name]
    
    base_metadata = {
        "extraction_timestamp": datetime.utcnow()
    }
    if metadata:
        base_metadata.update(metadata)

    operations = []

    def process_document(doc_content, page_num=None):
        """Prepares a single document for an upsert operation."""
        doc_metadata = base_metadata.copy()
        
        # Create a unique filter for the upsert operation
        # Use source (file path or URL) as the primary unique key
        if 'source' not in doc_metadata:
            logger.warning(f"Document is missing 'source' metadata for unique identification. Skipping. Content: {doc_content}")
            return None

        query_filter = {"source": doc_metadata["source"]}
        
        # If it's a multi-page document, add page_number to the filter
        if page_num is not None:
            doc_metadata["page_number"] = page_num
            query_filter["page_number"] = page_num

        # Combine content and metadata for the update operation
        update_data = {"$set": {**doc_content, **doc_metadata}}
        
        return UpdateOne(query_filter, update_data, upsert=True)

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                op = process_document(item)
                if op: operations.append(op)

    elif isinstance(data, dict):
        # Check if it's a dictionary of pages from OCR/PDF
        if all(isinstance(k, int) for k in data.keys()):
            for page_num, content in data.items():
                op = process_document({"text": content}, page_num=page_num)
                if op: operations.append(op)
        else:
            # It's a single document
            op = process_document(data)
            if op: operations.append(op)

    # Execute all the upsert operations in a single batch
    if operations:
        try:
            result = col.bulk_write(operations)
            upserted_count = result.upserted_count
            modified_count = result.modified_count
            logger.info(f"MongoDB write complete. New documents: {upserted_count}, Updated documents: {modified_count}.")
        except Exception as e:
            logger.error(f"Failed to perform bulk write to MongoDB: {e}")
    else:
        logger.warning("No valid documents were prepared for saving.")



