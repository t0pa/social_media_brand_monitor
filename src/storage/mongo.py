from pymongo import MongoClient, UpdateOne
from datetime import datetime
import sys
import os
import hashlib
import json

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
    - A stable unique filter is created per record (source+url, source+page, source+title/date, or source+hash).
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

    def build_query_filter(doc_content, doc_metadata, page_num=None):
        """Build a stable upsert filter so one source file can store many records safely."""
        source = doc_metadata.get("source")
        if not source:
            return None, None

        existing_id = doc_content.get("_id")
        if existing_id is not None and str(existing_id).strip() != "":
            return {"_id": existing_id}, None

        if page_num is not None:
            doc_metadata["page_number"] = page_num
            return {"source": source, "page_number": page_num}, None

        url_value = str(doc_content.get("url", "")).strip()
        if url_value:
            return {"source": source, "url": url_value}, None

        title_value = str(doc_content.get("title", "")).strip()
        author_value = str(doc_content.get("author", "")).strip()
        date_value = str(
            doc_content.get("publishedAt")
            or doc_content.get("date")
            or doc_content.get("mention_date")
            or ""
        ).strip()
        if title_value and (author_value or date_value):
            return {
                "source": source,
                "title": title_value,
                "author": author_value,
                "record_date": date_value,
            }, {"record_date": date_value}

        # Last-resort identifier for records without URL/title/date.
        payload = json.dumps(doc_content, sort_keys=True, default=str, ensure_ascii=False)
        content_hash = hashlib.sha1(payload.encode("utf-8")).hexdigest()
        doc_metadata["content_hash"] = content_hash
        return {"source": source, "content_hash": content_hash}, None

    def process_document(doc_content, page_num=None):
        """Prepares a single document for an upsert operation."""
        doc_metadata = base_metadata.copy()

        query_filter, extra_update_fields = build_query_filter(doc_content, doc_metadata, page_num=page_num)
        if query_filter is None:
            logger.warning(
                f"Document is missing 'source' metadata for unique identification. Skipping. Content: {doc_content}"
            )
            return None

        # Combine content and metadata for the update operation
        update_fields = {**doc_content, **doc_metadata}
        # Never attempt to update Mongo's immutable _id field.
        update_fields.pop("_id", None)
        if extra_update_fields:
            update_fields.update(extra_update_fields)
        update_data = {"$set": update_fields}
        
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



