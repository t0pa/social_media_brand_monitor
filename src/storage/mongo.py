from pymongo import MongoClient
from datetime import datetime


# Connect to local MongoDB (default port 27017)
client = MongoClient("mongodb://localhost:27017/")
db = client["social_media_brand_monitor"]
collection = db["raw_data"]

def save_to_mongo(data, collection_name="raw_data"):
	"""
	Save data to MongoDB collection. Accepts a dict or list of dicts.
	Adds a timestamp to each document.
	"""
	col = db[collection_name]
	if isinstance(data, list):
		for doc in data:
			doc["inserted_at"] = datetime.utcnow()
		col.insert_many(data)
	else:
		data["inserted_at"] = datetime.utcnow()
		col.insert_one(data)
