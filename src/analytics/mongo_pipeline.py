"""MongoDB aggregation pipelines for Apple brand-monitor analytics."""

from __future__ import annotations

import pandas as pd
from pymongo import MongoClient

from src.utils.logger import get_logger


logger = get_logger(__name__)

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "social_media_brand_monitor"
COLLECTION_NAME = "brand_mentions"


def build_source_mentions_pipeline(keyword: str = "apple") -> list[dict]:
    """Build a server-side aggregation pipeline for Apple mention counts by source."""
    return [
        {
            "$match": {
                "$or": [
                    {"title": {"$regex": keyword, "$options": "i"}},
                    {"description": {"$regex": keyword, "$options": "i"}},
                    {"content": {"$regex": keyword, "$options": "i"}},
                ]
            }
        },
        {
            "$group": {
                "_id": "$source",
                "mention_count": {"$sum": 1},
                "avg_rating": {"$avg": "$rating"},
            }
        },
        {"$sort": {"mention_count": -1}},
        {
            "$project": {
                "_id": 0,
                "source": {"$ifNull": ["$_id", "Unknown"]},
                "mention_count": 1,
                "avg_rating": 1,
            }
        },
    ]


def run_pipeline(
    pipeline: list[dict],
    mongo_uri: str = MONGO_URI,
    database_name: str = DATABASE_NAME,
    collection_name: str = COLLECTION_NAME,
) -> pd.DataFrame:
    """Execute a MongoDB aggregation pipeline and return a dataframe."""
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    collection = client[database_name][collection_name]
    results = list(collection.aggregate(pipeline))
    dataframe = pd.DataFrame(results)
    logger.info("MongoDB aggregation pipeline executed | rows=%s", len(dataframe))
    return dataframe
