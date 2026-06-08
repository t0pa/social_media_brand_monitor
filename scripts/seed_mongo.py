"""Seed MongoDB with the cleaned Apple brand monitoring dataset."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
from pymongo import ASCENDING, MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dashboard.data_access import DEFAULT_COLLECTION, DEFAULT_DB_NAME, DEFAULT_MONGO_URI, normalize_dashboard_frame


DEFAULT_CSV_PATH = PROJECT_ROOT / "data" / "processed" / "cleaned" / "cleaned_data.csv"


def seed_mongo(csv_path: str | Path = DEFAULT_CSV_PATH) -> int:
    """Replace the MongoDB collection with records from the cleaned CSV."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Cleaned CSV not found: {path}")

    dataframe = normalize_dashboard_frame(pd.read_csv(path))
    records = dataframe.where(pd.notna(dataframe), None).to_dict(orient="records")

    client = MongoClient(os.getenv("MONGO_URI", DEFAULT_MONGO_URI), serverSelectionTimeoutMS=5000)
    collection = client[os.getenv("MONGO_DB", DEFAULT_DB_NAME)][
        os.getenv("MONGO_COLLECTION", DEFAULT_COLLECTION)
    ]
    collection.delete_many({})
    if records:
        collection.insert_many(records)

    collection.create_index([("source", ASCENDING)])
    collection.create_index([("document_type", ASCENDING)])
    collection.create_index([("mention_year", ASCENDING)])
    collection.create_index([("title", ASCENDING)])
    return len(records)


if __name__ == "__main__":
    inserted = seed_mongo()
    print(f"Seeded {inserted} Apple brand mention records into MongoDB.")

