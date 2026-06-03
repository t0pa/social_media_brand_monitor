"""Data access helpers for the Apple brand monitoring dashboard."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import pandas as pd
from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV_PATH = PROJECT_ROOT / "data" / "processed" / "cleaned" / "cleaned_data.csv"
DEFAULT_MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DEFAULT_DB_NAME = os.getenv("MONGO_DB", "brand_monitor")
DEFAULT_COLLECTION = os.getenv("MONGO_COLLECTION", "apple_mentions")


def get_collection(
    mongo_uri: str | None = None,
    db_name: str | None = None,
    collection_name: str | None = None,
):
    """Return the configured MongoDB collection after a fast connection check."""
    client = MongoClient(
        mongo_uri or DEFAULT_MONGO_URI,
        serverSelectionTimeoutMS=1500,
        connectTimeoutMS=1500,
    )
    client.admin.command("ping")
    return client[db_name or DEFAULT_DB_NAME][collection_name or DEFAULT_COLLECTION]


def normalize_dashboard_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Normalize dashboard columns and dtypes used by callbacks."""
    df = dataframe.copy()

    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    for column in ["source", "document_type", "title", "author", "language", "type"]:
        if column not in df.columns:
            df[column] = "Unknown"
        df[column] = df[column].fillna("Unknown").astype(str)

    if "rating" not in df.columns:
        df["rating"] = pd.NA
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    date_candidates = ["mention_date", "publishedAt", "record_date", "date", "extraction_timestamp"]
    selected_date = next((column for column in date_candidates if column in df.columns), None)
    if selected_date is None:
        df["mention_date"] = pd.NaT
    else:
        df["mention_date"] = pd.to_datetime(df[selected_date], errors="coerce", utc=True)

    if "mention_year" not in df.columns:
        df["mention_year"] = df["mention_date"].dt.year
    df["mention_year"] = pd.to_numeric(df["mention_year"], errors="coerce")
    if df["mention_year"].isna().all():
        df["mention_year"] = pd.Timestamp.utcnow().year
    df["mention_year"] = df["mention_year"].fillna(df["mention_year"].median()).astype(int)

    df["title_search"] = df["title"].fillna("").astype(str).str.lower()
    df["content_search"] = (
        df.get("content", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    )
    return df


def load_mentions_from_mongo() -> pd.DataFrame:
    """Load Apple mention records from MongoDB."""
    collection = get_collection()
    records = list(collection.find({}))
    if not records:
        raise RuntimeError("MongoDB collection is empty")
    return normalize_dashboard_frame(pd.DataFrame(records))


def load_mentions_from_csv(csv_path: str | Path = DEFAULT_CSV_PATH) -> pd.DataFrame:
    """Load Apple mention records from the cleaned CSV fallback."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Cleaned dashboard CSV not found: {path}")
    return normalize_dashboard_frame(pd.read_csv(path))


def load_mentions() -> pd.DataFrame:
    """Load dashboard data from MongoDB, falling back to the cleaned CSV."""
    try:
        return load_mentions_from_mongo()
    except (PyMongoError, ServerSelectionTimeoutError, RuntimeError, OSError):
        return load_mentions_from_csv()


def get_source_options(dataframe: pd.DataFrame | None = None) -> list[dict[str, str]]:
    """Return dropdown options for available data sources."""
    df = dataframe if dataframe is not None else load_mentions()
    values = sorted(value for value in df["source"].dropna().astype(str).unique() if value)
    return [{"label": "All sources", "value": "All"}] + [{"label": value, "value": value} for value in values]


def get_document_type_options(dataframe: pd.DataFrame | None = None) -> list[dict[str, str]]:
    """Return dropdown options for document types."""
    df = dataframe if dataframe is not None else load_mentions()
    values = sorted(value for value in df["document_type"].dropna().astype(str).unique() if value)
    return [{"label": "All document types", "value": "All"}] + [
        {"label": value, "value": value} for value in values
    ]


def get_year_bounds(dataframe: pd.DataFrame | None = None) -> tuple[int, int]:
    """Return the minimum and maximum mention years."""
    df = dataframe if dataframe is not None else load_mentions()
    years = pd.to_numeric(df["mention_year"], errors="coerce").dropna()
    if years.empty:
        current_year = pd.Timestamp.utcnow().year
        return current_year, current_year
    return int(years.min()), int(years.max())


def filter_mentions(
    source: str | None = "All",
    document_type: str | None = "All",
    year_range: Iterable[int] | None = None,
    search_text: str | None = None,
    dataframe: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Filter mention records by source, document type, year range, and text search."""
    df = dataframe if dataframe is not None else load_mentions()
    filtered = df.copy()

    if source and source != "All":
        filtered = filtered[filtered["source"] == source]

    if document_type and document_type != "All":
        filtered = filtered[filtered["document_type"] == document_type]

    if year_range:
        years = list(year_range)
        if len(years) == 2:
            start_year, end_year = int(years[0]), int(years[1])
            filtered = filtered[
                (filtered["mention_year"] >= start_year) & (filtered["mention_year"] <= end_year)
            ]

    if search_text:
        query = search_text.strip().lower()
        if query:
            filtered = filtered[
                filtered["title_search"].str.contains(query, regex=False)
                | filtered["content_search"].str.contains(query, regex=False)
            ]

    return filtered


def ensure_indexes() -> None:
    """Create useful indexes for dashboard filtering and search."""
    collection = get_collection()
    collection.create_index([("source", ASCENDING)])
    collection.create_index([("document_type", ASCENDING)])
    collection.create_index([("mention_year", ASCENDING)])
    collection.create_index([("title", ASCENDING)])

