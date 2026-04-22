import os
import sys
from io import StringIO
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.utils.logger import get_logger


logger = get_logger(__name__)

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "social_media_brand_monitor"
COLLECTION_NAME = "brand_mentions"
BRAND_KEYWORD = "apple"
OUTPUT_DIR = Path("data/processed/apple_brand_eda")
CHARTS_DIR = OUTPUT_DIR / "charts"


def load_brand_dataset_from_mongodb(
    mongo_uri: str = MONGO_URI,
    database_name: str = DATABASE_NAME,
    collection_name: str = COLLECTION_NAME,
) -> pd.DataFrame:
    """Load all integrated brand-monitor records from MongoDB."""
    logger.info(
        "Apple brand EDA load started | database=%s | collection=%s",
        database_name,
        collection_name,
    )
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    collection = client[database_name][collection_name]
    documents = list(collection.find())
    logger.info("Apple brand EDA Mongo load complete | rows=%s", len(documents))

    if not documents:
        raise ValueError("No records were found in MongoDB.")

    return pd.json_normalize(documents)


def filter_apple_mentions(dataframe: pd.DataFrame, keyword: str = BRAND_KEYWORD) -> pd.DataFrame:
    """Keep rows that look related to Apple across source text fields."""
    working = dataframe.copy()

    search_columns = [column for column in ["title", "description", "content", "source"] if column in working.columns]
    if not search_columns:
        raise ValueError("No searchable text columns were found for Apple filtering.")

    mention_mask = pd.Series(False, index=working.index)
    for column in search_columns:
        mention_mask = mention_mask | working[column].fillna("").astype(str).str.contains(
            keyword, case=False, na=False
        )

    filtered = working[mention_mask].copy()
    logger.info(
        "Apple mention filter complete | keyword=%s | rows_before=%s | rows_after=%s",
        keyword,
        len(working),
        len(filtered),
    )

    if filtered.empty:
        raise ValueError(f"No Apple-related records were found using keyword '{keyword}'.")

    return filtered


def clean_brand_dataset(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Select the main columns and normalize fields for Apple brand-monitor EDA."""
    preferred_columns = [
        "title",
        "author",
        "description",
        "content",
        "source",
        "document_type",
        "type",
        "publishedAt",
        "date",
        "url",
    ]
    available_columns = [column for column in preferred_columns if column in dataframe.columns]
    cleaned = dataframe[available_columns].copy()

    for column in ["publishedAt", "date"]:
        if column in cleaned.columns:
            cleaned[column] = pd.to_datetime(cleaned[column], errors="coerce")

    for column in ["title", "author", "source", "document_type", "type"]:
        if column in cleaned.columns:
            cleaned[column] = cleaned[column].astype("string").str.strip()

    if "mention_date" not in cleaned.columns:
        if "publishedAt" in cleaned.columns:
            cleaned["mention_date"] = cleaned["publishedAt"].dt.date.astype("string")
        elif "date" in cleaned.columns:
            cleaned["mention_date"] = cleaned["date"].dt.date.astype("string")

    if "content" in cleaned.columns:
        cleaned["content_length"] = cleaned["content"].fillna("").astype(str).str.len()

    logger.info(
        "Apple dataset cleaned | rows=%s | columns=%s",
        len(cleaned),
        list(cleaned.columns),
    )
    return cleaned


def build_describe_table(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a version-safe describe table for mixed column types."""
    describe_table = dataframe.describe(include="all")

    datetime_columns = dataframe.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns
    for column in datetime_columns:
        series = dataframe[column].dropna()
        if not series.empty:
            describe_table.loc["min", column] = series.min()
            describe_table.loc["max", column] = series.max()

    return describe_table


def save_eda_text_report(dataframe: pd.DataFrame, output_dir: Path = OUTPUT_DIR) -> Path:
    """Save shape, info, describe, value counts, and unique counts to a text report."""
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "apple_brand_eda_report.txt"

    info_buffer = StringIO()
    dataframe.info(buf=info_buffer)
    describe_table = build_describe_table(dataframe)

    sections = [
        "Apple Brand Monitor EDA Report",
        "=" * 40,
        f"Shape: {dataframe.shape}",
        "",
        "Info:",
        info_buffer.getvalue(),
        "Describe:",
        describe_table.to_string(),
        "",
        "Nunique:",
        dataframe.nunique(dropna=True).to_string(),
    ]

    value_count_columns = [
        column
        for column in ["source", "document_type", "type", "author", "mention_date"]
        if column in dataframe.columns
    ]
    for column in value_count_columns:
        sections.extend(
            [
                "",
                f"Value counts for {column}:",
                dataframe[column].value_counts(dropna=False).head(15).to_string(),
            ]
        )

    report_path.write_text("\n".join(sections), encoding="utf-8")
    logger.info("Apple EDA text report saved | path=%s", report_path)
    return report_path


def create_brand_charts(dataframe: pd.DataFrame, charts_dir: Path = CHARTS_DIR) -> list[Path]:
    """Create and save key Apple mention distribution charts."""
    charts_dir.mkdir(parents=True, exist_ok=True)
    chart_paths: list[Path] = []

    if "source" in dataframe.columns and dataframe["source"].notna().any():
        plt.figure(figsize=(10, 5))
        dataframe["source"].value_counts().head(10).plot(kind="bar", color="#6c8ebf")
        plt.title("Top Apple Mention Sources")
        plt.xlabel("Source")
        plt.ylabel("Count")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        chart_path = charts_dir / "apple_mentions_by_source.png"
        plt.savefig(chart_path, dpi=150)
        plt.close()
        chart_paths.append(chart_path)
        logger.info("Saved chart | path=%s", chart_path)

    if "document_type" in dataframe.columns and dataframe["document_type"].notna().any():
        plt.figure(figsize=(7, 4))
        dataframe["document_type"].value_counts(dropna=False).plot(kind="bar", color="#7dbf8e")
        plt.title("Apple Mentions by Document Type")
        plt.xlabel("Document Type")
        plt.ylabel("Count")
        plt.tight_layout()
        chart_path = charts_dir / "apple_mentions_by_document_type.png"
        plt.savefig(chart_path, dpi=150)
        plt.close()
        chart_paths.append(chart_path)
        logger.info("Saved chart | path=%s", chart_path)

    if "mention_date" in dataframe.columns and dataframe["mention_date"].notna().any():
        plt.figure(figsize=(9, 4))
        dataframe["mention_date"].value_counts().sort_index().plot(kind="line", marker="o", color="#d97b66")
        plt.title("Apple Mentions Over Time")
        plt.xlabel("Date")
        plt.ylabel("Count")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        chart_path = charts_dir / "apple_mentions_over_time.png"
        plt.savefig(chart_path, dpi=150)
        plt.close()
        chart_paths.append(chart_path)
        logger.info("Saved chart | path=%s", chart_path)

    if "author" in dataframe.columns and dataframe["author"].notna().any():
        top_authors = dataframe["author"].value_counts().head(10)
        plt.figure(figsize=(9, 5))
        top_authors.plot(kind="bar", color="#c59d5f")
        plt.title("Top Authors Covering Apple")
        plt.xlabel("Author")
        plt.ylabel("Count")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        chart_path = charts_dir / "top_authors_covering_apple.png"
        plt.savefig(chart_path, dpi=150)
        plt.close()
        chart_paths.append(chart_path)
        logger.info("Saved chart | path=%s", chart_path)

    return chart_paths


def print_eda_summary(dataframe: pd.DataFrame) -> None:
    """Print the core EDA items requested in the assignment."""
    describe_table = build_describe_table(dataframe)
    print(f"Shape: {dataframe.shape}")
    print("\nInfo:")
    dataframe.info()
    print("\nDescribe:")
    print(describe_table)
    print("\nNunique:")
    print(dataframe.nunique(dropna=True))

    for column in ["source", "document_type", "type", "author", "mention_date"]:
        if column in dataframe.columns:
            print(f"\nValue counts for {column}:")
            print(dataframe[column].value_counts(dropna=False).head(15))


def main() -> None:
    logger.info("Apple brand EDA pipeline started")
    dataframe = load_brand_dataset_from_mongodb()
    apple_mentions = filter_apple_mentions(dataframe)
    cleaned = clean_brand_dataset(apple_mentions)
    report_path = save_eda_text_report(cleaned)
    chart_paths = create_brand_charts(cleaned)
    print_eda_summary(cleaned)
    print(f"\nSaved EDA report to: {report_path}")
    print("Saved charts:")
    for chart_path in chart_paths:
        print(f"- {chart_path}")
    logger.info(
        "Apple brand EDA pipeline finished | report=%s | charts=%s",
        report_path,
        [str(path) for path in chart_paths],
    )


if __name__ == "__main__":
    main()
