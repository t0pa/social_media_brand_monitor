import os
import sys
from pathlib import Path
from collections import defaultdict

import pandas as pd
from pymongo import MongoClient

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.utils.logger import get_logger


logger = get_logger(__name__)

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "social_media_brand_monitor"
COLLECTION_NAME = "brand_mentions"
EXPORT_PATH = Path("data/raw/csv/raw_brand_mentions.csv")
DEFAULT_CHUNK_CSV_PATH = Path("data/raw/csv/apple_ratings_large.csv")


def load_from_mongodb(
    mongo_uri: str = MONGO_URI,
    database_name: str = DATABASE_NAME,
    collection_name: str = COLLECTION_NAME,
) -> pd.DataFrame:
    """Load raw documents from MongoDB into a pandas DataFrame."""
    logger.info(
        "Loading raw data from MongoDB | database=%s | collection=%s",
        database_name,
        collection_name,
    )

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    collection = client[database_name][collection_name]
    documents = list(collection.find())

    logger.info("MongoDB load complete | documents_loaded=%s", len(documents))

    if not documents:
        logger.warning("MongoDB collection is empty; returning an empty DataFrame")
        return pd.DataFrame()

    dataframe = pd.json_normalize(documents)
    logger.info(
        "Data normalized into DataFrame | rows=%s | columns=%s",
        len(dataframe),
        len(dataframe.columns),
    )
    return dataframe


def export_raw_csv(dataframe: pd.DataFrame, export_path: Path = EXPORT_PATH) -> Path:
    """Export the raw MongoDB DataFrame to CSV."""
    export_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(export_path, index=False, encoding="utf-8")
    logger.info(
        "Raw CSV export complete | path=%s | rows=%s | columns=%s",
        export_path,
        len(dataframe),
        len(dataframe.columns),
    )
    return export_path

#testing csv file creation and chunked processing for lab requirement 4.2
def create_large_ratings_csv(
    export_path: Path = DEFAULT_CHUNK_CSV_PATH,
    row_count: int = 5000,
) -> Path:
    """Create a large Apple-themed CSV with a numeric rating column for chunk processing."""
    export_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    authors = ["John Doe", "Jane Smith", "Alex Brown", "Sara Lee"]
    languages = ["en", "de", "fr", "es"]
    titles = [
        "Apple iPhone review",
        "Apple Watch battery test",
        "MacBook Air student feedback",
        "AirPods daily use impressions",
    ]

    for index in range(row_count):
        rows.append(
            {
                "title": titles[index % len(titles)],
                "author": authors[index % len(authors)],
                "language": languages[index % len(languages)],
                "date": f"2026-04-{(index % 28) + 1:02d}",
                "rating": float((index % 5) + 1),
            }
        )

    dataframe = pd.DataFrame(rows)
    dataframe.to_csv(export_path, index=False, encoding="utf-8")
    logger.info(
        "Large ratings CSV created | path=%s | rows=%s | columns=%s",
        export_path,
        len(dataframe),
        len(dataframe.columns),
    )
    return export_path


def optimize_dataframe_dtypes(csv_path: Path) -> tuple[pd.DataFrame, float, float]:
    """Optimize DataFrame dtypes and log memory usage before and after in MB."""
    dataframe = pd.read_csv(csv_path)
    before_mb = dataframe.memory_usage(deep=True).sum() / (1024 * 1024)

    optimized = dataframe.copy()
    object_columns = optimized.select_dtypes(include=["object"]).columns
    for column in object_columns:
        unique_ratio = optimized[column].nunique(dropna=False) / max(len(optimized), 1)
        if unique_ratio < 0.5:
            optimized[column] = optimized[column].astype("category")

    float_columns = optimized.select_dtypes(include=["float64"]).columns
    for column in float_columns:
        optimized[column] = pd.to_numeric(optimized[column], downcast="float")

    int_columns = optimized.select_dtypes(include=["int64"]).columns
    for column in int_columns:
        optimized[column] = pd.to_numeric(optimized[column], downcast="integer")

    after_mb = optimized.memory_usage(deep=True).sum() / (1024 * 1024)
    logger.info(
        "Dtype optimization complete | path=%s | memory_before_mb=%.4f | memory_after_mb=%.4f | saved_mb=%.4f",
        csv_path,
        before_mb,
        after_mb,
        before_mb - after_mb,
    )
    logger.info("Optimized dtypes | dtypes=%s", optimized.dtypes.astype(str).to_dict())
    return optimized, before_mb, after_mb


def compute_global_mean_from_chunks(
    csv_path: Path,
    rating_column: str = "rating",
    chunk_size: int = 1000,
) -> float:
    """Load a CSV in chunks and compute the global mean of the rating column."""
    logger.info(
        "Chunked CSV mean calculation started | path=%s | rating_column=%s | chunk_size=%s",
        csv_path,
        rating_column,
        chunk_size,
    )

    total_sum = 0.0
    total_count = 0
    processed_chunks = 0

    for chunk_index, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size), start=1):
        processed_chunks += 1

        if rating_column not in chunk.columns:
            available_columns = ", ".join(chunk.columns.astype(str))
            logger.error(
                "Rating column missing during chunk processing | expected=%s | available=%s",
                rating_column,
                available_columns,
            )
            raise KeyError(
                f"Column '{rating_column}' was not found in {csv_path}. "
                f"Available columns: {available_columns}"
            )

        ratings = pd.to_numeric(chunk[rating_column], errors="coerce").dropna()
        chunk_sum = float(ratings.sum())
        chunk_count = int(ratings.count())

        total_sum += chunk_sum
        total_count += chunk_count

        logger.info(
            "Processed chunk %s | valid_ratings=%s | chunk_sum=%.4f",
            chunk_index,
            chunk_count,
            chunk_sum,
        )

    if processed_chunks == 0:
        logger.warning("No chunks were processed because the CSV file is empty")
        raise ValueError(f"The file '{csv_path}' is empty.")

    if total_count == 0:
        logger.warning("No numeric rating values were found across processed chunks")
        raise ValueError(f"No numeric values were found in column '{rating_column}'.")

    global_mean = total_sum / total_count
    logger.info(
        "Chunked CSV mean calculation finished | chunks=%s | total_values=%s | global_mean=%.4f",
        processed_chunks,
        total_count,
        global_mean,
    )
    return global_mean


def compute_per_language_mean_from_chunks(
    csv_path: Path,
    rating_column: str = "rating",
    language_column: str = "language",
    chunk_size: int = 1000,
) -> dict[str, float]:
    """Process CSV chunks per language and combine sum/count accumulators."""
    logger.info(
        "Per-language chunked mean calculation started | path=%s | rating_column=%s | language_column=%s | chunk_size=%s",
        csv_path,
        rating_column,
        language_column,
        chunk_size,
    )

    sum_accumulator = defaultdict(float)
    count_accumulator = defaultdict(int)
    processed_chunks = 0

    for chunk_index, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size), start=1):
        processed_chunks += 1

        missing_columns = [
            column for column in (rating_column, language_column) if column not in chunk.columns
        ]
        if missing_columns:
            available_columns = ", ".join(chunk.columns.astype(str))
            logger.error(
                "Required columns missing during per-language chunk processing | missing=%s | available=%s",
                missing_columns,
                available_columns,
            )
            raise KeyError(
                f"Missing columns {missing_columns} in {csv_path}. "
                f"Available columns: {available_columns}"
            )

        working_chunk = chunk[[language_column, rating_column]].copy()
        working_chunk[rating_column] = pd.to_numeric(working_chunk[rating_column], errors="coerce")
        working_chunk = working_chunk.dropna(subset=[language_column, rating_column])

        grouped = working_chunk.groupby(language_column)[rating_column].agg(["sum", "count"])
        for language, values in grouped.iterrows():
            sum_accumulator[str(language)] += float(values["sum"])
            count_accumulator[str(language)] += int(values["count"])

        logger.info(
            "Processed language chunk %s | languages=%s",
            chunk_index,
            list(grouped.index.astype(str)),
        )

    if processed_chunks == 0:
        logger.warning("No chunks were processed for per-language chunk analysis")
        raise ValueError(f"The file '{csv_path}' is empty.")

    if not count_accumulator:
        logger.warning("No valid language/rating pairs were found across processed chunks")
        raise ValueError(
            f"No valid values were found in columns '{language_column}' and '{rating_column}'."
        )

    language_means = {
        language: sum_accumulator[language] / count_accumulator[language]
        for language in sorted(count_accumulator)
    }
    logger.info("Per-language accumulator combination finished | means=%s", language_means)
    return language_means


def main() -> None:
    logger.info("MongoDB raw CSV export started")
    dataframe = load_from_mongodb()

    if dataframe.empty:
        print("No MongoDB records found. CSV export skipped.")
        logger.warning("CSV export skipped because no MongoDB records were available")
        return

    export_path = export_raw_csv(dataframe)
    print(f"Loaded {len(dataframe)} documents from MongoDB.")
    print(f"Exported raw CSV to: {export_path}")
    logger.info("MongoDB raw CSV export finished successfully")


if __name__ == "__main__":
    main()

    try:
        create_large_ratings_csv()
        _, before_mb, after_mb = optimize_dataframe_dtypes(DEFAULT_CHUNK_CSV_PATH)
        mean_rating = compute_global_mean_from_chunks(DEFAULT_CHUNK_CSV_PATH)
        language_means = compute_per_language_mean_from_chunks(DEFAULT_CHUNK_CSV_PATH)
        print(f"Memory usage before dtype optimization: {before_mb:.4f} MB")
        print(f"Memory usage after dtype optimization: {after_mb:.4f} MB")
        print(f"Global mean of rating column across chunks: {mean_rating:.2f}")
        print(f"Per-language mean ratings across chunks: {language_means}")
    except (FileNotFoundError, KeyError, ValueError) as exc:
        print(f"Chunked CSV mean calculation could not run: {exc}")
        logger.warning("Chunked CSV mean calculation skipped | reason=%s", exc)
