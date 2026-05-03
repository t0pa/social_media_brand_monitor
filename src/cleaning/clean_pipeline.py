"""Reusable cleaning pipeline for the Apple brand-monitor dataset."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analytics.explorer import filter_apple_mentions
from src.cleaning.deduplicator import (
    count_duplicates,
    drop_duplicate_ids,
    drop_duplicate_title_date_pairs,
    remove_exact_duplicates,
)
from src.cleaning.missing_handler import handle_missing_values, report_missing
from src.cleaning.string_cleaner import clean_brand_strings
from src.cleaning.type_converter import convert_brand_types
from src.cleaning.validator import validate_brand_dataset
from src.utils.logger import get_logger


logger = get_logger(__name__)

OUTPUT_DIR = Path("data/processed/cleaned")
DEFAULT_OUTPUT_PATH = OUTPUT_DIR / "cleaned_data.csv"
ALTERNATE_OUTPUT_PATH = OUTPUT_DIR / "clean.csv"
MISSING_REPORT_PATH = OUTPUT_DIR / "missing_report.csv"


def run_cleaning_pipeline(
    raw_df: pd.DataFrame,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> pd.DataFrame:
    """Apply all cleaning steps in sequence and save the cleaned Apple/iPhone dataset."""
    logger.info("Cleaning pipeline started | rows=%s | columns=%s", len(raw_df), len(raw_df.columns))
    cleaned = raw_df.copy()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cleaned = filter_apple_mentions(cleaned, keyword="apple")
    logger.info(
        "Restricted cleaning dataset to Apple/iPhone project scope | rows=%s | columns=%s",
        len(cleaned),
        len(cleaned.columns),
    )

    missing_report = report_missing(cleaned)
    missing_report.to_csv(MISSING_REPORT_PATH, index=False, encoding="utf-8")
    logger.info("Missing-value report saved | path=%s", MISSING_REPORT_PATH)

    cleaned = handle_missing_values(
        cleaned,
        critical_columns=["_id", "title"],
        text_columns=["author", "description", "content", "title", "source", "document_type", "type"],
        zero_as_missing_columns=["rating", "price", "content_length"],
        numeric_columns=["rating", "price", "content_length", "page", "page_number", "query_params.year"],
        high_missing_threshold=0.85,
        protected_columns=["_id", "title", "source", "document_type", "type", "publishedAt", "date", "url", "language", "rating", "content_hash", "record_date"],
        text_placeholder="Unknown",
    )

    if "price" in cleaned.columns and pd.to_numeric(cleaned["price"], errors="coerce").notna().sum() == 0:
        cleaned = cleaned.drop(columns=["price"])
        logger.info(
            "Dropped price column because the Apple/iPhone dataset contained no usable numeric price values."
        )

    cleaned = clean_brand_strings(cleaned)
    cleaned = remove_exact_duplicates(cleaned)

    duplicate_id_count = count_duplicates(cleaned, "_id")
    logger.info("Duplicate _id count before ID deduplication: %s", duplicate_id_count)
    cleaned = drop_duplicate_ids(cleaned)
    cleaned = drop_duplicate_title_date_pairs(cleaned)
    cleaned = convert_brand_types(cleaned)
    cleaned = validate_brand_dataset(cleaned)

    cleaned.to_csv(output_path, index=False, encoding="utf-8")
    cleaned.to_csv(ALTERNATE_OUTPUT_PATH, index=False, encoding="utf-8")
    logger.info(
        "Cleaning pipeline finished | output=%s | alternate_output=%s | rows=%s | columns=%s",
        output_path,
        ALTERNATE_OUTPUT_PATH,
        len(cleaned),
        len(cleaned.columns),
    )
    return cleaned
