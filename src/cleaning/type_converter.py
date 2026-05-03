"""Type-conversion helpers for the Apple brand-monitor dataset."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def convert_datetime_columns(
    df: pd.DataFrame,
    date_columns: Iterable[str] = ("publishedAt", "date", "mention_date", "record_date", "extraction_timestamp"),
) -> pd.DataFrame:
    """Convert date-like string columns to pandas datetime."""
    cleaned = df.copy()
    converted: list[str] = []

    for column in date_columns:
        if column in cleaned.columns:
            cleaned[column] = pd.to_datetime(cleaned[column], errors="coerce", utc=True)
            converted.append(column)

    logger.info("Converted datetime columns: %s", converted)
    return cleaned


def convert_numeric_columns(
    df: pd.DataFrame,
    int_columns: Iterable[str] = ("page", "page_number", "awards", "best_picture", "nominations", "year", "query_params.year", "mention_year"),
    float_columns: Iterable[str] = ("price", "rating", "content_length"),
) -> pd.DataFrame:
    """Convert numeric fields using coercion for invalid values."""
    cleaned = df.copy()

    for column in int_columns:
        if column in cleaned.columns:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce").astype("Int64")

    for column in float_columns:
        if column in cleaned.columns:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce").astype("float32")

    logger.info(
        "Converted numeric columns | ints=%s | floats=%s",
        [column for column in int_columns if column in cleaned.columns],
        [column for column in float_columns if column in cleaned.columns],
    )
    return cleaned


def convert_to_categories(
    df: pd.DataFrame,
    category_columns: Iterable[str] = ("source", "document_type", "type", "language"),
    max_unique_ratio: float = 0.5,
) -> pd.DataFrame:
    """Convert low-cardinality text columns to category dtype."""
    cleaned = df.copy()
    converted: list[str] = []

    for column in category_columns:
        if column not in cleaned.columns:
            continue

        unique_ratio = cleaned[column].nunique(dropna=False) / max(len(cleaned), 1)
        if unique_ratio <= max_unique_ratio:
            cleaned[column] = cleaned[column].astype("category")
            converted.append(column)

    logger.info("Converted category columns: %s", converted)
    return cleaned


def memory_report(before_df: pd.DataFrame, after_df: pd.DataFrame) -> pd.DataFrame:
    """Compare memory usage before and after dtype conversion."""
    before_bytes = before_df.memory_usage(deep=True).sum()
    after_bytes = after_df.memory_usage(deep=True).sum()
    report = pd.DataFrame(
        [
            {
                "stage": "before",
                "memory_bytes": before_bytes,
                "memory_mb": before_bytes / (1024 * 1024),
            },
            {
                "stage": "after",
                "memory_bytes": after_bytes,
                "memory_mb": after_bytes / (1024 * 1024),
            },
            {
                "stage": "saved",
                "memory_bytes": before_bytes - after_bytes,
                "memory_mb": (before_bytes - after_bytes) / (1024 * 1024),
            },
        ]
    )
    logger.info(
        "Memory report created | before_bytes=%s | after_bytes=%s | saved_bytes=%s",
        before_bytes,
        after_bytes,
        before_bytes - after_bytes,
    )
    return report


def convert_brand_types(df: pd.DataFrame) -> pd.DataFrame:
    """Run the standard type-conversion sequence for brand-monitor data."""
    cleaned = convert_datetime_columns(df)
    cleaned = convert_numeric_columns(cleaned)
    cleaned = convert_to_categories(cleaned)
    logger.info("Completed type conversion. Shape: %s", cleaned.shape)
    return cleaned
