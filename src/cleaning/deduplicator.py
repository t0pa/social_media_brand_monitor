"""Duplicate-detection helpers for the Apple brand-monitor dataset."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def count_duplicates(df: pd.DataFrame, column: str) -> int:
    """Count duplicated non-null values in a chosen column."""
    if column not in df.columns:
        logger.info("Duplicate count skipped because column %s is missing.", column)
        return 0

    duplicate_count = int(df[column].dropna().duplicated().sum())
    logger.info("Counted %s duplicate values in column %s", duplicate_count, column)
    return duplicate_count


def remove_exact_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove fully identical rows across all columns."""
    before = len(df)
    cleaned = df.drop_duplicates().copy()
    logger.info("Removed %s exact duplicate rows.", before - len(cleaned))
    return cleaned


def drop_duplicate_ids(
    df: pd.DataFrame,
    id_columns: Iterable[str] = ("_id", "url", "content_hash"),
) -> pd.DataFrame:
    """Drop repeated identifier-like values while keeping the first occurrence."""
    cleaned = df.copy()

    for column in id_columns:
        if column not in cleaned.columns:
            continue
        before = len(cleaned)
        non_null_mask = cleaned[column].notna()
        deduped_non_null = cleaned.loc[non_null_mask].drop_duplicates(subset=[column], keep="first")
        cleaned = pd.concat([deduped_non_null, cleaned.loc[~non_null_mask]], axis=0).sort_index()
        logger.info(
            "Removed %s duplicate rows using identifier column %s",
            before - len(cleaned),
            column,
        )

    return cleaned.copy()


def drop_duplicate_title_date_pairs(
    df: pd.DataFrame,
    title_column: str = "title",
    date_column: str = "mention_date",
) -> pd.DataFrame:
    """Drop repeated title/date combinations after string normalization."""
    if title_column not in df.columns or date_column not in df.columns:
        logger.info(
            "Title/date deduplication skipped because %s or %s is missing.",
            title_column,
            date_column,
        )
        return df.copy()

    before = len(df)
    cleaned = df.drop_duplicates(subset=[title_column, date_column], keep="first").copy()
    logger.info(
        "Removed %s duplicate title/date rows using columns %s and %s",
        before - len(cleaned),
        title_column,
        date_column,
    )
    return cleaned
