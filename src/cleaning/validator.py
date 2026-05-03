"""Validation helpers for cleaned Apple brand-monitor datasets."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from src.analytics.regex_ops import detect_invalid_date_formats, detect_invalid_language_codes
from src.utils.logger import get_logger


logger = get_logger(__name__)


def validate_brand_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Fail fast if the cleaned dataset still contains invalid core records."""
    assert not df.empty, "Cleaned dataframe must not be empty."

    if "_id" in df.columns:
        assert df["_id"].notna().all(), "All cleaned rows must keep a non-null _id."
        assert not df["_id"].duplicated().any(), "Duplicate _id values remain after cleaning."

    if "title" in df.columns:
        titles = df["title"].astype("string")
        assert titles.notna().all(), "Titles must not be missing after cleaning."
        assert titles.str.strip().ne("").all(), "Titles must not be blank after cleaning."

    if "url" in df.columns:
        populated_urls = df["url"].dropna().astype("string")
        assert populated_urls.str.match(r"^https?://", na=False).all(), "All non-null URLs must be valid HTTP(S) links."

    if "mention_date" in df.columns:
        assert not df["mention_date"].dropna().isna().any(), "Mention dates must use a valid datetime format."
        current_year = datetime.now(timezone.utc).year + 1
        years = df["mention_date"].dropna().dt.year
        assert years.between(1990, current_year).all(), "Mention dates fall outside the expected modern news range."

    if "rating" in df.columns:
        ratings = df["rating"].dropna()
        assert ratings.between(0, 5).all(), "Ratings must stay within the expected 0-5 range."

    if "content_length" in df.columns:
        lengths = df["content_length"].dropna()
        assert (lengths >= 0).all(), "Content length cannot be negative."

    invalid_date_count = detect_invalid_date_formats(df, column="publishedAt")
    assert invalid_date_count == 0, "Invalid publishedAt date strings remain after conversion."

    invalid_language_count = detect_invalid_language_codes(df, column="language")
    assert invalid_language_count == 0, "Invalid language codes remain after cleaning."

    logger.info("Validation completed successfully. Shape: %s", df.shape)
    return df
