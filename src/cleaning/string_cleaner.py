"""String cleaning helpers for the Apple brand-monitor dataset."""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)

MULTISPACE_PATTERN = re.compile(r"\s+")
MOJIBAKE_FIXES = {
    "â€™": "'",
    "â€˜": "'",
    "â€œ": '"',
    "â€": '"',
    "â€“": "-",
    "â€”": "-",
    "Â": "",
}
LANGUAGE_NAME_MAP = {
    "english": "en",
    "german": "de",
    "french": "fr",
    "spanish": "es",
}


def _existing_columns(df: pd.DataFrame, columns: Iterable[str] | None) -> list[str]:
    if columns is None:
        return []
    return [column for column in columns if column in df.columns]


def _normalize_text_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype("string")
    cleaned = cleaned.str.replace(MULTISPACE_PATTERN, " ", regex=True).str.strip()

    for broken_text, replacement in MOJIBAKE_FIXES.items():
        cleaned = cleaned.str.replace(broken_text, replacement, regex=False)

    cleaned = cleaned.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "null": pd.NA})
    return cleaned


def clean_text_columns(
    df: pd.DataFrame,
    text_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Trim whitespace and normalize visible mojibake in text fields."""
    cleaned = df.copy()
    columns = _existing_columns(cleaned, text_columns)

    if text_columns is None:
        columns = cleaned.select_dtypes(include=["object", "string"]).columns.tolist()

    for column in columns:
        cleaned[column] = _normalize_text_series(cleaned[column])

    logger.info("Normalized text formatting in columns: %s", columns)
    return cleaned


def clean_title_column(df: pd.DataFrame, column: str = "title") -> pd.DataFrame:
    """Normalize title spacing while preserving editorial casing."""
    cleaned = df.copy()
    if column not in cleaned.columns:
        logger.info("Title column %s not found; skipping title cleaning.", column)
        return cleaned

    cleaned[column] = _normalize_text_series(cleaned[column])
    logger.info("Cleaned title column: %s", column)
    return cleaned


def normalize_language_codes(df: pd.DataFrame, column: str = "language") -> pd.DataFrame:
    """Normalize language codes to lowercase short codes like en, de, or en-us."""
    cleaned = df.copy()
    if column not in cleaned.columns:
        logger.info("Language column %s not found; skipping normalization.", column)
        return cleaned

    series = _normalize_text_series(cleaned[column]).str.lower()
    series = series.str.replace("_", "-", regex=False)
    series = series.replace(LANGUAGE_NAME_MAP)
    cleaned[column] = series
    logger.info("Normalized language codes in column: %s", column)
    return cleaned


def clean_overview_text(
    df: pd.DataFrame,
    source_columns: Iterable[str] = ("description", "content"),
    output_column: str = "overview",
) -> pd.DataFrame:
    """Create a cleaned overview column from description/content fallback text."""
    cleaned = df.copy()
    available = _existing_columns(cleaned, source_columns)
    if not available:
        logger.info("No overview source columns found; skipping overview creation.")
        return cleaned

    overview = pd.Series(pd.NA, index=cleaned.index, dtype="string")
    for column in available:
        normalized = _normalize_text_series(cleaned[column])
        overview = overview.fillna(normalized)

    cleaned[output_column] = overview
    logger.info("Created cleaned overview column %s from %s", output_column, available)
    return cleaned


def sanitize_url_column(df: pd.DataFrame, column: str = "url") -> pd.DataFrame:
    """Preserve valid HTTP(S) URLs and coerce malformed links to missing values."""
    cleaned = df.copy()
    if column not in cleaned.columns:
        logger.info("URL column %s not found; skipping URL sanitization.", column)
        return cleaned

    series = _normalize_text_series(cleaned[column])
    cleaned[column] = series.where(series.str.match(r"^https?://", na=False), pd.NA)
    logger.info("Sanitized URL values in column: %s", column)
    return cleaned


def create_mention_date(
    df: pd.DataFrame,
    source_columns: Iterable[str] = ("publishedAt", "date", "record_date"),
    output_column: str = "mention_date",
) -> pd.DataFrame:
    """Create a unified mention_date string column from available date sources."""
    cleaned = df.copy()
    available = _existing_columns(cleaned, source_columns)
    if output_column in cleaned.columns and cleaned[output_column].notna().any():
        logger.info("Mention date column %s already present; preserving existing values.", output_column)
        return cleaned

    if not available:
        logger.info("No source columns found for mention_date creation.")
        return cleaned

    mention_date = pd.Series(pd.NA, index=cleaned.index, dtype="string")
    for column in available:
        parsed = pd.to_datetime(cleaned[column], errors="coerce")
        mention_date = mention_date.fillna(parsed.dt.strftime("%Y-%m-%d"))

    cleaned[output_column] = mention_date
    logger.info("Created %s from source columns %s", output_column, available)
    return cleaned


def extract_mention_year(
    df: pd.DataFrame,
    date_column: str = "mention_date",
    output_column: str = "mention_year",
) -> pd.DataFrame:
    """Extract a 4-digit year from the cleaned mention date field."""
    cleaned = df.copy()
    if date_column not in cleaned.columns:
        logger.info("Date column %s not found; skipping year extraction.", date_column)
        return cleaned

    cleaned[output_column] = (
        cleaned[date_column]
        .astype("string")
        .str.extract(r"((?:19|20)\d{2})", expand=False)
    )
    logger.info("Extracted year column %s from %s", output_column, date_column)
    return cleaned


def clean_brand_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Run the standard string-cleaning sequence for brand-monitor data."""
    cleaned = clean_text_columns(df)
    cleaned = clean_title_column(cleaned, column="title")
    cleaned = normalize_language_codes(cleaned, column="language")
    cleaned = clean_overview_text(cleaned)
    cleaned = sanitize_url_column(cleaned, column="url")
    cleaned = create_mention_date(cleaned)
    cleaned = extract_mention_year(cleaned)
    logger.info("Completed string cleaning. Shape: %s", cleaned.shape)
    return cleaned
