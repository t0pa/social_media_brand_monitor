"""Utilities for reporting and handling missing values in tabular datasets."""

from __future__ import annotations

from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


DEFAULT_TEXT_PLACEHOLDER = "Unknown"


def _existing_columns(df: pd.DataFrame, columns: Iterable[str] | None) -> list[str]:
    """Return only the requested columns that are present in the dataframe."""
    if columns is None:
        return []
    return [column for column in columns if column in df.columns]


def report_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Create a column-level missing-value summary.

    The report is sorted by highest missing ratio first so that high-risk fields
    are easy to inspect before deciding whether to drop, impute, or preserve.
    """
    if df.empty:
        logger.warning("Missing-value report requested for an empty dataframe.")

    missing_count = df.isna().sum()
    total_rows = len(df)
    missing_ratio = missing_count / total_rows if total_rows else missing_count.astype(float)

    report = pd.DataFrame(
        {
            "column": df.columns,
            "dtype": [str(dtype) for dtype in df.dtypes],
            "missing_count": missing_count.values,
            "missing_ratio": missing_ratio.values,
            "non_missing_count": (total_rows - missing_count).values,
        }
    )
    report = report.sort_values(
        by=["missing_ratio", "missing_count", "column"],
        ascending=[False, False, True],
    ).reset_index(drop=True)

    logger.info("Generated missing-value report for %s columns.", len(df.columns))
    return report


def remove_missing_critical_identifiers(
    df: pd.DataFrame,
    critical_columns: Iterable[str],
) -> pd.DataFrame:
    """Drop rows where required identifier columns are missing."""
    columns = _existing_columns(df, critical_columns)
    if not columns:
        logger.info("No critical identifier columns found to validate.")
        return df.copy()

    before = len(df)
    cleaned = df.dropna(subset=columns).copy()
    logger.info(
        "Dropped %s rows with missing critical identifiers: %s",
        before - len(cleaned),
        columns,
    )
    return cleaned


def fill_text_placeholders(
    df: pd.DataFrame,
    text_columns: Iterable[str] | None = None,
    placeholder: str = DEFAULT_TEXT_PLACEHOLDER,
    placeholders: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Fill missing descriptive text fields with placeholder strings."""
    cleaned = df.copy()
    selected_columns = _existing_columns(cleaned, text_columns)

    if text_columns is None:
        selected_columns = cleaned.select_dtypes(include=["object", "string"]).columns.tolist()

    if placeholders:
        selected_columns = sorted(set(selected_columns).union(placeholders.keys()))

    for column in _existing_columns(cleaned, selected_columns):
        value = placeholders.get(column, placeholder) if placeholders else placeholder
        cleaned[column] = cleaned[column].fillna(value)

    logger.info("Filled missing text values in %s columns.", len(selected_columns))
    return cleaned


def replace_unrealistic_zeros(
    df: pd.DataFrame,
    zero_as_missing_columns: Iterable[str],
) -> pd.DataFrame:
    """Replace zero values with NaN where zero is not a realistic value."""
    cleaned = df.copy()
    columns = _existing_columns(cleaned, zero_as_missing_columns)

    for column in columns:
        numeric_series = pd.to_numeric(cleaned[column], errors="coerce")
        zero_mask = numeric_series.eq(0)
        cleaned.loc[zero_mask, column] = np.nan

    logger.info("Replaced unrealistic zeros with NaN in columns: %s", columns)
    return cleaned


def fill_numeric_medians(
    df: pd.DataFrame,
    numeric_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Fill missing numeric values with each column median."""
    cleaned = df.copy()
    selected_columns = _existing_columns(cleaned, numeric_columns)

    if numeric_columns is None:
        selected_columns = cleaned.select_dtypes(include=[np.number]).columns.tolist()

    for column in selected_columns:
        numeric_series = pd.to_numeric(cleaned[column], errors="coerce")
        median = numeric_series.median()
        if pd.isna(median):
            if numeric_series.notna().sum() == 0:
                logger.info(
                    "Skipping median fill for %s because the column has no usable numeric values.",
                    column,
                )
            else:
                logger.warning("Skipping median fill for %s because its median is NaN.", column)
            continue
        cleaned[column] = numeric_series.fillna(median)

    logger.info("Filled missing numeric values with medians in %s columns.", len(selected_columns))
    return cleaned


def drop_high_missing_columns(
    df: pd.DataFrame,
    threshold: float = 0.6,
    protected_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Drop columns whose missing-data ratio is greater than the threshold."""
    if not 0 <= threshold <= 1:
        raise ValueError("threshold must be between 0 and 1")

    protected = set(protected_columns or [])
    missing_ratio = df.isna().mean()
    columns_to_drop = [
        column
        for column, ratio in missing_ratio.items()
        if ratio > threshold and column not in protected
    ]

    logger.info(
        "Dropped %s columns above missing threshold %.2f: %s",
        len(columns_to_drop),
        threshold,
        columns_to_drop,
    )
    return df.drop(columns=columns_to_drop).copy()


def handle_missing_values(
    df: pd.DataFrame,
    critical_columns: Iterable[str] | None = None,
    text_columns: Iterable[str] | None = None,
    zero_as_missing_columns: Iterable[str] | None = None,
    numeric_columns: Iterable[str] | None = None,
    high_missing_threshold: float = 0.6,
    protected_columns: Iterable[str] | None = None,
    text_placeholder: str = DEFAULT_TEXT_PLACEHOLDER,
) -> pd.DataFrame:
    """Run the standard missing-value handling sequence for the pipeline."""
    protected = set(protected_columns or [])
    protected.update(critical_columns or [])

    cleaned = remove_missing_critical_identifiers(df, critical_columns or [])
    cleaned = replace_unrealistic_zeros(cleaned, zero_as_missing_columns or [])
    cleaned = drop_high_missing_columns(
        cleaned,
        threshold=high_missing_threshold,
        protected_columns=protected,
    )
    cleaned = fill_text_placeholders(
        cleaned,
        text_columns=text_columns,
        placeholder=text_placeholder,
    )
    cleaned = fill_numeric_medians(cleaned, numeric_columns=numeric_columns)

    logger.info("Completed missing-value handling. Shape: %s", cleaned.shape)
    return cleaned
