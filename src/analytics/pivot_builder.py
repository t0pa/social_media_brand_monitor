"""Reshaping and pivot helpers for Apple brand-monitor analytics."""

from __future__ import annotations

import re

import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)

KEYWORD_PATTERNS = [
    ("iphone", re.compile(r"\biphone\b", re.IGNORECASE)),
    ("ipad", re.compile(r"\bipad\b", re.IGNORECASE)),
    ("macbook", re.compile(r"\bmacbook\b", re.IGNORECASE)),
    ("airpods", re.compile(r"\bairpods?\b", re.IGNORECASE)),
    ("apple watch", re.compile(r"\bapple\s+watch\b|\bwatch\b", re.IGNORECASE)),
    ("tim cook", re.compile(r"\btim cook\b", re.IGNORECASE)),
    ("apple services", re.compile(r"\bapple tv\b|\bicloud\b|\bapp store\b|\bapple music\b", re.IGNORECASE)),
]


def add_primary_keyword(df: pd.DataFrame, text_column: str = "overview") -> pd.DataFrame:
    """Assign a primary Apple keyword/topic based on mention text."""
    cleaned = df.copy()
    text_series = cleaned.get(text_column, pd.Series(index=cleaned.index, dtype="string")).fillna("").astype(str)

    keywords = []
    for text in text_series:
        matched_keyword = "general_apple"
        for label, pattern in KEYWORD_PATTERNS:
            if pattern.search(text):
                matched_keyword = label
                break
        keywords.append(matched_keyword)

    cleaned["primary_keyword"] = pd.Series(keywords, index=cleaned.index, dtype="string")
    logger.info("Primary keyword column created | unique_keywords=%s", cleaned["primary_keyword"].nunique(dropna=True))
    return cleaned


def melt_metrics(
    df: pd.DataFrame,
    id_vars: list[str],
    value_vars: list[str],
    var_name: str = "metric_name",
    value_name: str = "metric_value",
) -> pd.DataFrame:
    """Convert a wide metrics table into long format."""
    melted = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name,
    )
    logger.info("Melted dataframe | rows=%s | metrics=%s", len(melted), value_vars)
    return melted


def pivot_metrics(
    long_df: pd.DataFrame,
    index: list[str],
    columns: str,
    values: str,
) -> pd.DataFrame:
    """Convert long-format metrics back to wide format."""
    pivoted = long_df.pivot(index=index, columns=columns, values=values).reset_index()
    logger.info("Pivoted dataframe back to wide format | rows=%s", len(pivoted))
    return pivoted


def build_keyword_year_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """Create a keyword-by-year pivot table using mention counts."""
    pivot = pd.pivot_table(
        df,
        index="mention_year",
        columns="primary_keyword",
        values="_id",
        aggfunc="count",
        margins=True,
    )
    logger.info("Keyword/year pivot table created | rows=%s | columns=%s", len(pivot), len(pivot.columns))
    return pivot


def build_language_year_crosstab(df: pd.DataFrame) -> pd.DataFrame:
    """Create a cross-tabulation of language by mention year."""
    crosstab = pd.crosstab(df["language"], df["mention_year"], margins=True)
    logger.info("Language/year crosstab created | rows=%s | columns=%s", len(crosstab), len(crosstab.columns))
    return crosstab
