"""Time-series helpers for Apple brand-monitor analytics."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def parse_mention_dates(df: pd.DataFrame, date_column: str = "mention_date") -> pd.DataFrame:
    """Parse mention dates and derive common date parts."""
    cleaned = df.copy()
    cleaned[date_column] = pd.to_datetime(cleaned[date_column], errors="coerce", utc=True)
    cleaned["mention_year"] = cleaned[date_column].dt.year.astype("Int64")
    cleaned["mention_month"] = cleaned[date_column].dt.month.astype("Int64")
    cleaned["mention_weekday"] = cleaned[date_column].dt.day_name()
    cleaned["mention_quarter"] = cleaned[date_column].dt.quarter.astype("Int64")
    logger.info("Parsed mention dates | valid_dates=%s | missing_dates=%s", cleaned[date_column].notna().sum(), cleaned[date_column].isna().sum())
    return cleaned


def build_monthly_mentions(df: pd.DataFrame, date_column: str = "mention_date") -> pd.DataFrame:
    """Build a monthly mention-count time series."""
    working = df.dropna(subset=[date_column]).copy()
    monthly = (
        working.set_index(date_column)
        .resample("ME")
        .agg(
            mention_count=("_id", "count"),
            avg_rating=("rating", "mean"),
            avg_overview_chars=("overview_length", "mean"),
        )
        .reset_index()
    )
    logger.info("Monthly mention series created | rows=%s", len(monthly))
    return monthly


def resample_mentions(df: pd.DataFrame, date_column: str = "mention_date", frequency: str = "YE") -> pd.DataFrame:
    """Resample Apple mention counts to a chosen frequency."""
    working = df.dropna(subset=[date_column]).copy()
    resampled = (
        working.set_index(date_column)
        .resample(frequency)
        .agg(
            mention_count=("_id", "count"),
            avg_rating=("rating", "mean"),
            avg_overview_chars=("overview_length", "mean"),
        )
        .reset_index()
    )
    logger.info("Resampled mention series created | frequency=%s | rows=%s", frequency, len(resampled))
    return resampled


def add_rolling_averages(monthly_df: pd.DataFrame, value_column: str = "mention_count") -> pd.DataFrame:
    """Calculate 3, 6, and 12 period rolling averages."""
    cleaned = monthly_df.copy()
    for window in [3, 6, 12]:
        cleaned[f"{value_column}_rolling_{window}"] = cleaned[value_column].rolling(window=window, min_periods=1).mean()
    logger.info("Added rolling averages to monthly series | value_column=%s", value_column)
    return cleaned


def save_rolling_mentions_chart(monthly_df: pd.DataFrame, output_path: Path) -> Path:
    """Save a line chart of monthly Apple mentions with rolling averages."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(10, 5))
    plt.plot(monthly_df["mention_date"], monthly_df["mention_count"], label="Monthly mentions", color="#6c5b7b")
    plt.plot(monthly_df["mention_date"], monthly_df["mention_count_rolling_3"], label="3-month avg", color="#c06c50")
    plt.plot(monthly_df["mention_date"], monthly_df["mention_count_rolling_6"], label="6-month avg", color="#355c7d")
    plt.plot(monthly_df["mention_date"], monthly_df["mention_count_rolling_12"], label="12-month avg", color="#2a9d8f")
    plt.title("Monthly Apple Mention Trend with Rolling Averages")
    plt.xlabel("Month")
    plt.ylabel("Mentions")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    logger.info("Saved rolling mentions chart | path=%s", output_path)
    return output_path
