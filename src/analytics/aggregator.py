"""Grouped aggregation helpers for Apple brand-monitor analytics."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def add_length_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived title and overview length metrics used across Lab 10."""
    cleaned = df.copy()
    cleaned["title_length"] = cleaned.get("title", pd.Series(index=cleaned.index, dtype="string")).astype("string").str.len()
    cleaned["overview_length"] = cleaned.get("overview", pd.Series(index=cleaned.index, dtype="string")).astype("string").str.len()
    return cleaned


def source_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Create a source-level summary with multiple named aggregations."""
    summary = (
        df.groupby("source")
        .agg(
            mention_count=("_id", "count"),
            avg_rating=("rating", "mean"),
            median_rating=("rating", "median"),
            total_title_chars=("title_length", "sum"),
            avg_overview_chars=("overview_length", "mean"),
        )
        .sort_values(["mention_count", "avg_overview_chars"], ascending=[False, False])
        .reset_index()
    )
    logger.info("Source summary created | rows=%s", len(summary))
    return summary


def yearly_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize Apple mention trends by year."""
    trends = (
        df.groupby("mention_year")
        .agg(
            mention_count=("_id", "count"),
            unique_sources=("source", "nunique"),
            avg_rating=("rating", "mean"),
            avg_overview_chars=("overview_length", "mean"),
        )
        .reset_index()
        .sort_values("mention_year")
    )
    logger.info("Yearly trends created | rows=%s", len(trends))
    return trends


def top_n_per_group(
    df: pd.DataFrame,
    group_column: str,
    sort_column: str,
    n: int = 3,
) -> pd.DataFrame:
    """Return the top N rows within each group."""
    top_rows = (
        df.sort_values([group_column, sort_column], ascending=[True, False])
        .groupby(group_column, group_keys=False)
        .apply(lambda group: group.head(n))
        .reset_index(drop=True)
    )
    logger.info(
        "Top-N per group created | group_column=%s | sort_column=%s | n=%s | rows=%s",
        group_column,
        sort_column,
        n,
        len(top_rows),
    )
    return top_rows


def save_yearly_trends_chart(trends_df: pd.DataFrame, output_path: Path) -> Path:
    """Save a chart of Apple mention volume over time."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax1 = plt.subplots(figsize=(9, 4.5))

    ax1.plot(trends_df["mention_year"], trends_df["mention_count"], marker="o", color="#c06c50")
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Mention Count", color="#c06c50")
    ax1.tick_params(axis="y", labelcolor="#c06c50")

    ax2 = ax1.twinx()
    ax2.plot(trends_df["mention_year"], trends_df["avg_overview_chars"], marker="s", color="#355c7d")
    ax2.set_ylabel("Average Overview Length", color="#355c7d")
    ax2.tick_params(axis="y", labelcolor="#355c7d")

    plt.title("Apple Mention Trends Over Time")
    fig.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info("Saved yearly trends chart | path=%s", output_path)
    return output_path
