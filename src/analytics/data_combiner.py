"""DataFrame combination helpers for Apple brand-monitor analytics."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def merge_on_key(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    key: str = "_id",
    how: str = "inner",
) -> pd.DataFrame:
    """Merge two dataframes on a shared key."""
    merged = pd.merge(left_df, right_df, on=key, how=how, suffixes=("_mongo", "_mysql"))
    logger.info(
        "Merged dataframes | how=%s | key=%s | left_rows=%s | right_rows=%s | merged_rows=%s",
        how,
        key,
        len(left_df),
        len(right_df),
        len(merged),
    )
    return merged


def compare_join_types(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    key: str = "_id",
) -> pd.DataFrame:
    """Compare inner, left, right, and outer joins by output size."""
    results = []
    for how in ["inner", "left", "right", "outer"]:
        merged = merge_on_key(left_df, right_df, key=key, how=how)
        results.append({"join_type": how, "row_count": len(merged)})
    comparison = pd.DataFrame(results)
    logger.info("Join comparison created | rows=%s", len(comparison))
    return comparison


def save_join_comparison_chart(comparison_df: pd.DataFrame, output_path: Path) -> Path:
    """Save a simple join-size comparison bar chart."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(7, 4))
    plt.bar(comparison_df["join_type"], comparison_df["row_count"], color="#5b8c85")
    plt.title("Join Type Comparison for Apple Mention Data")
    plt.xlabel("Join Type")
    plt.ylabel("Row Count")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    logger.info("Saved join comparison chart | path=%s", output_path)
    return output_path


def concatenate_frames(dataframes: list[pd.DataFrame], ignore_index: bool = True) -> pd.DataFrame:
    """Concatenate dataframes that share the same structure."""
    combined = pd.concat(dataframes, ignore_index=ignore_index)
    logger.info("Concatenated dataframes | input_frames=%s | rows=%s", len(dataframes), len(combined))
    return combined
