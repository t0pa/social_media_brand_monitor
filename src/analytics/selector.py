import os
import sys
from pathlib import Path

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.utils.logger import get_logger


logger = get_logger(__name__)

DEFAULT_OUTPUT_PATH = Path("data/processed/apple_brand_eda/apple_selection_examples.txt")


def select_columns(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Return a label-based column slice using loc."""
    available_columns = [column for column in columns if column in dataframe.columns]
    result = dataframe.loc[:, available_columns].copy()
    logger.info("Selector loc column slice built | columns=%s | rows=%s", available_columns, len(result))
    return result


def sample_rows_iloc(dataframe: pd.DataFrame, row_count: int = 3, column_count: int = 4) -> pd.DataFrame:
    """Return a positional sample using iloc."""
    result = dataframe.iloc[:row_count, :column_count].copy()
    logger.info(
        "Selector iloc sample built | row_count=%s | column_count=%s | result_rows=%s",
        row_count,
        column_count,
        len(result),
    )
    return result


def boolean_filter(dataframe: pd.DataFrame, column: str, minimum_value: float) -> pd.DataFrame:
    """Filter rows where a numeric column is greater than a threshold."""
    if column not in dataframe.columns:
        logger.warning("Boolean filter skipped because column was missing | column=%s", column)
        return dataframe.head(0).copy()

    numeric_series = pd.to_numeric(dataframe[column], errors="coerce")
    result = dataframe.loc[numeric_series > minimum_value].copy()
    logger.info(
        "Selector boolean filter built | column=%s | minimum_value=%s | matched_rows=%s",
        column,
        minimum_value,
        len(result),
    )
    return result


def filter_with_isin(
    dataframe: pd.DataFrame,
    column: str,
    values: list[str],
    exclude: bool = False,
) -> pd.DataFrame:
    """Filter rows using isin or exclusion."""
    if column not in dataframe.columns:
        logger.warning("isin filter skipped because column was missing | column=%s", column)
        return dataframe.head(0).copy()

    mask = dataframe[column].isin(values)
    if exclude:
        mask = ~mask
    result = dataframe.loc[mask].copy()
    logger.info(
        "Selector isin filter built | column=%s | values=%s | exclude=%s | matched_rows=%s",
        column,
        values,
        exclude,
        len(result),
    )
    return result


def filter_with_between(
    dataframe: pd.DataFrame,
    column: str,
    lower: float,
    upper: float,
) -> pd.DataFrame:
    """Filter rows where a numeric or datetime-derived column falls in a range."""
    if column not in dataframe.columns:
        logger.warning("between filter skipped because column was missing | column=%s", column)
        return dataframe.head(0).copy()

    numeric_series = pd.to_numeric(dataframe[column], errors="coerce")
    result = dataframe.loc[numeric_series.between(lower, upper, inclusive="both")].copy()
    logger.info(
        "Selector between filter built | column=%s | lower=%s | upper=%s | matched_rows=%s",
        column,
        lower,
        upper,
        len(result),
    )
    return result


def build_selector_examples(dataframe: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build the selection examples requested in the assignment."""
    if "mention_date" in dataframe.columns:
        mention_day = pd.to_datetime(dataframe["mention_date"], errors="coerce").dt.day
    else:
        mention_day = pd.Series(index=dataframe.index, dtype="float64")

    if {"title", "content_length"}.issubset(dataframe.columns):
        boolean_example = boolean_filter(dataframe, "content_length", 0)[["title", "content_length"]].head(5)
    else:
        boolean_example = dataframe.head(0).copy()

    if {"title", "document_type"}.issubset(dataframe.columns):
        isin_example = filter_with_isin(dataframe, "document_type", ["json", "csv"])[
            ["title", "document_type"]
        ].head(5)
    else:
        isin_example = dataframe.head(0).copy()

    if {"title", "mention_date"}.issubset(dataframe.columns):
        between_example = dataframe.loc[mention_day.between(2, 4, inclusive="both"), ["title", "mention_date"]]
        between_example = between_example.head(5).copy()
    else:
        between_example = dataframe.head(0).copy()

    examples = {
        "loc_example": select_columns(dataframe, ["title", "author", "source"]).head(3),
        "iloc_example": sample_rows_iloc(dataframe, row_count=3, column_count=4),
        "boolean_example": boolean_example,
        "isin_example": isin_example,
        "between_example": between_example,
    }
    logger.info("Selector examples built | examples=%s", list(examples))
    return examples


def selector_examples_to_text(examples: dict[str, pd.DataFrame]) -> str:
    """Render selector examples as plain text for the report file."""
    sections: list[str] = []
    for name, example in examples.items():
        sections.extend([name, "=" * len(name), example.to_string(index=False), ""])
    return "\n".join(sections)


def save_selector_examples(
    examples: dict[str, pd.DataFrame],
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> Path:
    """Save selector examples to a text report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(selector_examples_to_text(examples), encoding="utf-8")
    logger.info("Selector examples report saved | path=%s", output_path)
    return output_path
