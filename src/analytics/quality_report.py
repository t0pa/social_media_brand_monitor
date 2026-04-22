import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.analytics.explorer import (
    clean_brand_dataset,
    filter_apple_mentions,
    load_brand_dataset_from_mongodb,
)
from src.utils.logger import get_logger


logger = get_logger(__name__)

OUTPUT_DIR = Path("data/processed/apple_brand_eda")
QUALITY_REPORT_PATH = OUTPUT_DIR / "apple_quality_report.csv"
MISSING_HEATMAP_PATH = OUTPUT_DIR / "charts" / "apple_missing_values_heatmap.png"
INDEXING_REPORT_PATH = OUTPUT_DIR / "apple_selection_examples.txt"


def prepare_quality_dataset() -> pd.DataFrame:
    """Load and enrich the Apple dataset for quality checks."""
    dataframe = load_brand_dataset_from_mongodb()
    apple_mentions = filter_apple_mentions(dataframe)
    cleaned = clean_brand_dataset(apple_mentions)

    enriched = cleaned.copy()
    enriched["overview"] = (
        enriched.get("description", pd.Series(index=enriched.index, dtype="string"))
        .fillna(enriched.get("content", pd.Series(index=enriched.index, dtype="string")))
        .astype("string")
        .str.strip()
    )
    enriched["genres"] = (
        enriched.get("document_type", pd.Series(index=enriched.index, dtype="string"))
        .fillna("unknown")
        .astype("string")
        .str.strip()
    )
    logger.info("Quality dataset prepared | rows=%s | columns=%s", len(enriched), list(enriched.columns))
    return enriched


def build_quality_report(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Create a full quality report for the Apple dataset."""
    report = pd.DataFrame(
        {
            "column": dataframe.columns,
            "dtype": dataframe.dtypes.astype(str).values,
            "non_null_count": dataframe.notna().sum().values,
            "missing_count": dataframe.isna().sum().values,
            "missing_pct": (dataframe.isna().mean() * 100).round(2).values,
            "unique_count": dataframe.nunique(dropna=True).values,
        }
    )
    logger.info("Quality report built | rows=%s", len(report))
    return report


def save_quality_report_csv(report: pd.DataFrame, output_path: Path = QUALITY_REPORT_PATH) -> Path:
    """Save the full quality report as CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Quality report CSV saved | path=%s", output_path)
    return output_path


def save_missing_value_heatmap(dataframe: pd.DataFrame, output_path: Path = MISSING_HEATMAP_PATH) -> Path:
    """Save a missing-value heatmap image."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(12, 4))
    plt.imshow(dataframe.isna().astype(int), aspect="auto", cmap="Blues")
    plt.title("Apple Dataset Missing Value Heatmap")
    plt.xlabel("Columns")
    plt.ylabel("Rows")
    plt.xticks(range(len(dataframe.columns)), dataframe.columns, rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    logger.info("Missing value heatmap saved | path=%s", output_path)
    return output_path


def build_indexing_examples(dataframe: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Demonstrate loc, iloc, boolean filtering, isin, and between."""
    mention_date_series = pd.to_datetime(dataframe["mention_date"], errors="coerce")
    content_length_series = pd.to_numeric(dataframe["content_length"], errors="coerce")

    examples = {
        "loc_example": dataframe.loc[:, ["title", "author", "source"]].head(3),
        "iloc_example": dataframe.iloc[:3, :4],
        "boolean_example": dataframe[content_length_series > 0][["title", "content_length"]].head(5),
        "isin_example": dataframe[dataframe["document_type"].isin(["json", "csv"])][
            ["title", "document_type"]
        ].head(5),
        "between_example": dataframe[mention_date_series.dt.day.between(2, 4, inclusive="both")][
            ["title", "mention_date"]
        ].head(5),
    }
    logger.info("Indexing examples built | examples=%s", list(examples))
    return examples


def save_indexing_examples(examples: dict[str, pd.DataFrame], output_path: Path = INDEXING_REPORT_PATH) -> Path:
    """Save indexing and filtering examples to a text file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sections: list[str] = []
    for name, example in examples.items():
        sections.extend([name, "=" * len(name), example.to_string(index=False), ""])
    output_path.write_text("\n".join(sections), encoding="utf-8")
    logger.info("Indexing examples report saved | path=%s", output_path)
    return output_path


def main() -> None:
    logger.info("Apple quality report pipeline started")
    dataframe = prepare_quality_dataset()
    report = build_quality_report(dataframe)
    quality_report_path = save_quality_report_csv(report)
    heatmap_path = save_missing_value_heatmap(dataframe)
    indexing_examples = build_indexing_examples(dataframe)
    indexing_report_path = save_indexing_examples(indexing_examples)

    print("Missing value summary:")
    print(report[["column", "missing_count", "missing_pct"]])
    print(f"\nSaved quality report CSV to: {quality_report_path}")
    print(f"Saved missing value heatmap to: {heatmap_path}")
    print(f"Saved loc/iloc/filtering examples to: {indexing_report_path}")
    logger.info(
        "Apple quality report pipeline finished | report=%s | heatmap=%s | selections=%s",
        quality_report_path,
        heatmap_path,
        indexing_report_path,
    )


if __name__ == "__main__":
    main()
