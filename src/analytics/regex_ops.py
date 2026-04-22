
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.analytics.quality_report import prepare_quality_dataset
from src.utils.logger import get_logger


logger = get_logger(__name__)

OUTPUT_DIR = Path("data/processed/apple_brand_eda")
REGEX_RESULTS_PATH = OUTPUT_DIR / "apple_regex_results.csv"


def run_regex_operations(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Perform 4+ regex operations on title, overview, and genres columns."""
    working = dataframe.copy()

    working["title_has_device_keyword"] = working["title"].fillna("").str.contains(
        r"\b(apple|iphone|ipad|macbook|airpods)\b", case=False, regex=True
    )
    working["title_extracted_product"] = working["title"].fillna("").str.extract(
        r"\b(Apple|iPhone|iPad|MacBook|AirPods)\b", expand=False
    )
    working["overview_extracted_discount"] = working["overview"].fillna("").str.extract(
        r"(\d+%|\$\d+)", expand=False
    )
    working["overview_has_lawsuit_term"] = working["overview"].fillna("").str.contains(
        r"\blawsuit\b", case=False, regex=True
    )
    working["genres_standardized"] = (
        working["genres"].fillna("").str.replace(r"[^A-Za-z0-9]+", "_", regex=True).str.lower()
    )
    working["genres_has_structured_doc"] = working["genres"].fillna("").str.contains(
        r"\b(json|csv|xml|pdf)\b", case=False, regex=True
    )

    logger.info(
        "Regex operations completed | rows=%s | columns_added=%s",
        len(working),
        [
            "title_has_device_keyword",
            "title_extracted_product",
            "overview_extracted_discount",
            "overview_has_lawsuit_term",
            "genres_standardized",
            "genres_has_structured_doc",
        ],
    )
    return working


def save_regex_results(dataframe: pd.DataFrame, output_path: Path = REGEX_RESULTS_PATH) -> Path:
    """Save regex operation results as CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Regex results CSV saved | path=%s", output_path)
    return output_path


def main() -> None:
    logger.info("Apple regex pipeline started")
    dataframe = prepare_quality_dataset()
    regex_results = run_regex_operations(dataframe)
    output_path = save_regex_results(
        regex_results[
            [
                "title",
                "overview",
                "genres",
                "title_has_device_keyword",
                "title_extracted_product",
                "overview_extracted_discount",
                "overview_has_lawsuit_term",
                "genres_standardized",
                "genres_has_structured_doc",
            ]
        ]
    )

    print("Regex operation preview:")
    print(
        regex_results[
            [
                "title",
                "title_has_device_keyword",
                "title_extracted_product",
                "overview_extracted_discount",
                "overview_has_lawsuit_term",
                "genres_standardized",
            ]
        ].head()
    )
    print(f"\nSaved regex results to: {output_path}")
    logger.info("Apple regex pipeline finished | output=%s", output_path)


if __name__ == "__main__":
    main()
