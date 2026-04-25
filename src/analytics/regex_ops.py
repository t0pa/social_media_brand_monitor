import os
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.analytics.quality_report import prepare_quality_dataset
from src.utils.logger import get_logger


logger = get_logger(__name__)

OUTPUT_DIR = Path("data/processed/apple_brand_eda")
REGEX_RESULTS_PATH = OUTPUT_DIR / "apple_regex_results.csv"

YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
NUMBER_PATTERN = re.compile(r"(\d+%|\$\d+|\d+)")
PRODUCT_PATTERN = re.compile(r"\b(apple|iphone|ipad|macbook|airpods|watch)\b", re.IGNORECASE)
CRIME_PATTERN = re.compile(r"\b(lawsuit|fine|fraud|probe|investigation)\b", re.IGNORECASE)
TITLE_PREFIX_PATTERN = re.compile(r"^(apple|iphone|ipad|macbook|airpods)\b", re.IGNORECASE)
LOCAL_ID_PATTERN = re.compile(r"^[0-9a-f]{24}$", re.IGNORECASE)


def run_regex_operations(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Perform multiple regex operations on title, overview, and genres columns."""
    working = dataframe.copy()

    title_series = working["title"].fillna("").astype(str)
    overview_series = working["overview"].fillna("").astype(str)
    genres_series = working["genres"].fillna("").astype(str)
    id_source = working.get("_id", pd.Series(index=working.index, dtype="string")).fillna("").astype(str)

    working["title_prefix_match"] = title_series.str.extract(TITLE_PREFIX_PATTERN, expand=False)
    working["title_extracted_product"] = title_series.str.extract(PRODUCT_PATTERN, expand=False)
    working["title_extracted_year"] = title_series.str.extract(YEAR_PATTERN, expand=False)
    working["overview_extracted_number"] = overview_series.str.extract(NUMBER_PATTERN, expand=False)
    working["overview_crime_term_count"] = overview_series.str.count(CRIME_PATTERN)
    working["overview_is_unusually_short"] = overview_series.str.len().lt(40)
    working["genres_standardized"] = genres_series.str.replace(r"[^A-Za-z0-9]+", "_", regex=True).str.lower()
    working["genres_has_structured_doc"] = genres_series.str.contains(
        r"\b(?:json|csv|xml|pdf|docx|xlsx)\b",
        case=False,
        regex=True,
    )
    working["valid_local_id_format"] = id_source.str.match(LOCAL_ID_PATTERN)

    logger.info(
        "Regex operations completed | rows=%s | columns_added=%s",
        len(working),
        [
            "title_prefix_match",
            "title_extracted_product",
            "title_extracted_year",
            "overview_extracted_number",
            "overview_crime_term_count",
            "overview_is_unusually_short",
            "genres_standardized",
            "genres_has_structured_doc",
            "valid_local_id_format",
        ],
    )
    return working


def save_regex_results(dataframe: pd.DataFrame, output_path: Path = REGEX_RESULTS_PATH) -> Path:
    """Save regex operation results as CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Regex results CSV saved | path=%s", output_path)
    return output_path


def run_regex_pipeline() -> dict[str, object]:
    """Run the regex analysis workflow."""
    logger.info("Apple regex pipeline started")
    dataframe = prepare_quality_dataset()
    regex_results = run_regex_operations(dataframe)
    output_path = save_regex_results(
        regex_results[
            [
                "title",
                "overview",
                "genres",
                "title_prefix_match",
                "title_extracted_product",
                "title_extracted_year",
                "overview_extracted_number",
                "overview_crime_term_count",
                "overview_is_unusually_short",
                "genres_standardized",
                "genres_has_structured_doc",
                "valid_local_id_format",
            ]
        ]
    )
    logger.info("Apple regex pipeline finished | output=%s", output_path)
    return {"dataframe": regex_results, "output_path": output_path}


def main() -> None:
    results = run_regex_pipeline()

    print("Regex operation preview:")
    print(
        results["dataframe"][
            [
                "title",
                "title_prefix_match",
                "title_extracted_product",
                "overview_extracted_number",
                "overview_crime_term_count",
                "genres_standardized",
                "valid_local_id_format",
            ]
        ].head()
    )
    print(f"\nSaved regex results to: {results['output_path']}")


if __name__ == "__main__":
    main()
