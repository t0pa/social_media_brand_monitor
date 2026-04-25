import os
import re
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
from src.analytics.selector import build_selector_examples, save_selector_examples
from src.utils.logger import get_logger


logger = get_logger(__name__)

OUTPUT_DIR = Path("data/processed/apple_brand_eda")
QUALITY_REPORT_PATH = OUTPUT_DIR / "apple_quality_report.csv"
QUALITY_ISSUES_PATH = OUTPUT_DIR / "apple_quality_issues.csv"
MISSING_HEATMAP_PATH = OUTPUT_DIR / "charts" / "apple_missing_values_heatmap.png"
INDEXING_REPORT_PATH = OUTPUT_DIR / "apple_selection_examples.txt"

TITLE_PLACEHOLDER_PATTERN = re.compile(r"^(untitled|unknown|n/?a|nan)?$", re.IGNORECASE)
URL_PATTERN = re.compile(r"^https?://", re.IGNORECASE)


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


def severity_from_missing_pct(missing_pct: float) -> str:
    """Map missing-value percentage to a simple severity label."""
    if missing_pct >= 50:
        return "critical"
    if missing_pct >= 20:
        return "high"
    if missing_pct > 0:
        return "medium"
    return "low"


def build_missing_value_report(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Build a completeness report with severity scoring."""
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
    report["missing_severity"] = report["missing_pct"].apply(severity_from_missing_pct)
    logger.info("Missing value report built | rows=%s", len(report))
    return report


def detect_zero_as_missing_numeric_fields(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Flag numeric columns where zeros may indicate missing values."""
    issues: list[dict[str, object]] = []
    numeric_columns = dataframe.select_dtypes(include=["number"]).columns

    for column in numeric_columns:
        numeric_series = pd.to_numeric(dataframe[column], errors="coerce").dropna()
        if numeric_series.empty:
            continue

        zero_count = int((numeric_series == 0).sum())
        if zero_count == 0:
            continue

        zero_pct = round((zero_count / len(numeric_series)) * 100, 2)
        issues.append(
            {
                "issue_type": "zero_as_missing_candidate",
                "column": column,
                "severity": "medium" if zero_pct < 50 else "high",
                "affected_rows": zero_count,
                "metric": zero_pct,
                "details": f"{zero_pct}% of non-null values are zero",
            }
        )

    issue_frame = pd.DataFrame(issues)
    logger.info("Zero-as-missing scan completed | issues=%s", len(issue_frame))
    return issue_frame


def detect_iqr_outliers(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Detect numeric outliers using the IQR rule."""
    issues: list[dict[str, object]] = []
    numeric_columns = dataframe.select_dtypes(include=["number"]).columns

    for column in numeric_columns:
        numeric_series = pd.to_numeric(dataframe[column], errors="coerce").dropna()
        if len(numeric_series) < 4:
            continue

        q1 = numeric_series.quantile(0.25)
        q3 = numeric_series.quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr == 0:
            continue

        lower_bound = q1 - (1.5 * iqr)
        upper_bound = q3 + (1.5 * iqr)
        outlier_mask = (numeric_series < lower_bound) | (numeric_series > upper_bound)
        outlier_count = int(outlier_mask.sum())
        if outlier_count == 0:
            continue

        issues.append(
            {
                "issue_type": "iqr_outlier",
                "column": column,
                "severity": "medium",
                "affected_rows": outlier_count,
                "metric": round((outlier_count / len(numeric_series)) * 100, 2),
                "details": f"Bounds [{lower_bound:.2f}, {upper_bound:.2f}]",
            }
        )

    issue_frame = pd.DataFrame(issues)
    logger.info("IQR outlier scan completed | issues=%s", len(issue_frame))
    return issue_frame


def detect_duplicate_identifier_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Detect duplicated identifier-like columns."""
    issues: list[dict[str, object]] = []
    candidate_columns = [column for column in ["_id", "url", "title"] if column in dataframe.columns]

    for column in candidate_columns:
        non_null = dataframe[column].dropna()
        duplicate_count = int(non_null.duplicated().sum())
        if duplicate_count == 0:
            continue

        issues.append(
            {
                "issue_type": "duplicate_identifier",
                "column": column,
                "severity": "high" if column in {"_id", "url"} else "medium",
                "affected_rows": duplicate_count,
                "metric": round((duplicate_count / len(non_null)) * 100, 2),
                "details": f"Detected duplicate values in {column}",
            }
        )

    issue_frame = pd.DataFrame(issues)
    logger.info("Duplicate identifier scan completed | issues=%s", len(issue_frame))
    return issue_frame


def detect_invalid_titles(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Detect empty, placeholder, or suspiciously short titles."""
    if "title" not in dataframe.columns:
        return pd.DataFrame()

    title_series = dataframe["title"].fillna("").astype(str).str.strip()
    empty_mask = title_series.eq("")
    placeholder_mask = title_series.str.match(TITLE_PLACEHOLDER_PATTERN)
    short_mask = title_series.str.len().between(1, 2, inclusive="both")

    issues = []
    for issue_type, mask, severity in [
        ("missing_title", empty_mask, "high"),
        ("placeholder_title", placeholder_mask & ~empty_mask, "medium"),
        ("too_short_title", short_mask, "medium"),
    ]:
        affected_rows = int(mask.sum())
        if affected_rows == 0:
            continue
        issues.append(
            {
                "issue_type": issue_type,
                "column": "title",
                "severity": severity,
                "affected_rows": affected_rows,
                "metric": round((affected_rows / len(dataframe)) * 100, 2),
                "details": f"{issue_type.replace('_', ' ')} detected in title field",
            }
        )

    issue_frame = pd.DataFrame(issues)
    logger.info("Invalid title scan completed | issues=%s", len(issue_frame))
    return issue_frame


def detect_format_inconsistencies(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Detect inconsistent date and URL formats."""
    issues: list[dict[str, object]] = []

    if "mention_date" in dataframe.columns:
        parsed_dates = pd.to_datetime(dataframe["mention_date"], errors="coerce")
        invalid_dates = int(dataframe["mention_date"].notna().sum() - parsed_dates.notna().sum())
        if invalid_dates:
            issues.append(
                {
                    "issue_type": "invalid_date_format",
                    "column": "mention_date",
                    "severity": "medium",
                    "affected_rows": invalid_dates,
                    "metric": round((invalid_dates / len(dataframe)) * 100, 2),
                    "details": "Some mention_date values could not be parsed",
                }
            )

    if "url" in dataframe.columns:
        url_series = dataframe["url"].fillna("").astype(str).str.strip()
        invalid_urls = int(((url_series != "") & ~url_series.str.match(URL_PATTERN)).sum())
        if invalid_urls:
            issues.append(
                {
                    "issue_type": "invalid_url_format",
                    "column": "url",
                    "severity": "medium",
                    "affected_rows": invalid_urls,
                    "metric": round((invalid_urls / len(dataframe)) * 100, 2),
                    "details": "Some URL values do not start with http:// or https://",
                }
            )

    issue_frame = pd.DataFrame(issues)
    logger.info("Format inconsistency scan completed | issues=%s", len(issue_frame))
    return issue_frame


def run_full_quality_audit(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Combine all quality checks into a single issues table."""
    missing_report = build_missing_value_report(dataframe)
    missing_issues = missing_report.loc[missing_report["missing_count"] > 0, ["column", "missing_severity", "missing_count", "missing_pct"]].copy()
    if not missing_issues.empty:
        missing_issues = missing_issues.rename(
            columns={
                "missing_severity": "severity",
                "missing_count": "affected_rows",
                "missing_pct": "metric",
            }
        )
        missing_issues["issue_type"] = "missing_values"
        missing_issues["details"] = "Column contains missing values"
        missing_issues = missing_issues[["issue_type", "column", "severity", "affected_rows", "metric", "details"]]

    issue_frames = [
        missing_issues,
        detect_zero_as_missing_numeric_fields(dataframe),
        detect_iqr_outliers(dataframe),
        detect_duplicate_identifier_rows(dataframe),
        detect_invalid_titles(dataframe),
        detect_format_inconsistencies(dataframe),
    ]
    non_empty_frames = [frame for frame in issue_frames if frame is not None and not frame.empty]
    if not non_empty_frames:
        combined = pd.DataFrame(columns=["issue_type", "column", "severity", "affected_rows", "metric", "details"])
    else:
        combined = pd.concat(non_empty_frames, ignore_index=True)
    logger.info("Full quality audit completed | issues=%s", len(combined))
    return combined


def build_quality_report(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Create the full column-level quality report."""
    return build_missing_value_report(dataframe)


def save_quality_report_csv(report: pd.DataFrame, output_path: Path = QUALITY_REPORT_PATH) -> Path:
    """Save the full quality report as CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Quality report CSV saved | path=%s", output_path)
    return output_path


def save_quality_issues_csv(issues: pd.DataFrame, output_path: Path = QUALITY_ISSUES_PATH) -> Path:
    """Save the aggregated issues audit as CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    issues.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Quality issues CSV saved | path=%s", output_path)
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


def run_quality_pipeline() -> dict[str, object]:
    """Run the complete quality-report workflow."""
    logger.info("Apple quality report pipeline started")
    dataframe = prepare_quality_dataset()
    report = build_quality_report(dataframe)
    issues = run_full_quality_audit(dataframe)
    quality_report_path = save_quality_report_csv(report)
    issues_path = save_quality_issues_csv(issues)
    heatmap_path = save_missing_value_heatmap(dataframe)
    selector_examples = build_selector_examples(dataframe)
    indexing_report_path = save_selector_examples(selector_examples, INDEXING_REPORT_PATH)
    logger.info(
        "Apple quality report pipeline finished | report=%s | issues=%s | heatmap=%s | selections=%s",
        quality_report_path,
        issues_path,
        heatmap_path,
        indexing_report_path,
    )
    return {
        "dataframe": dataframe,
        "report": report,
        "issues": issues,
        "quality_report_path": quality_report_path,
        "issues_path": issues_path,
        "heatmap_path": heatmap_path,
        "selection_report_path": indexing_report_path,
    }


def main() -> None:
    results = run_quality_pipeline()

    print("Missing value summary:")
    print(results["report"][["column", "missing_count", "missing_pct", "missing_severity"]])
    print("\nQuality issues summary:")
    print(results["issues"] if not results["issues"].empty else "No quality issues detected.")
    print(f"\nSaved quality report CSV to: {results['quality_report_path']}")
    print(f"Saved quality issues CSV to: {results['issues_path']}")
    print(f"Saved missing value heatmap to: {results['heatmap_path']}")
    print(f"Saved loc/iloc/filtering examples to: {results['selection_report_path']}")


if __name__ == "__main__":
    main()
