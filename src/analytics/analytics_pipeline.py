"""Reusable Lab 10 analytics pipeline for Apple brand monitoring."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analytics.aggregator import (
    add_length_metrics,
    save_yearly_trends_chart,
    source_summary,
    top_n_per_group,
    yearly_trends,
)
from src.analytics.data_combiner import compare_join_types, merge_on_key, save_join_comparison_chart
from src.analytics.db_connector import (
    create_article_metrics_table,
    get_mysql_connection,
    populate_article_metrics,
    query_article_metrics,
)
from src.analytics.explorer import filter_apple_mentions
from src.analytics.insight_reporter import (
    run_all_questions,
    save_insight_summary,
    save_keyword_chart,
    save_language_distribution_chart,
    save_question_report,
    save_source_share_chart,
    save_yearly_volume_chart,
)
from src.analytics.mongo_pipeline import build_source_mentions_pipeline, run_pipeline as run_mongo_aggregation
from src.analytics.pivot_builder import (
    add_primary_keyword,
    build_keyword_year_pivot,
    build_language_year_crosstab,
    melt_metrics,
)
from src.analytics.time_series import (
    add_rolling_averages,
    build_monthly_mentions,
    parse_mention_dates,
    resample_mentions,
    save_rolling_mentions_chart,
)
from src.utils.logger import get_logger


logger = get_logger(__name__)

OUTPUT_DIR = Path("data/processed/analytics")


def _namespace_mysql_columns(mysql_df: pd.DataFrame) -> pd.DataFrame:
    """Rename MySQL columns to avoid collisions with cleaned metadata columns."""
    renamed = mysql_df.rename(columns={"mention_id": "_id"}).copy()
    rename_map = {
        column: f"mysql_{column}"
        for column in renamed.columns
        if column != "_id"
    }
    renamed = renamed.rename(columns=rename_map)
    logger.info("Namespaced MySQL columns | columns=%s", list(renamed.columns))
    return renamed


def run_analytics_pipeline(cleaned_csv_path: Path | str) -> dict[str, object]:
    """Run the Apple-specific Lab 10 analytics workflow."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cleaned_df = pd.read_csv(cleaned_csv_path)
    logger.info("Analytics pipeline loaded cleaned CSV | path=%s | rows=%s", cleaned_csv_path, len(cleaned_df))

    filtered_df = filter_apple_mentions(cleaned_df, keyword="apple")
    filtered_df = parse_mention_dates(filtered_df)
    filtered_df["rating"] = pd.to_numeric(filtered_df.get("rating"), errors="coerce")
    filtered_df = add_length_metrics(filtered_df)
    filtered_df = add_primary_keyword(filtered_df)

    mysql_df = pd.DataFrame()
    mysql_available = True
    try:
        connection = get_mysql_connection()
        create_article_metrics_table(connection)
        populate_article_metrics(connection, filtered_df)
        mysql_df = query_article_metrics(where_clause="mention_year IS NOT NULL")
        connection.close()
    except Exception as exc:
        mysql_available = False
        logger.warning("MySQL analytics stage skipped because connection/query failed: %s", exc)

    metadata_df = filtered_df[[
        "_id", "title", "source", "document_type", "language", "mention_year",
        "mention_month", "mention_weekday", "mention_quarter",
        "overview", "mention_date", "rating", "title_length", "overview_length", "primary_keyword"
    ]].copy()

    if mysql_available and not mysql_df.empty:
        mysql_df = _namespace_mysql_columns(mysql_df)
        join_comparison = compare_join_types(metadata_df, mysql_df, key="_id")
        save_join_comparison_chart(join_comparison, OUTPUT_DIR / "join_type_comparison.png")
        combined_df = merge_on_key(metadata_df, mysql_df, key="_id", how="left")
    else:
        join_comparison = pd.DataFrame(
            [{"join_type": "left", "row_count": len(metadata_df)}]
        )
        combined_df = metadata_df.copy()

    long_metrics = melt_metrics(
        combined_df,
        id_vars=["_id", "title", "source", "primary_keyword", "mention_year"],
        value_vars=[column for column in ["rating", "title_length", "overview_length"] if column in combined_df.columns],
    )
    long_metrics.to_csv(OUTPUT_DIR / "long_metrics.csv", index=False, encoding="utf-8")

    source_analysis = source_summary(combined_df)
    source_analysis.to_csv(OUTPUT_DIR / "source_analysis.csv", index=False, encoding="utf-8")

    year_trends = yearly_trends(combined_df)
    year_trends.to_csv(OUTPUT_DIR / "yearly_trends.csv", index=False, encoding="utf-8")
    save_yearly_trends_chart(year_trends, OUTPUT_DIR / "yearly_trends.png")

    keyword_pivot = build_keyword_year_pivot(combined_df)
    keyword_pivot.to_csv(OUTPUT_DIR / "pivot_keyword_year.csv", encoding="utf-8")

    language_crosstab = build_language_year_crosstab(combined_df)
    language_crosstab.to_csv(OUTPUT_DIR / "language_year_crosstab.csv", encoding="utf-8")

    monthly_mentions = build_monthly_mentions(combined_df)
    monthly_mentions = add_rolling_averages(monthly_mentions)
    monthly_mentions.to_csv(OUTPUT_DIR / "monthly_mentions.csv", index=False, encoding="utf-8")
    save_rolling_mentions_chart(monthly_mentions, OUTPUT_DIR / "rolling_mentions.png")

    yearly_resampled = resample_mentions(combined_df, frequency="YE")
    yearly_resampled.to_csv(OUTPUT_DIR / "resampled_yearly_mentions.csv", index=False, encoding="utf-8")

    mongo_pipeline_df = run_mongo_aggregation(build_source_mentions_pipeline())
    if not mongo_pipeline_df.empty:
        mongo_pipeline_df.to_csv(OUTPUT_DIR / "mongo_source_mentions.csv", index=False, encoding="utf-8")

    insights = run_all_questions(combined_df)
    save_keyword_chart(insights["keyword_summary"], OUTPUT_DIR / "top_keywords.png")
    save_keyword_chart(insights["keyword_summary"], OUTPUT_DIR / "question1_top_keywords.png")
    save_source_share_chart(insights["source_summary"], OUTPUT_DIR / "question2_top_sources.png")
    save_yearly_volume_chart(insights["yearly_summary"], OUTPUT_DIR / "question3_yearly_volume.png")
    save_language_distribution_chart(insights["language_summary"], OUTPUT_DIR / "question4_language_distribution.png")
    save_insight_summary(insights, OUTPUT_DIR / "insight_summary.txt")
    save_question_report(insights, OUTPUT_DIR / "analytical_questions.txt")

    top_titles_by_keyword = top_n_per_group(
        combined_df,
        group_column="primary_keyword",
        sort_column="overview_length",
        n=3,
    )
    top_titles_by_keyword.to_csv(OUTPUT_DIR / "top_titles_by_keyword.csv", index=False, encoding="utf-8")

    logger.info(
        "Analytics pipeline finished | rows=%s | outputs_dir=%s | mysql_used=%s",
        len(combined_df),
        OUTPUT_DIR,
        mysql_available and not mysql_df.empty,
    )
    return {
        "combined_df": combined_df,
        "join_comparison": join_comparison,
        "source_analysis": source_analysis,
        "yearly_trends": year_trends,
        "keyword_pivot": keyword_pivot,
        "language_crosstab": language_crosstab,
        "monthly_mentions": monthly_mentions,
        "yearly_resampled": yearly_resampled,
        "mongo_pipeline_df": mongo_pipeline_df,
        "insights": insights,
        "top_titles_by_keyword": top_titles_by_keyword,
    }
