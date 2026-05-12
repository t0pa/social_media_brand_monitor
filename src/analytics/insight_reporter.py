"""Insight reporting for Apple brand-monitor analytics."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)


def top_keywords_by_mention_count(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    summary = (
        df.groupby("primary_keyword")
        .agg(
            mention_count=("_id", "count"),
            avg_overview_chars=("overview_length", "mean"),
            avg_rating=("rating", "mean"),
        )
        .sort_values("mention_count", ascending=False)
        .head(top_n)
        .reset_index()
    )
    return summary


def source_share_summary(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    counts = df["source"].value_counts(dropna=False).head(top_n).reset_index()
    counts.columns = ["source", "mention_count"]
    counts["share_pct"] = (counts["mention_count"] / len(df) * 100).round(2)
    return counts


def yearly_release_volume(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("mention_year")
        .agg(
            mention_count=("_id", "count"),
            unique_sources=("source", "nunique"),
        )
        .reset_index()
        .sort_values("mention_year")
    )
    return summary


def language_distribution(df: pd.DataFrame) -> pd.DataFrame:
    summary = df["language"].fillna("unknown").value_counts(dropna=False).reset_index()
    summary.columns = ["language", "mention_count"]
    summary["share_pct"] = (summary["mention_count"] / len(df) * 100).round(2)
    return summary


def run_all_questions(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Answer the main Apple brand-monitor analytical questions."""
    keyword_summary = top_keywords_by_mention_count(df)
    source_summary = source_share_summary(df)
    yearly_summary = yearly_release_volume(df)
    language_summary = language_distribution(df)

    if not keyword_summary.empty:
        top_keyword = keyword_summary.iloc[0]
        logger.info(
            "Insight | top_keyword=%s | mention_count=%s",
            top_keyword["primary_keyword"],
            top_keyword["mention_count"],
        )
    if not source_summary.empty:
        top_source = source_summary.iloc[0]
        logger.info(
            "Insight | top_source=%s | share_pct=%s",
            top_source["source"],
            top_source["share_pct"],
        )
    if not yearly_summary.empty:
        peak_year = yearly_summary.sort_values("mention_count", ascending=False).iloc[0]
        logger.info(
            "Insight | peak_year=%s | mention_count=%s",
            peak_year["mention_year"],
            peak_year["mention_count"],
        )
    if not language_summary.empty:
        top_language = language_summary.iloc[0]
        logger.info(
            "Insight | top_language=%s | share_pct=%s",
            top_language["language"],
            top_language["share_pct"],
        )

    return {
        "keyword_summary": keyword_summary,
        "source_summary": source_summary,
        "yearly_summary": yearly_summary,
        "language_summary": language_summary,
    }


def save_keyword_chart(keyword_summary: pd.DataFrame, output_path: Path) -> Path:
    """Save a bar chart of top Apple keywords by mention count."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(9, 4.5))
    bars = plt.bar(keyword_summary["primary_keyword"], keyword_summary["mention_count"], color="#457b9d")
    plt.title("Top Apple Topics by Mention Count")
    plt.xlabel("Primary Keyword")
    plt.ylabel("Mention Count")
    plt.xticks(rotation=30, ha="right")
    for bar, value in zip(bars, keyword_summary["mention_count"]):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(int(value)), ha="center", va="bottom")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    logger.info("Saved keyword insight chart | path=%s", output_path)
    return output_path


def save_source_share_chart(source_summary: pd.DataFrame, output_path: Path) -> Path:
    """Save a chart of top Apple mention sources."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(9, 4.5))
    bars = plt.bar(source_summary["source"], source_summary["mention_count"], color="#2a9d8f")
    plt.title("Top Sources Covering Apple")
    plt.xlabel("Source")
    plt.ylabel("Mention Count")
    plt.xticks(rotation=30, ha="right")
    for bar, value in zip(bars, source_summary["mention_count"]):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(int(value)), ha="center", va="bottom")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    logger.info("Saved source-share chart | path=%s", output_path)
    return output_path


def save_yearly_volume_chart(yearly_summary: pd.DataFrame, output_path: Path) -> Path:
    """Save a chart of Apple mention volume by year."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(8, 4.5))
    bars = plt.bar(yearly_summary["mention_year"].astype(str), yearly_summary["mention_count"], color="#e76f51")
    plt.title("Apple Mention Volume by Year")
    plt.xlabel("Year")
    plt.ylabel("Mention Count")
    for bar, value in zip(bars, yearly_summary["mention_count"]):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(int(value)), ha="center", va="bottom")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    logger.info("Saved yearly-volume chart | path=%s", output_path)
    return output_path


def save_language_distribution_chart(language_summary: pd.DataFrame, output_path: Path) -> Path:
    """Save a chart of language distribution for Apple mentions."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(8, 4.5))
    bars = plt.bar(language_summary["language"], language_summary["mention_count"], color="#6d597a")
    plt.title("Language Distribution of Apple Mentions")
    plt.xlabel("Language")
    plt.ylabel("Mention Count")
    for bar, value in zip(bars, language_summary["mention_count"]):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(int(value)), ha="center", va="bottom")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    logger.info("Saved language-distribution chart | path=%s", output_path)
    return output_path


def save_question_report(insights: dict[str, pd.DataFrame], output_path: Path) -> Path:
    """Save four explicit analytical questions with quantified findings."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    keyword_summary = insights.get("keyword_summary", pd.DataFrame())
    source_summary = insights.get("source_summary", pd.DataFrame())
    yearly_summary = insights.get("yearly_summary", pd.DataFrame())
    language_summary = insights.get("language_summary", pd.DataFrame())

    lines: list[str] = [
        "Apple Brand Monitor Analytical Questions",
        "=" * 42,
        "",
    ]

    if not keyword_summary.empty:
        top_keyword = keyword_summary.iloc[0]
        lines.extend([
            "Q1. Which Apple topic appears most often in the cleaned dataset?",
            (
                f"Finding: {top_keyword['primary_keyword']} leads with "
                f"{int(top_keyword['mention_count'])} mentions."
            ),
            "Code: top_keywords_by_mention_count(combined_df)",
            "Chart: data/processed/analytics/question1_top_keywords.png",
            "",
        ])

    if not source_summary.empty:
        top_source = source_summary.iloc[0]
        lines.extend([
            "Q2. Which source contributes the largest share of Apple coverage?",
            (
                f"Finding: {top_source['source']} contributes "
                f"{int(top_source['mention_count'])} mentions, or "
                f"{float(top_source['share_pct']):.2f}% of the dataset."
            ),
            "Code: source_share_summary(combined_df)",
            "Chart: data/processed/analytics/question2_top_sources.png",
            "",
        ])

    if not yearly_summary.empty:
        peak_year = yearly_summary.sort_values("mention_count", ascending=False).iloc[0]
        lines.extend([
            "Q3. In which year does Apple mention volume peak?",
            (
                f"Finding: {int(peak_year['mention_year'])} is the peak year with "
                f"{int(peak_year['mention_count'])} mentions."
            ),
            "Code: yearly_release_volume(combined_df)",
            "Chart: data/processed/analytics/question3_yearly_volume.png",
            "",
        ])

    if not language_summary.empty:
        top_language = language_summary.iloc[0]
        lines.extend([
            "Q4. What is the dominant language in Apple coverage?",
            (
                f"Finding: {top_language['language']} accounts for "
                f"{int(top_language['mention_count'])} mentions, or "
                f"{float(top_language['share_pct']):.2f}% of all rows."
            ),
            "Code: language_distribution(combined_df)",
            "Chart: data/processed/analytics/question4_language_distribution.png",
            "",
        ])

    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Saved analytical question report | path=%s", output_path)
    return output_path


def save_insight_summary(insights: dict[str, pd.DataFrame], output_path: Path) -> Path:
    """Save a concise text summary of the main Apple-monitor findings."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = ["Apple Brand Monitor Lab 10 Insights", "=" * 40, ""]

    keyword_summary = insights.get("keyword_summary", pd.DataFrame())
    source_summary = insights.get("source_summary", pd.DataFrame())
    yearly_summary = insights.get("yearly_summary", pd.DataFrame())
    language_summary = insights.get("language_summary", pd.DataFrame())

    if not keyword_summary.empty:
        top_keyword = keyword_summary.iloc[0]
        lines.append(
            f"Top keyword/topic: {top_keyword['primary_keyword']} "
            f"with {int(top_keyword['mention_count'])} mentions."
        )
    if not source_summary.empty:
        top_source = source_summary.iloc[0]
        lines.append(
            f"Top source: {top_source['source']} "
            f"with {int(top_source['mention_count'])} mentions "
            f"({float(top_source['share_pct']):.2f}% share)."
        )
    if not yearly_summary.empty:
        peak_year = yearly_summary.sort_values("mention_count", ascending=False).iloc[0]
        lines.append(
            f"Peak year: {int(peak_year['mention_year'])} "
            f"with {int(peak_year['mention_count'])} Apple mentions."
        )
    if not language_summary.empty:
        top_language = language_summary.iloc[0]
        lines.append(
            f"Most common language: {top_language['language']} "
            f"({float(top_language['share_pct']):.2f}% of mentions)."
        )

    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Saved insight summary text | path=%s", output_path)
    return output_path
