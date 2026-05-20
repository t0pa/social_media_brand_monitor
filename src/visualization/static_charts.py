"""Static visualization module for the Apple brand-monitor project."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.ticker import MaxNLocator


STATIC_PALETTE = {
    "primary": "#555B6E",
    "secondary": "#89B0AE",
    "accent": "#BEE3DB",
    "warm": "#E07A5F",
    "dark": "#283044",
    "gold": "#D4A373",
    "muted": "#6C757D",
}
SEABORN_PALETTE = [
    STATIC_PALETTE["primary"],
    STATIC_PALETTE["secondary"],
    STATIC_PALETTE["warm"],
    STATIC_PALETTE["gold"],
    STATIC_PALETTE["accent"],
    STATIC_PALETTE["muted"],
]


def _set_theme() -> None:
    """Apply a consistent static chart theme."""
    sns.set_theme(
        style="whitegrid",
        context="talk",
        palette=SEABORN_PALETTE,
        rc={
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.titleweight": "bold",
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "grid.alpha": 0.25,
        },
    )


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Create safe plotting columns without mutating the caller's DataFrame."""
    working = df.copy()

    for column in ["rating", "mention_year"]:
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")

    if "mention_date" in working.columns:
        working["mention_date"] = pd.to_datetime(working["mention_date"], errors="coerce", utc=True)
    elif "publishedAt" in working.columns:
        working["mention_date"] = pd.to_datetime(working["publishedAt"], errors="coerce", utc=True)
    elif "date" in working.columns:
        working["mention_date"] = pd.to_datetime(working["date"], errors="coerce", utc=True)

    if "mention_date" in working.columns and "mention_year" not in working.columns:
        working["mention_year"] = working["mention_date"].dt.year

    for column in ["title", "overview", "source", "author", "document_type", "language"]:
        if column in working.columns:
            working[column] = working[column].fillna("Unknown").astype(str).str.strip()
            working.loc[working[column] == "", column] = "Unknown"

    if "title" in working.columns:
        working["title_length"] = working["title"].str.len()
    if "overview" in working.columns:
        working["overview_length"] = working["overview"].str.len()

    return working


def _require_columns(df: pd.DataFrame, columns: list[str], chart_name: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{chart_name} requires columns {missing}, but they were not found in the DataFrame.")


def _save_figure(fig: plt.Figure, output_dir: str | Path, filename_stem: str) -> dict[str, Path]:
    """Save a figure to both PNG and PDF."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    png_path = output_path / f"{filename_stem}.png"
    pdf_path = output_path / f"{filename_stem}.pdf"

    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    return {"png": png_path, "pdf": pdf_path}


def plot_top_sources_barh(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Horizontal bar chart of the top Apple mention sources."""
    _set_theme()
    working = _prepare_dataframe(df)
    _require_columns(working, ["source"], "plot_top_sources_barh")

    top_sources = (
        working["source"]
        .replace("", "Unknown")
        .value_counts()
        .head(10)
        .sort_values(ascending=True)
    )

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.barh(top_sources.index, top_sources.values, color=STATIC_PALETTE["primary"])
    ax.set_title("Top 10 Sources Mentioning Apple")
    ax.set_xlabel("Mention Count")
    ax.set_ylabel("Source")

    for index, value in enumerate(top_sources.values):
        ax.text(value + 0.15, index, f"{int(value)}", va="center", fontsize=11, color=STATIC_PALETTE["dark"])

    ax.grid(axis="y", visible=False)
    saved_paths = _save_figure(fig, output_dir, "top_sources_barh")
    return {"figure": fig, "axes": ax, "paths": saved_paths}


def plot_mentions_and_rating_by_year(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Dual-axis view of yearly mention volume and average rating."""
    _set_theme()
    working = _prepare_dataframe(df)
    _require_columns(working, ["mention_year", "rating"], "plot_mentions_and_rating_by_year")

    yearly = (
        working.dropna(subset=["mention_year"])
        .groupby("mention_year", as_index=False)
        .agg(mention_count=("rating", "size"), avg_rating=("rating", "mean"))
        .sort_values("mention_year")
    )

    if yearly.empty:
        raise ValueError("plot_mentions_and_rating_by_year could not build a non-empty yearly summary.")

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.bar(yearly["mention_year"], yearly["mention_count"], color=STATIC_PALETTE["secondary"], alpha=0.85)
    ax1.set_title("Apple Mention Volume and Average Rating by Year")
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Mention Count", color=STATIC_PALETTE["secondary"])
    ax1.tick_params(axis="y", labelcolor=STATIC_PALETTE["secondary"])
    ax1.xaxis.set_major_locator(MaxNLocator(integer=True))

    ax2 = ax1.twinx()
    ax2.plot(
        yearly["mention_year"],
        yearly["avg_rating"],
        color=STATIC_PALETTE["warm"],
        marker="o",
        linewidth=2.5,
    )
    ax2.set_ylabel("Average Rating", color=STATIC_PALETTE["warm"])
    ax2.tick_params(axis="y", labelcolor=STATIC_PALETTE["warm"])

    saved_paths = _save_figure(fig, output_dir, "mentions_and_rating_by_year")
    return {"figure": fig, "axes": (ax1, ax2), "paths": saved_paths}


def plot_mentions_by_document_type(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Bar chart comparing Apple mention counts by document type."""
    _set_theme()
    working = _prepare_dataframe(df)
    _require_columns(working, ["document_type"], "plot_mentions_by_document_type")

    counts = (
        working["document_type"]
        .replace("", "Unknown")
        .value_counts()
        .head(10)
        .sort_values(ascending=False)
        .reset_index()
    )
    counts.columns = ["document_type", "mention_count"]

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(
        data=counts,
        x="document_type",
        y="mention_count",
        hue="document_type",
        dodge=False,
        legend=False,
        ax=ax,
    )
    ax.set_title("Apple Mentions by Document Type")
    ax.set_xlabel("Document Type")
    ax.set_ylabel("Mention Count")
    ax.tick_params(axis="x", rotation=20)

    saved_paths = _save_figure(fig, output_dir, "mentions_by_document_type")
    return {"figure": fig, "axes": ax, "paths": saved_paths}


def plot_rating_distribution(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Histogram showing the distribution of article ratings."""
    _set_theme()
    working = _prepare_dataframe(df)
    _require_columns(working, ["rating"], "plot_rating_distribution")

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.histplot(
        working.dropna(subset=["rating"]),
        x="rating",
        bins=12,
        kde=True,
        color=STATIC_PALETTE["warm"],
        edgecolor="white",
        ax=ax,
    )
    ax.set_title("Distribution of Apple Mention Ratings")
    ax.set_xlabel("Rating")
    ax.set_ylabel("Frequency")

    saved_paths = _save_figure(fig, output_dir, "rating_distribution")
    return {"figure": fig, "axes": ax, "paths": saved_paths}


def plot_rating_by_source_boxplot(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Boxplot comparing rating distributions across the busiest sources."""
    _set_theme()
    working = _prepare_dataframe(df)
    _require_columns(working, ["source", "rating"], "plot_rating_by_source_boxplot")

    top_sources = working["source"].value_counts().head(8).index
    plot_df = working[working["source"].isin(top_sources)].dropna(subset=["rating"]).copy()
    if plot_df.empty:
        raise ValueError("plot_rating_by_source_boxplot could not build a non-empty source/rating subset.")

    fig, ax = plt.subplots(figsize=(13, 7))
    sns.boxplot(
        data=plot_df,
        x="source",
        y="rating",
        hue="source",
        dodge=False,
        legend=False,
        ax=ax,
    )
    ax.set_title("Rating Distribution Across Top Apple Sources")
    ax.set_xlabel("Source")
    ax.set_ylabel("Rating")
    ax.tick_params(axis="x", rotation=25)

    saved_paths = _save_figure(fig, output_dir, "rating_by_source_boxplot")
    return {"figure": fig, "axes": ax, "paths": saved_paths}


def plot_title_length_vs_rating(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Scatter plot for title length versus rating, coloured by document type."""
    _set_theme()
    working = _prepare_dataframe(df)
    _require_columns(working, ["title", "rating", "document_type"], "plot_title_length_vs_rating")

    plot_df = working.dropna(subset=["rating"]).copy()
    if plot_df.empty:
        raise ValueError("plot_title_length_vs_rating requires at least one row with a numeric rating.")

    top_document_types = plot_df["document_type"].value_counts().head(5).index
    plot_df["document_type_grouped"] = plot_df["document_type"].where(
        plot_df["document_type"].isin(top_document_types),
        "Other",
    )

    fig, ax = plt.subplots(figsize=(11, 7))
    scatter_kwargs = {
        "data": plot_df,
        "x": "title_length",
        "y": "rating",
        "hue": "document_type_grouped",
        "alpha": 0.75,
        "ax": ax,
    }
    if "overview_length" in plot_df.columns:
        scatter_kwargs["size"] = "overview_length"
        scatter_kwargs["sizes"] = (40, 240)

    sns.scatterplot(**scatter_kwargs)
    ax.set_title("Title Length vs Rating for Apple Mentions")
    ax.set_xlabel("Title Length (characters)")
    ax.set_ylabel("Rating")

    saved_paths = _save_figure(fig, output_dir, "title_length_vs_rating")
    return {"figure": fig, "axes": ax, "paths": saved_paths}


def plot_language_year_heatmap(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Heatmap of language frequency across mention years."""
    _set_theme()
    working = _prepare_dataframe(df)
    _require_columns(working, ["language", "mention_year"], "plot_language_year_heatmap")

    top_languages = working["language"].replace("", "Unknown").value_counts().head(8).index
    filtered = working[working["language"].isin(top_languages)].dropna(subset=["mention_year"]).copy()

    pivot = pd.crosstab(filtered["language"], filtered["mention_year"])
    if pivot.empty:
        raise ValueError("plot_language_year_heatmap could not build a non-empty language/year pivot table.")

    fig, ax = plt.subplots(figsize=(12, 6.5))
    sns.heatmap(
        pivot,
        cmap="YlGnBu",
        annot=True,
        fmt="g",
        linewidths=0.5,
        cbar_kws={"label": "Mention Count"},
        ax=ax,
    )
    ax.set_title("Apple Mention Languages Across Years")
    ax.set_xlabel("Year")
    ax.set_ylabel("Language")

    saved_paths = _save_figure(fig, output_dir, "language_year_heatmap")
    return {"figure": fig, "axes": ax, "paths": saved_paths}


def plot_monthly_mentions_line(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Time-series line chart of monthly Apple mention volume."""
    _set_theme()
    working = _prepare_dataframe(df)
    _require_columns(working, ["mention_date"], "plot_monthly_mentions_line")

    monthly = (
        working.dropna(subset=["mention_date"])
        .set_index("mention_date")
        .resample("ME")
        .size()
        .rename("mention_count")
        .reset_index()
    )
    if monthly.empty:
        raise ValueError("plot_monthly_mentions_line could not build a non-empty monthly series.")

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.lineplot(
        data=monthly,
        x="mention_date",
        y="mention_count",
        marker="o",
        color=STATIC_PALETTE["primary"],
        linewidth=2.5,
        ax=ax,
    )
    ax.set_title("Monthly Apple Mention Trend")
    ax.set_xlabel("Month")
    ax.set_ylabel("Mention Count")

    saved_paths = _save_figure(fig, output_dir, "monthly_mentions_line")
    return {"figure": fig, "axes": ax, "paths": saved_paths}


def plot_dashboard_subplots(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Four-panel dashboard summarizing the Apple mention dataset."""
    _set_theme()
    working = _prepare_dataframe(df)
    _require_columns(
        working,
        ["source", "document_type", "rating", "mention_date"],
        "plot_dashboard_subplots",
    )

    top_sources = working["source"].value_counts().head(5).sort_values()
    document_counts = working["document_type"].value_counts().head(6)
    monthly = (
        working.dropna(subset=["mention_date"])
        .set_index("mention_date")
        .resample("ME")
        .size()
        .rename("mention_count")
        .reset_index()
    )
    document_counts_df = document_counts.rename_axis("document_type").reset_index(name="mention_count")

    fig, axes = plt.subplots(2, 2, figsize=(16, 11))
    fig.suptitle("Apple Brand Monitor Static Dashboard", fontsize=20, fontweight="bold")

    axes[0, 0].barh(top_sources.index, top_sources.values, color=STATIC_PALETTE["primary"])
    axes[0, 0].set_title("Top Sources")
    axes[0, 0].set_xlabel("Mentions")
    axes[0, 0].set_ylabel("Source")

    sns.barplot(
        data=document_counts_df,
        x="document_type",
        y="mention_count",
        hue="document_type",
        dodge=False,
        legend=False,
        ax=axes[0, 1],
    )
    axes[0, 1].set_title("Document Type Mix")
    axes[0, 1].set_xlabel("Document Type")
    axes[0, 1].set_ylabel("Mentions")
    axes[0, 1].tick_params(axis="x", rotation=20)

    sns.histplot(working.dropna(subset=["rating"]), x="rating", bins=12, color=STATIC_PALETTE["warm"], ax=axes[1, 0])
    axes[1, 0].set_title("Rating Distribution")
    axes[1, 0].set_xlabel("Rating")
    axes[1, 0].set_ylabel("Frequency")

    sns.lineplot(data=monthly, x="mention_date", y="mention_count", marker="o", color=STATIC_PALETTE["secondary"], ax=axes[1, 1])
    axes[1, 1].set_title("Monthly Mentions")
    axes[1, 1].set_xlabel("Month")
    axes[1, 1].set_ylabel("Mentions")

    fig.tight_layout(rect=[0, 0, 1, 0.97])
    saved_paths = _save_figure(fig, output_dir, "dashboard_subplots")
    return {"figure": fig, "axes": axes, "paths": saved_paths}


STATIC_CHART_FUNCTIONS = [
    plot_top_sources_barh,
    plot_mentions_and_rating_by_year,
    plot_mentions_by_document_type,
    plot_rating_distribution,
    plot_rating_by_source_boxplot,
    plot_title_length_vs_rating,
    plot_language_year_heatmap,
    plot_monthly_mentions_line,
    plot_dashboard_subplots,
]
