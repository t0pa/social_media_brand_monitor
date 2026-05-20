"""Interactive Plotly charts for the Apple brand-monitor project."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


PLOTLY_SEQUENCE = ["#555B6E", "#89B0AE", "#E07A5F", "#D4A373", "#BEE3DB", "#6C757D"]
PLOTLY_TEMPLATE = "plotly_white"


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Create safe plotting columns without mutating caller data."""
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

    if "mention_date" in working.columns:
        mention_date_naive = working["mention_date"].dt.tz_localize(None)
        working["mention_month"] = mention_date_naive.dt.to_period("M").dt.to_timestamp()

    return working


def _require_columns(df: pd.DataFrame, columns: list[str], chart_name: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{chart_name} requires columns {missing}, but they were not found in the DataFrame.")


def _apply_layout(fig: go.Figure, title: str) -> go.Figure:
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        colorway=PLOTLY_SEQUENCE,
        title=title,
        font={"family": "Arial, sans-serif"},
        hoverlabel={"bgcolor": "white"},
    )
    return fig


def _save_html(fig: go.Figure, output_dir: str | Path, filename_stem: str) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    html_path = output_path / f"{filename_stem}.html"
    fig.write_html(html_path, full_html=True, include_plotlyjs=True)
    return html_path


def interactive_top_sources(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Interactive bar chart of the top sources mentioning Apple."""
    working = _prepare_dataframe(df)
    _require_columns(working, ["source", "document_type", "language"], "interactive_top_sources")

    summary = (
        working.groupby("source", dropna=False)
        .agg(
            mention_count=("source", "size"),
            unique_document_types=("document_type", "nunique"),
            unique_languages=("language", "nunique"),
        )
        .reset_index()
        .sort_values("mention_count", ascending=False)
        .head(10)
        .sort_values("mention_count", ascending=True)
    )

    fig = px.bar(
        summary,
        x="mention_count",
        y="source",
        orientation="h",
        color="mention_count",
        color_continuous_scale="Tealgrn",
        hover_data={
            "source": True,
            "mention_count": ":.0f",
            "unique_document_types": True,
            "unique_languages": True,
        },
        labels={
            "mention_count": "Mention Count",
            "source": "Source",
            "unique_document_types": "Unique Document Types",
            "unique_languages": "Unique Languages",
        },
    )
    _apply_layout(fig, "Top Sources Mentioning Apple")
    html_path = _save_html(fig, output_dir, "interactive_top_sources")
    return {"figure": fig, "path": html_path}


def interactive_mentions_timeline(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Interactive monthly timeline of Apple mention volume and average rating."""
    working = _prepare_dataframe(df)
    _require_columns(working, ["mention_date", "rating", "source"], "interactive_mentions_timeline")

    timeline = (
        working.dropna(subset=["mention_month"])
        .groupby("mention_month", as_index=False)
        .agg(
            mention_count=("source", "size"),
            avg_rating=("rating", "mean"),
            unique_sources=("source", "nunique"),
        )
        .sort_values("mention_month")
    )

    fig = px.line(
        timeline,
        x="mention_month",
        y="mention_count",
        markers=True,
        custom_data=["avg_rating", "unique_sources"],
        hover_data={
            "mention_month": "|%Y-%m",
            "mention_count": ":.0f",
            "avg_rating": ":.2f",
            "unique_sources": True,
        },
        labels={
            "mention_month": "Month",
            "mention_count": "Mention Count",
            "avg_rating": "Average Rating",
            "unique_sources": "Unique Sources",
        },
    )
    fig.update_traces(
        line={"width": 3},
        marker={"size": 8},
        hovertemplate=(
            "Month=%{x|%Y-%m}<br>"
            "Mentions=%{y}<br>"
            "Average rating=%{customdata[0]:.2f}<br>"
            "Unique sources=%{customdata[1]}<extra></extra>"
        ),
    )
    _apply_layout(fig, "Monthly Apple Mention Timeline")
    html_path = _save_html(fig, output_dir, "interactive_mentions_timeline")
    return {"figure": fig, "path": html_path}


def interactive_rating_by_document_type(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Interactive box plot of rating distributions across document types."""
    working = _prepare_dataframe(df)
    _require_columns(
        working,
        ["document_type", "rating", "source", "language", "title"],
        "interactive_rating_by_document_type",
    )

    plot_df = working.dropna(subset=["rating"]).copy()
    top_types = plot_df["document_type"].value_counts().head(8).index
    plot_df = plot_df[plot_df["document_type"].isin(top_types)].copy()
    if plot_df.empty:
        raise ValueError("interactive_rating_by_document_type could not build a non-empty plotting subset.")

    fig = px.box(
        plot_df,
        x="document_type",
        y="rating",
        color="document_type",
        points="all",
        hover_data={
            "title": True,
            "source": True,
            "language": True,
            "rating": ":.2f",
            "document_type": True,
        },
        labels={
            "document_type": "Document Type",
            "rating": "Rating",
            "source": "Source",
            "language": "Language",
            "title": "Title",
        },
    )
    fig.update_traces(jitter=0.25, pointpos=0)
    _apply_layout(fig, "Rating Distribution by Document Type")
    html_path = _save_html(fig, output_dir, "interactive_rating_by_document_type")
    return {"figure": fig, "path": html_path}


def interactive_title_length_scatter(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Interactive scatter plot for title length versus rating."""
    working = _prepare_dataframe(df)
    _require_columns(
        working,
        ["title", "rating", "document_type", "source", "overview"],
        "interactive_title_length_scatter",
    )

    plot_df = working.dropna(subset=["rating"]).copy()
    if plot_df.empty:
        raise ValueError("interactive_title_length_scatter requires at least one row with numeric rating.")

    fig = px.scatter(
        plot_df,
        x="title_length",
        y="rating",
        color="document_type",
        size="overview_length",
        hover_name="title",
        hover_data={
            "source": True,
            "document_type": True,
            "overview_length": True,
            "rating": ":.2f",
            "title_length": True,
        },
        labels={
            "title_length": "Title Length",
            "rating": "Rating",
            "document_type": "Document Type",
            "overview_length": "Overview Length",
            "source": "Source",
        },
    )
    fig.update_traces(marker={"opacity": 0.75, "line": {"width": 0.5, "color": "white"}})
    _apply_layout(fig, "Title Length vs Rating for Apple Mentions")
    html_path = _save_html(fig, output_dir, "interactive_title_length_scatter")
    return {"figure": fig, "path": html_path}


def interactive_multi_layout_dashboard(df: pd.DataFrame, output_dir: str | Path) -> dict[str, object]:
    """Interactive 2x2 dashboard using Plotly Graph Objects."""
    working = _prepare_dataframe(df)
    _require_columns(
        working,
        ["source", "document_type", "rating", "language", "mention_date"],
        "interactive_multi_layout_dashboard",
    )

    source_summary = (
        working.groupby("source", dropna=False)
        .agg(
            mention_count=("source", "size"),
            avg_rating=("rating", "mean"),
            unique_languages=("language", "nunique"),
        )
        .reset_index()
        .sort_values("mention_count", ascending=False)
        .head(6)
        .sort_values("mention_count", ascending=True)
    )
    document_summary = (
        working.groupby("document_type", dropna=False)
        .agg(
            mention_count=("document_type", "size"),
            avg_rating=("rating", "mean"),
            unique_sources=("source", "nunique"),
        )
        .reset_index()
        .sort_values("mention_count", ascending=False)
        .head(6)
    )
    language_summary = (
        working.groupby("language", dropna=False)
        .agg(
            mention_count=("language", "size"),
            avg_rating=("rating", "mean"),
            unique_sources=("source", "nunique"),
        )
        .reset_index()
        .sort_values("mention_count", ascending=False)
        .head(6)
    )
    monthly = (
        working.dropna(subset=["mention_month"])
        .groupby("mention_month", as_index=False)
        .agg(
            mention_count=("source", "size"),
            avg_rating=("rating", "mean"),
            unique_sources=("source", "nunique"),
        )
        .sort_values("mention_month")
    )

    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=("Top Sources", "Document Type Mix", "Monthly Mentions", "Language Coverage"),
    )

    fig.add_trace(
        go.Bar(
            x=source_summary["mention_count"],
            y=source_summary["source"],
            orientation="h",
            marker={"color": PLOTLY_SEQUENCE[0]},
            customdata=source_summary[["avg_rating", "unique_languages"]].to_numpy(),
            hovertemplate=(
                "Source=%{y}<br>"
                "Mentions=%{x}<br>"
                "Average rating=%{customdata[0]:.2f}<br>"
                "Unique languages=%{customdata[1]}<extra></extra>"
            ),
            showlegend=False,
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=document_summary["document_type"],
            y=document_summary["mention_count"],
            marker={"color": PLOTLY_SEQUENCE[1]},
            customdata=document_summary[["avg_rating", "unique_sources"]].to_numpy(),
            hovertemplate=(
                "Document type=%{x}<br>"
                "Mentions=%{y}<br>"
                "Average rating=%{customdata[0]:.2f}<br>"
                "Unique sources=%{customdata[1]}<extra></extra>"
            ),
            showlegend=False,
        ),
        row=1,
        col=2,
    )

    fig.add_trace(
        go.Scatter(
            x=monthly["mention_month"],
            y=monthly["mention_count"],
            mode="lines+markers",
            marker={"size": 7, "color": PLOTLY_SEQUENCE[2]},
            line={"width": 3, "color": PLOTLY_SEQUENCE[2]},
            customdata=monthly[["avg_rating", "unique_sources"]].to_numpy(),
            hovertemplate=(
                "Month=%{x|%Y-%m}<br>"
                "Mentions=%{y}<br>"
                "Average rating=%{customdata[0]:.2f}<br>"
                "Unique sources=%{customdata[1]}<extra></extra>"
            ),
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=language_summary["language"],
            y=language_summary["mention_count"],
            marker={"color": PLOTLY_SEQUENCE[3]},
            customdata=language_summary[["avg_rating", "unique_sources"]].to_numpy(),
            hovertemplate=(
                "Language=%{x}<br>"
                "Mentions=%{y}<br>"
                "Average rating=%{customdata[0]:.2f}<br>"
                "Unique sources=%{customdata[1]}<extra></extra>"
            ),
            showlegend=False,
        ),
        row=2,
        col=2,
    )

    fig.update_xaxes(title_text="Mention Count", row=1, col=1)
    fig.update_yaxes(title_text="Source", row=1, col=1)
    fig.update_xaxes(title_text="Document Type", row=1, col=2)
    fig.update_yaxes(title_text="Mention Count", row=1, col=2)
    fig.update_xaxes(title_text="Month", row=2, col=1)
    fig.update_yaxes(title_text="Mention Count", row=2, col=1)
    fig.update_xaxes(title_text="Language", row=2, col=2)
    fig.update_yaxes(title_text="Mention Count", row=2, col=2)

    fig.update_layout(height=900)
    _apply_layout(fig, "Apple Brand Monitor Interactive Dashboard")
    html_path = _save_html(fig, output_dir, "interactive_multi_layout_dashboard")
    return {"figure": fig, "path": html_path}


INTERACTIVE_CHART_FUNCTIONS = [
    interactive_top_sources,
    interactive_mentions_timeline,
    interactive_rating_by_document_type,
    interactive_title_length_scatter,
    interactive_multi_layout_dashboard,
]
