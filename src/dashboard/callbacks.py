"""Dash callbacks for the Apple brand monitoring dashboard."""

from __future__ import annotations

import random

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, html

from src.dashboard.data_access import filter_mentions, load_mentions


dashboard_df = load_mentions()
COMMON_INPUTS = [
    Input("source-filter", "value"),
    Input("document-type-filter", "value"),
    Input("year-range-filter", "value"),
    Input("search-filter", "value"),
]


def _filtered(source: str, document_type: str, year_range: list[int], search_text: str | None) -> pd.DataFrame:
    return filter_mentions(source, document_type, year_range, search_text, dashboard_df)


def _empty_figure(message: str) -> go.Figure:
    figure = go.Figure()
    figure.add_annotation(text=message, x=0.5, y=0.5, showarrow=False, font={"size": 16})
    figure.update_layout(
        template="plotly_dark",
        paper_bgcolor="#181c21",
        plot_bgcolor="#181c21",
        xaxis={"visible": False},
        yaxis={"visible": False},
        margin={"l": 30, "r": 20, "t": 50, "b": 30},
    )
    return figure


def _style_figure(figure: go.Figure, title: str) -> go.Figure:
    figure.update_layout(
        title=title,
        template="plotly_dark",
        paper_bgcolor="#181c21",
        plot_bgcolor="#181c21",
        font={"color": "#edf2f7"},
        margin={"l": 40, "r": 20, "t": 58, "b": 42},
        legend_title_text="",
    )
    return figure


def register_callbacks(app) -> None:
    """Register all dashboard callbacks on the Dash app."""

    @app.callback(
        Output("kpi-total-mentions", "children"),
        Output("kpi-average-rating", "children"),
        Output("kpi-top-source", "children"),
        Output("kpi-document-types", "children"),
        *COMMON_INPUTS,
    )
    def update_kpis(source, document_type, year_range, search_text):
        df = _filtered(source, document_type, year_range, search_text)
        total = f"{len(df):,}"
        average_rating = "N/A" if df["rating"].dropna().empty else f"{df['rating'].mean():.2f}"
        top_source = "N/A" if df.empty else df["source"].value_counts().idxmax()
        doc_types = f"{df['document_type'].nunique():,}" if not df.empty else "0"
        return total, average_rating, top_source, doc_types

    @app.callback(Output("source-volume-chart", "figure"), *COMMON_INPUTS)
    def update_source_volume_chart(source, document_type, year_range, search_text):
        df = _filtered(source, document_type, year_range, search_text)
        if df.empty:
            return _empty_figure("No matching Apple mentions")
        chart_df = df.groupby("source", as_index=False).size().sort_values("size", ascending=False).head(12)
        figure = px.bar(chart_df, x="size", y="source", orientation="h", color="size", color_continuous_scale="Teal")
        figure.update_yaxes(categoryorder="total ascending", title="")
        figure.update_xaxes(title="Mentions")
        return _style_figure(figure, "Top Apple Mention Sources")

    @app.callback(Output("rating-distribution-chart", "figure"), *COMMON_INPUTS)
    def update_rating_distribution_chart(source, document_type, year_range, search_text):
        df = _filtered(source, document_type, year_range, search_text).dropna(subset=["rating"])
        if df.empty:
            return _empty_figure("No rating data for the selected filters")
        figure = px.histogram(df, x="rating", color="document_type", nbins=12, barmode="overlay")
        figure.update_xaxes(title="Rating")
        figure.update_yaxes(title="Mention count")
        return _style_figure(figure, "Rating Distribution by Document Type")

    @app.callback(Output("rating-source-scatter-chart", "figure"), *COMMON_INPUTS)
    def update_rating_source_scatter_chart(source, document_type, year_range, search_text):
        df = _filtered(source, document_type, year_range, search_text).dropna(subset=["rating"])
        if df.empty:
            return _empty_figure("No source/rating data for the selected filters")
        chart_df = df.copy()
        chart_df["title_short"] = chart_df["title"].str.slice(0, 80)
        figure = px.scatter(
            chart_df,
            x="mention_year",
            y="rating",
            color="document_type",
            size=[1] * len(chart_df),
            hover_name="title_short",
            hover_data={"source": True, "author": True, "mention_year": True},
        )
        figure.update_xaxes(title="Mention year")
        figure.update_yaxes(title="Rating")
        return _style_figure(figure, "Ratings Over Time")

    @app.callback(Output("yearly-trend-chart", "figure"), *COMMON_INPUTS)
    def update_yearly_trend_chart(source, document_type, year_range, search_text):
        df = _filtered(source, document_type, year_range, search_text)
        if df.empty:
            return _empty_figure("No yearly trend data for the selected filters")
        chart_df = df.groupby(["mention_year", "document_type"], as_index=False).size()
        figure = px.line(chart_df, x="mention_year", y="size", color="document_type", markers=True)
        figure.update_xaxes(title="Mention year", dtick=1)
        figure.update_yaxes(title="Mentions")
        return _style_figure(figure, "Apple Mentions by Year")

    @app.callback(Output("recent-mentions-table", "children"), *COMMON_INPUTS)
    def update_recent_mentions_table(source, document_type, year_range, search_text):
        df = _filtered(source, document_type, year_range, search_text)
        if df.empty:
            return html.Div("No matching mentions.", className="text-secondary")
        recent = df.sort_values("mention_date", ascending=False).head(6)
        rows = [
            html.Tr(
                [
                    html.Td(row.get("title", "Unknown")[:70]),
                    html.Td(row.get("source", "Unknown")[:32]),
                    html.Td(str(row.get("mention_year", ""))),
                ]
            )
            for _, row in recent.iterrows()
        ]
        return dbc.Table(
            [html.Thead(html.Tr([html.Th("Title"), html.Th("Source"), html.Th("Year")])), html.Tbody(rows)],
            bordered=False,
            hover=True,
            responsive=True,
            size="sm",
            className="dash-table",
        )

    @app.callback(Output("live-ticker-chart", "figure"), Input("live-interval", "n_intervals"))
    def update_live_ticker(n_intervals):
        random.seed(n_intervals)
        points = list(range(max(0, n_intervals - 19), n_intervals + 1))
        values = [max(0, 30 + random.randint(-8, 16) + (point % 5) * 3) for point in points]
        figure = go.Figure(
            data=[
                go.Scatter(
                    x=points,
                    y=values,
                    mode="lines+markers",
                    line={"color": "#37d0c8", "width": 3},
                    marker={"size": 7},
                    fill="tozeroy",
                )
            ]
        )
        figure.update_xaxes(title="Interval tick")
        figure.update_yaxes(title="Simulated live mentions")
        return _style_figure(figure, "Live Apple Mention Ticker")

