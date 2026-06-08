"""Dash layout for the Apple social media brand monitoring dashboard."""

from __future__ import annotations

import dash_bootstrap_components as dbc
from dash import dcc, html

from src.dashboard.data_access import (
    get_document_type_options,
    get_source_options,
    get_year_bounds,
    load_mentions,
)


dashboard_df = load_mentions()
min_year, max_year = get_year_bounds(dashboard_df)


def _kpi_card(title: str, value_id: str, subtitle: str) -> dbc.Col:
    return dbc.Col(
        dbc.Card(
            dbc.CardBody(
                [
                    html.Div(title, className="kpi-label"),
                    html.H3(id=value_id, className="kpi-value"),
                    html.Div(subtitle, className="kpi-subtitle"),
                ]
            ),
            className="kpi-card",
        ),
        md=3,
        sm=6,
        xs=12,
    )


def create_layout() -> dbc.Container:
    """Create the full dashboard layout."""
    return dbc.Container(
        [
            dcc.Store(id="dashboard-data-version", data="apple-brand-monitor"),
            html.Div(
                [
                    html.Div(
                        [
                            html.P("Apple social media brand monitoring", className="eyebrow"),
                            html.H1("Apple Brand Intelligence Dashboard"),
                            html.P(
                                "Track mention volume, source mix, ratings, and live monitoring signals from the cleaned pipeline data.",
                                className="header-copy",
                            ),
                        ],
                        className="header-text",
                    ),
                    html.Div("Dash + MongoDB", className="header-badge"),
                ],
                className="dashboard-header",
            ),
            dbc.Row(
                [
                    _kpi_card("Total mentions", "kpi-total-mentions", "Filtered records"),
                    _kpi_card("Average rating", "kpi-average-rating", "Mean score"),
                    _kpi_card("Top source", "kpi-top-source", "Most frequent source"),
                    _kpi_card("Document types", "kpi-document-types", "Active formats"),
                ],
                className="g-3 mb-4",
            ),
            dbc.Card(
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Source", htmlFor="source-filter"),
                                    dcc.Dropdown(
                                        id="source-filter",
                                        options=get_source_options(dashboard_df),
                                        value="All",
                                        clearable=False,
                                    ),
                                ],
                                lg=3,
                                md=6,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Document type", htmlFor="document-type-filter"),
                                    dcc.Dropdown(
                                        id="document-type-filter",
                                        options=get_document_type_options(dashboard_df),
                                        value="All",
                                        clearable=False,
                                    ),
                                ],
                                lg=3,
                                md=6,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Mention year", htmlFor="year-range-filter"),
                                    dcc.RangeSlider(
                                        id="year-range-filter",
                                        min=min_year,
                                        max=max_year,
                                        value=[min_year, max_year],
                                        marks={year: str(year) for year in range(min_year, max_year + 1)},
                                        allowCross=False,
                                        tooltip={"placement": "bottom", "always_visible": False},
                                    ),
                                ],
                                lg=3,
                                md=6,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Search title/content", htmlFor="search-filter"),
                                    dcc.Input(
                                        id="search-filter",
                                        type="text",
                                        debounce=True,
                                        placeholder="Search Apple, Tim Cook, Maps...",
                                        className="form-control",
                                    ),
                                ],
                                lg=3,
                                md=6,
                            ),
                        ],
                        className="g-3 align-items-end",
                    )
                ),
                className="filter-card mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="source-volume-chart"), lg=6),
                    dbc.Col(dcc.Graph(id="rating-distribution-chart"), lg=6),
                ],
                className="g-4 mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="rating-source-scatter-chart"), lg=6),
                    dbc.Col(dcc.Graph(id="yearly-trend-chart"), lg=6),
                ],
                className="g-4 mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="live-ticker-chart"), lg=8),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H5("Recent matching mentions"),
                                    html.Div(id="recent-mentions-table"),
                                ]
                            ),
                            className="table-card",
                        ),
                        lg=4,
                    ),
                ],
                className="g-4",
            ),
            dcc.Interval(id="live-interval", interval=3000, n_intervals=0),
        ],
        fluid=True,
        className="dashboard-shell",
    )


CUSTOM_CSS = """
body {
    background: #101214;
    color: #f5f7fa;
}
.dashboard-shell {
    min-height: 100vh;
    padding: 28px;
}
.dashboard-header {
    display: flex;
    justify-content: space-between;
    gap: 24px;
    align-items: flex-start;
    margin-bottom: 24px;
}
.eyebrow {
    margin: 0 0 8px;
    color: #9fb3c8;
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0;
}
.dashboard-header h1 {
    margin: 0;
    font-size: clamp(2rem, 4vw, 3.3rem);
    font-weight: 760;
}
.header-copy {
    max-width: 760px;
    color: #c7d0da;
    margin: 12px 0 0;
}
.header-badge {
    border: 1px solid #35404c;
    border-radius: 8px;
    padding: 8px 12px;
    color: #d6edf3;
    background: #182028;
    white-space: nowrap;
}
.kpi-card,
.filter-card,
.table-card {
    background: #181c21;
    border: 1px solid #2a323b;
    border-radius: 8px;
}
.kpi-label,
.kpi-subtitle {
    color: #9aa8b6;
    font-size: 0.9rem;
}
.kpi-value {
    margin: 8px 0;
    color: #ffffff;
}
label {
    color: #c7d0da;
    margin-bottom: 6px;
}
.dash-table {
    color: #dce5ee;
}
"""

