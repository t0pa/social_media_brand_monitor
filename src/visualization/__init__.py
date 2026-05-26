"""Visualization package exports for the Apple brand-monitor project."""

from src.visualization.chart_generator import (
    generate_all_visualizations,
    load_visualization_dataset,
)
from src.visualization.interactive_charts import (
    INTERACTIVE_CHART_FUNCTIONS,
    interactive_mentions_timeline,
    interactive_multi_layout_dashboard,
    interactive_rating_by_document_type,
    interactive_title_length_scatter,
    interactive_top_sources,
)
from src.visualization.static_charts import (
    STATIC_CHART_FUNCTIONS,
    plot_dashboard_subplots,
    plot_language_year_heatmap,
    plot_mentions_and_rating_by_year,
    plot_mentions_by_document_type,
    plot_monthly_mentions_line,
    plot_rating_by_source_boxplot,
    plot_rating_distribution,
    plot_title_length_vs_rating,
    plot_top_sources_barh,
)

__all__ = [
    "generate_all_visualizations",
    "load_visualization_dataset",
    "STATIC_CHART_FUNCTIONS",
    "INTERACTIVE_CHART_FUNCTIONS",
    "plot_top_sources_barh",
    "plot_mentions_and_rating_by_year",
    "plot_mentions_by_document_type",
    "plot_rating_distribution",
    "plot_rating_by_source_boxplot",
    "plot_title_length_vs_rating",
    "plot_language_year_heatmap",
    "plot_monthly_mentions_line",
    "plot_dashboard_subplots",
    "interactive_top_sources",
    "interactive_mentions_timeline",
    "interactive_rating_by_document_type",
    "interactive_title_length_scatter",
    "interactive_multi_layout_dashboard",
]

