"""Analytics package for the brand-monitor data exploration lab."""

from src.analytics.analytics_pipeline import run_analytics_pipeline
from src.analytics.data_combiner import compare_join_types, merge_on_key
from src.analytics.data_loader import run_data_loader_pipeline
from src.analytics.db_connector import populate_article_metrics, query_article_metrics
from src.analytics.explorer import run_explorer_pipeline
from src.analytics.insight_reporter import run_all_questions
from src.analytics.mongo_pipeline import build_source_mentions_pipeline
from src.analytics.numpy_ops import main as run_numpy_demo
from src.analytics.pivot_builder import add_primary_keyword, build_keyword_year_pivot
from src.analytics.quality_report import run_quality_pipeline
from src.analytics.regex_ops import run_regex_pipeline
from src.analytics.selector import save_selector_examples, selector_examples_to_text
from src.analytics.time_series import add_rolling_averages, build_monthly_mentions

__all__ = [
    "add_primary_keyword",
    "add_rolling_averages",
    "build_keyword_year_pivot",
    "build_monthly_mentions",
    "build_source_mentions_pipeline",
    "compare_join_types",
    "merge_on_key",
    "populate_article_metrics",
    "query_article_metrics",
    "run_data_loader_pipeline",
    "run_explorer_pipeline",
    "run_all_questions",
    "run_analytics_pipeline",
    "run_numpy_demo",
    "run_quality_pipeline",
    "run_regex_pipeline",
    "save_selector_examples",
    "selector_examples_to_text",
]
