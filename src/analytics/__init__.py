"""Analytics package for the brand-monitor data exploration lab."""

from src.analytics.data_loader import run_data_loader_pipeline
from src.analytics.explorer import run_explorer_pipeline
from src.analytics.numpy_ops import main as run_numpy_demo
from src.analytics.quality_report import run_quality_pipeline
from src.analytics.regex_ops import run_regex_pipeline
from src.analytics.selector import save_selector_examples, selector_examples_to_text

__all__ = [
    "run_data_loader_pipeline",
    "run_explorer_pipeline",
    "run_numpy_demo",
    "run_quality_pipeline",
    "run_regex_pipeline",
    "save_selector_examples",
    "selector_examples_to_text",
]
