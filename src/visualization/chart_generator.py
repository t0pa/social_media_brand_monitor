"""Visualization orchestrator for static and interactive chart generation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.logger import get_logger
from src.visualization.interactive_charts import INTERACTIVE_CHART_FUNCTIONS
from src.visualization.static_charts import STATIC_CHART_FUNCTIONS


logger = get_logger(__name__)

DEFAULT_DATA_PATH = Path("data/processed/cleaned/cleaned_data.csv")
DEFAULT_OUTPUT_ROOT = Path("outputs/visualizations")
DEFAULT_STATIC_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "static"
DEFAULT_INTERACTIVE_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "interactive"


def load_visualization_dataset(data_path: str | Path = DEFAULT_DATA_PATH) -> pd.DataFrame:
    """Load the cleaned dataset used for visualization generation."""
    dataset_path = Path(data_path)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Visualization dataset not found: {dataset_path}")

    dataframe = pd.read_csv(dataset_path)
    logger.info(
        "Visualization dataset loaded | path=%s | rows=%s | columns=%s",
        dataset_path,
        len(dataframe),
        len(dataframe.columns),
    )
    return dataframe


def _run_chart_functions(
    dataframe: pd.DataFrame,
    chart_functions: list,
    output_dir: Path,
    chart_group: str,
) -> list[dict[str, object]]:
    """Run a sequence of chart functions and collect their outputs."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []

    for chart_function in chart_functions:
        logger.info("Generating %s chart | function=%s | output_dir=%s", chart_group, chart_function.__name__, output_dir)
        result = chart_function(dataframe, output_dir)
        results.append(
            {
                "name": chart_function.__name__,
                "output_dir": output_dir,
                "result": result,
            }
        )

    return results


def generate_all_visualizations(
    data_path: str | Path = DEFAULT_DATA_PATH,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, object]:
    """Generate every static and interactive chart from the cleaned dataset."""
    dataframe = load_visualization_dataset(data_path)
    output_root_path = Path(output_root)
    static_output_dir = output_root_path / "static"
    interactive_output_dir = output_root_path / "interactive"

    static_results = _run_chart_functions(
        dataframe=dataframe,
        chart_functions=STATIC_CHART_FUNCTIONS,
        output_dir=static_output_dir,
        chart_group="static",
    )
    interactive_results = _run_chart_functions(
        dataframe=dataframe,
        chart_functions=INTERACTIVE_CHART_FUNCTIONS,
        output_dir=interactive_output_dir,
        chart_group="interactive",
    )

    logger.info(
        "Visualization generation complete | static=%s | interactive=%s | output_root=%s",
        len(static_results),
        len(interactive_results),
        output_root_path,
    )
    return {
        "data_path": Path(data_path),
        "output_root": output_root_path,
        "static_output_dir": static_output_dir,
        "interactive_output_dir": interactive_output_dir,
        "static_results": static_results,
        "interactive_results": interactive_results,
    }

