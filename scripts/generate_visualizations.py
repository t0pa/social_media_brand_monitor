"""CLI entry-point for generating all static and interactive visualizations."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.visualization.chart_generator import generate_all_visualizations


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Apple brand-monitor visualizations from a cleaned CSV dataset.",
    )
    parser.add_argument(
        "--data",
        default=str(PROJECT_ROOT / "data" / "processed" / "cleaned" / "cleaned_data.csv"),
        help="Path to the cleaned CSV file used to generate the charts.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "outputs" / "visualizations"),
        help="Root directory where visualization outputs will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    results = generate_all_visualizations(data_path=args.data, output_root=args.output)

    print(f"Visualization dataset: {results['data_path']}")
    print(f"Static charts directory: {results['static_output_dir']}")
    print(f"Interactive charts directory: {results['interactive_output_dir']}")
    print(f"Static charts generated: {len(results['static_results'])}")
    print(f"Interactive charts generated: {len(results['interactive_results'])}")


if __name__ == "__main__":
    main()

