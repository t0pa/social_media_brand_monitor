import os
import sys

import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.utils.logger import get_logger


logger = get_logger(__name__)


def describe_array(name: str, array: np.ndarray) -> None:
    """Print the array and its core NumPy properties for the lab report."""
    logger.info(
        "Array created: %s | shape=%s | dtype=%s | ndim=%s",
        name,
        array.shape,
        array.dtype,
        array.ndim,
    )
    print(f"\n{name}")
    print(array)
    print(f"shape: {array.shape}")
    print(f"dtype: {array.dtype}")
    print(f"ndim: {array.ndim}")


def describe_operation(name: str, result: np.ndarray) -> None:
    """Print vectorized arithmetic results for the lab requirement."""
    logger.info("Vectorized operation: %s | result=%s", name, result)
    print(f"\n{name}")
    print(result)


def create_brand_monitor_arrays() -> dict[str, np.ndarray]:
    """Create Apple-themed arrays with several different NumPy constructors."""
    return {
        "1. np.array() from daily Apple mention counts": np.array([120, 135, 128, 142, 150]),
        "2. np.arange() for hourly monitoring windows": np.arange(0, 24, 4),
        "3. np.linspace() for engagement score scale": np.linspace(0.0, 1.0, 5),
        "4. np.zeros() for unresolved complaint counts": np.zeros((2, 3), dtype=int),
        "5. np.ones() for campaign baseline matrix": np.ones((3, 2), dtype=float),
    }


def run_vectorized_arithmetic() -> None:
    """Show NumPy arithmetic without Python loops."""
    daily_mentions = np.array([120, 135, 128, 142, 150])
    positive_sentiment = np.array([82, 90, 85, 95, 102])

    mention_growth = daily_mentions + 15
    doubled_sentiment = positive_sentiment * 2
    total_attention = daily_mentions + positive_sentiment
    engagement_ratio = positive_sentiment / daily_mentions

    print("\nVectorized arithmetic with no Python loops")
    describe_operation("Original daily mentions", daily_mentions)
    describe_operation("Original positive sentiment counts", positive_sentiment)
    describe_operation("Add 15 mentions to every day", mention_growth)
    describe_operation("Double sentiment counts", doubled_sentiment)
    describe_operation("Add two arrays element-wise", total_attention)
    describe_operation("Divide arrays element-wise", engagement_ratio)


def main() -> None:
    logger.info("NumPy analytics demo started for Apple brand monitor assignment")
    arrays = create_brand_monitor_arrays()

    print("NumPy array creation methods for the Apple brand monitor project")
    for name, array in arrays.items():
        describe_array(name, array)

    run_vectorized_arithmetic()
    logger.info("NumPy analytics demo finished successfully")


if __name__ == "__main__":
    main()
