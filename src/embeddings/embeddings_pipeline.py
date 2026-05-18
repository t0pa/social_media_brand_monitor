"""Pipeline entrypoint for Apple semantic-search embeddings."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.embeddings.chroma_store import ChromaBrandStore, prepare_embedding_dataframe
from src.embeddings.search_engine import compare_search_side_by_side
from src.utils.logger import get_logger


logger = get_logger(__name__)

OUTPUT_DIR = Path("data/processed/embeddings")
DEFAULT_DEMO_QUERY = "Apple leadership and product strategy"


def run_embeddings_pipeline(
    cleaned_csv_path: str | Path,
    reset_collection: bool = False,
    demo_query: str = DEFAULT_DEMO_QUERY,
) -> dict[str, object]:
    """Build the Chroma collection from the cleaned Apple dataset and run a demo search."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cleaned_df = pd.read_csv(cleaned_csv_path)
    prepared_df = prepare_embedding_dataframe(cleaned_df)

    store = ChromaBrandStore()
    store.get_or_create_collection(reset=reset_collection)
    inserted_count = store.add_documents(prepared_df)

    comparison_df = compare_search_side_by_side(
        query=demo_query,
        df=prepared_df,
        store=store,
        n_results=5,
    )
    comparison_path = OUTPUT_DIR / "search_comparison_demo.csv"
    comparison_df.to_csv(comparison_path, index=False, encoding="utf-8")

    logger.info(
        "Embeddings pipeline finished | rows=%s | inserted=%s | collection_count=%s | comparison_output=%s",
        len(prepared_df),
        inserted_count,
        store.count(),
        comparison_path,
    )
    return {
        "prepared_df": prepared_df,
        "store": store,
        "inserted_count": inserted_count,
        "collection_count": store.count(),
        "demo_query": demo_query,
        "comparison_df": comparison_df,
        "comparison_path": comparison_path,
    }
