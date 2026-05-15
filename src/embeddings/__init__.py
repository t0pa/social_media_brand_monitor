"""Embedding and vector-search utilities for Lab 11."""

from src.embeddings.chroma_store import (
    ChromaBrandStore,
    ChromaMovieStore,
    build_filter_examples,
    prepare_embedding_dataframe,
)
from src.embeddings.embedder import (
    BrandEmbedder,
    build_brand_document,
    MovieEmbedder,
    build_movie_document,
    cosine_similarity_matrix,
    cosine_similarity_scores,
    dot_product_scores,
    euclidean_distances,
    get_embedder,
    rank_texts_by_similarity,
)
from src.embeddings.embeddings_pipeline import run_embeddings_pipeline
from src.embeddings.hybrid_search import reciprocal_rank_fusion
from src.embeddings.search_engine import (
    calculate_result_overlap,
    compare_search,
    compare_search_side_by_side,
    compare_synonym_query_pairs,
    hybrid_search,
    keyword_search,
    semantic_search,
)

__all__ = [
    "BrandEmbedder",
    "build_filter_examples",
    "ChromaBrandStore",
    "ChromaMovieStore",
    "build_brand_document",
    "MovieEmbedder",
    "build_movie_document",
    "calculate_result_overlap",
    "compare_search",
    "compare_search_side_by_side",
    "compare_synonym_query_pairs",
    "cosine_similarity_matrix",
    "cosine_similarity_scores",
    "dot_product_scores",
    "euclidean_distances",
    "get_embedder",
    "hybrid_search",
    "keyword_search",
    "prepare_embedding_dataframe",
    "rank_texts_by_similarity",
    "reciprocal_rank_fusion",
    "run_embeddings_pipeline",
    "semantic_search",
]
