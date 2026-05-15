"""Sentence-transformer helpers for Apple brand-monitor semantic search."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

from src.utils.logger import get_logger


logger = get_logger(__name__)

DEFAULT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


def _normalise_text(value: Any) -> str:
    """Convert values to clean text fragments."""
    if value is None:
        return ""
    if isinstance(value, float) and np.isnan(value):
        return ""
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


def build_brand_document(
    title: Any = None,
    overview: Any = None,
    keywords: Any = None,
    extra_fields: dict[str, Any] | None = None,
) -> str:
    """Combine Apple mention fields into one embedding document."""
    parts: list[str] = []

    cleaned_title = _normalise_text(title)
    cleaned_overview = _normalise_text(overview)
    cleaned_keywords = _normalise_text(keywords)

    if cleaned_title:
        parts.append(f"Title: {cleaned_title}")
    if cleaned_overview:
        parts.append(f"Overview: {cleaned_overview}")
    if cleaned_keywords:
        parts.append(f"Keywords: {cleaned_keywords}")

    if extra_fields:
        for key, value in extra_fields.items():
            cleaned_value = _normalise_text(value)
            if cleaned_value:
                parts.append(f"{key.replace('_', ' ').title()}: {cleaned_value}")

    return " | ".join(parts)


def build_documents_from_dataframe(
    df: pd.DataFrame,
    title_column: str = "title",
    overview_column: str = "overview",
    keywords_column: str = "primary_keyword",
    extra_columns: Sequence[str] | None = None,
) -> list[str]:
    """Create one semantic-search document per dataframe row."""
    if df.empty:
        return []

    documents: list[str] = []
    present_extra_columns = [column for column in (extra_columns or []) if column in df.columns]

    for _, row in df.iterrows():
        extra_fields = {column: row[column] for column in present_extra_columns}
        documents.append(
            build_brand_document(
                title=row.get(title_column),
                overview=row.get(overview_column),
                keywords=row.get(keywords_column) if keywords_column in df.columns else None,
                extra_fields=extra_fields,
            )
        )
    return documents


@dataclass
class BrandEmbedder:
    """Wrapper around a sentence-transformer model with lazy loading."""

    model_name: str = DEFAULT_MODEL_NAME
    normalize_embeddings: bool = True
    _model: Any = field(default=None, init=False, repr=False)

    def load_model(self) -> Any:
        """Load the transformer model on first use."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as exc:
                raise ImportError(
                    "sentence-transformers is required for Lab 11. Install dependencies from requirements.txt."
                ) from exc

            logger.info("Loading embedding model | model_name=%s", self.model_name)
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def encode(self, texts: str | Sequence[str], normalize_embeddings: bool | None = None) -> np.ndarray:
        """Encode one or many texts into embedding vectors."""
        model = self.load_model()
        should_normalize = self.normalize_embeddings if normalize_embeddings is None else normalize_embeddings
        single_text = isinstance(texts, str)
        payload = [texts] if single_text else list(texts)

        embeddings = model.encode(
            payload,
            convert_to_numpy=True,
            normalize_embeddings=should_normalize,
        )
        logger.info("Generated embeddings | texts=%s | dimensions=%s", len(payload), embeddings.shape[-1])
        return embeddings[0] if single_text else embeddings

    def encode_dataframe(
        self,
        df: pd.DataFrame,
        title_column: str = "title",
        overview_column: str = "overview",
        keywords_column: str = "primary_keyword",
        extra_columns: Sequence[str] | None = None,
        normalize_embeddings: bool | None = None,
    ) -> tuple[list[str], np.ndarray]:
        """Build documents from a dataframe and embed them."""
        documents = build_documents_from_dataframe(
            df,
            title_column=title_column,
            overview_column=overview_column,
            keywords_column=keywords_column,
            extra_columns=extra_columns,
        )
        embeddings = self.encode(documents, normalize_embeddings=normalize_embeddings)
        return documents, embeddings


MovieEmbedder = BrandEmbedder
build_movie_document = build_brand_document


_DEFAULT_EMBEDDER: BrandEmbedder | None = None


def get_embedder(
    model_name: str = DEFAULT_MODEL_NAME,
    normalize_embeddings: bool = True,
) -> BrandEmbedder:
    """Return a shared embedder instance."""
    global _DEFAULT_EMBEDDER
    if _DEFAULT_EMBEDDER is None or _DEFAULT_EMBEDDER.model_name != model_name:
        _DEFAULT_EMBEDDER = BrandEmbedder(
            model_name=model_name,
            normalize_embeddings=normalize_embeddings,
        )
    return _DEFAULT_EMBEDDER


def _ensure_2d(array: np.ndarray) -> np.ndarray:
    """Force vectors to matrix shape."""
    matrix = np.asarray(array, dtype=float)
    if matrix.ndim == 1:
        return matrix.reshape(1, -1)
    return matrix


def cosine_similarity_scores(query_embedding: np.ndarray, candidate_embeddings: np.ndarray) -> np.ndarray:
    """Compute cosine similarity between one query vector and many candidates."""
    query = _ensure_2d(query_embedding)
    candidates = _ensure_2d(candidate_embeddings)

    query_norms = np.linalg.norm(query, axis=1, keepdims=True)
    candidate_norms = np.linalg.norm(candidates, axis=1, keepdims=True).T
    similarities = (query @ candidates.T) / np.clip(query_norms * candidate_norms, a_min=1e-12, a_max=None)
    return similarities[0]


def cosine_similarity_matrix(embeddings: np.ndarray) -> np.ndarray:
    """Compute the full pairwise cosine-similarity matrix."""
    matrix = _ensure_2d(embeddings)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    normalised = matrix / np.clip(norms, a_min=1e-12, a_max=None)
    return normalised @ normalised.T


def dot_product_scores(query_embedding: np.ndarray, candidate_embeddings: np.ndarray) -> np.ndarray:
    """Compute dot products between one query vector and many candidates."""
    query = _ensure_2d(query_embedding)
    candidates = _ensure_2d(candidate_embeddings)
    return (query @ candidates.T)[0]


def euclidean_distances(query_embedding: np.ndarray, candidate_embeddings: np.ndarray) -> np.ndarray:
    """Compute Euclidean distances between one query vector and many candidates."""
    query = _ensure_2d(query_embedding)
    candidates = _ensure_2d(candidate_embeddings)
    return np.linalg.norm(candidates - query[0], axis=1)


def rank_texts_by_similarity(
    query_text: str,
    candidate_texts: Sequence[str],
    embedder: BrandEmbedder | None = None,
) -> dict[str, pd.DataFrame]:
    """Rank candidate texts against a query using three common similarity measures."""
    active_embedder = embedder or get_embedder()
    query_embedding = active_embedder.encode(query_text)
    candidate_embeddings = active_embedder.encode(candidate_texts)

    cosine_scores = cosine_similarity_scores(query_embedding, candidate_embeddings)
    dot_scores = dot_product_scores(query_embedding, candidate_embeddings)
    euclidean_scores = euclidean_distances(query_embedding, candidate_embeddings)

    def _build_result_frame(score_values: Iterable[float], score_name: str, ascending: bool) -> pd.DataFrame:
        result = pd.DataFrame({"text": list(candidate_texts), score_name: list(score_values)})
        return result.sort_values(score_name, ascending=ascending).reset_index(drop=True)

    return {
        "cosine": _build_result_frame(cosine_scores, "cosine_similarity", ascending=False),
        "dot_product": _build_result_frame(dot_scores, "dot_product", ascending=False),
        "euclidean": _build_result_frame(euclidean_scores, "euclidean_distance", ascending=True),
    }
