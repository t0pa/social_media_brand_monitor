"""Persistent ChromaDB storage helpers for Apple brand-monitor semantic search."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from src.embeddings.embedder import BrandEmbedder, build_documents_from_dataframe, get_embedder
from src.utils.logger import get_logger


logger = get_logger(__name__)

DEFAULT_CHROMA_PATH = Path("data/embeddings/chroma_db")
DEFAULT_COLLECTION_NAME = "data"
DEFAULT_METADATA_COLUMNS = [
    "title",
    "source",
    "author",
    "document_type",
    "type",
    "language",
    "rating",
    "mention_year",
    "mention_month",
    "mention_weekday",
    "mention_quarter",
    "primary_keyword",
]
DEFAULT_EXTRA_TEXT_COLUMNS = [
    "source",
    "author",
    "description",
    "content",
    "document_type",
    "language",
    "primary_keyword",
]


def _normalise_metadata_value(value: Any) -> Any:
    """Convert pandas values into Chroma-friendly metadata scalars."""
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return value


def prepare_embedding_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Create a Chroma-ready dataframe from the cleaned Apple dataset."""
    prepared = df.copy()

    if "primary_keyword" not in prepared.columns:
        text_source = prepared.get("overview", prepared.get("title", "")).fillna("").astype(str)
        prepared["primary_keyword"] = text_source.str.extract(r"\b(iphone|ipad|mac|watch|airpods|vision|tim cook|ios)\b", expand=False)
        prepared["primary_keyword"] = prepared["primary_keyword"].fillna("apple")

    if "language" in prepared.columns:
        prepared["language"] = prepared["language"].replace("", pd.NA).fillna("unknown")
    else:
        prepared["language"] = "unknown"

    if "mention_year" in prepared.columns:
        prepared["mention_year"] = pd.to_numeric(prepared["mention_year"], errors="coerce").astype("Int64")
    if "rating" in prepared.columns:
        prepared["rating"] = pd.to_numeric(prepared["rating"], errors="coerce")

    for column in ["title", "overview", "description", "content", "source", "author", "document_type"]:
        if column in prepared.columns:
            prepared[column] = prepared[column].fillna("Unknown")

    return prepared


def build_filter_examples() -> dict[str, dict[str, Any]]:
    """Return ready-to-use Chroma metadata filter examples for the Apple dataset."""
    return {
        "eq_language_unknown": {"language": {"$eq": "unknown"}},
        "gte_2025": {"mention_year": {"$gte": 2025}},
        "in_document_types": {"document_type": {"$in": ["json", "csv", "web_scrape"]}},
        "and_keyword_year": {
            "$and": [
                {"primary_keyword": {"$eq": "apple"}},
                {"mention_year": {"$gte": 2025}},
            ]
        },
        "or_language_source": {
            "$or": [
                {"language": {"$eq": "unknown"}},
                {"source": {"$eq": "sample.csv"}},
            ]
        },
    }


class ChromaBrandStore:
    """Small wrapper around a persistent ChromaDB collection."""

    def __init__(
        self,
        persist_directory: str | Path = DEFAULT_CHROMA_PATH,
        collection_name: str = DEFAULT_COLLECTION_NAME,
        embedder: BrandEmbedder | None = None,
    ) -> None:
        self.persist_directory = Path(persist_directory)
        self.collection_name = collection_name
        self.embedder = embedder or get_embedder()
        self._client = None
        self._collection = None

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                import chromadb
            except ImportError as exc:
                raise ImportError("chromadb is required for Lab 11. Install dependencies from requirements.txt.") from exc

            self.persist_directory.mkdir(parents=True, exist_ok=True)
            self._client = chromadb.PersistentClient(path=str(self.persist_directory))
            logger.info("Connected to ChromaDB | path=%s", self.persist_directory)
        return self._client

    def get_or_create_collection(self, reset: bool = False) -> Any:
        """Create or reuse the configured collection."""
        client = self._get_client()
        if reset:
            try:
                client.delete_collection(self.collection_name)
                logger.info("Deleted existing ChromaDB collection | name=%s", self.collection_name)
            except Exception:
                logger.info("ChromaDB collection reset requested but collection did not yet exist | name=%s", self.collection_name)

        if self._collection is None or reset:
            self._collection = client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"},
            )
            logger.info("Ready ChromaDB collection | name=%s", self.collection_name)
        return self._collection

    def count(self) -> int:
        """Return the number of stored vectors."""
        collection = self.get_or_create_collection()
        return collection.count()

    def add_documents(
        self,
        df: pd.DataFrame,
        id_column: str = "_id",
        title_column: str = "title",
        overview_column: str = "overview",
        keywords_column: str = "primary_keyword",
        metadata_columns: Sequence[str] | None = None,
        extra_text_columns: Sequence[str] | None = None,
        batch_size: int = 128,
    ) -> int:
        """Upsert Apple mention documents into ChromaDB with embeddings and metadata."""
        if df.empty:
            logger.warning("Skipped ChromaDB upsert because dataframe was empty.")
            return 0

        prepared_df = prepare_embedding_dataframe(df)
        collection = self.get_or_create_collection()
        metadata_columns = [column for column in (metadata_columns or DEFAULT_METADATA_COLUMNS) if column in prepared_df.columns]
        extra_text_columns = [column for column in (extra_text_columns or DEFAULT_EXTRA_TEXT_COLUMNS) if column in prepared_df.columns]

        documents = build_documents_from_dataframe(
            prepared_df,
            title_column=title_column,
            overview_column=overview_column,
            keywords_column=keywords_column,
            extra_columns=extra_text_columns,
        )
        embeddings = self.embedder.encode(documents).tolist()
        ids = prepared_df[id_column].astype(str).tolist()

        metadatas: list[dict[str, Any]] = []
        for _, row in prepared_df.iterrows():
            metadata: dict[str, Any] = {}
            for column in metadata_columns:
                value = _normalise_metadata_value(row[column])
                if value is not None:
                    metadata[column] = value
            metadatas.append(metadata)

        inserted = 0
        for start in range(0, len(prepared_df), batch_size):
            end = start + batch_size
            collection.upsert(
                ids=ids[start:end],
                documents=documents[start:end],
                embeddings=embeddings[start:end],
                metadatas=metadatas[start:end],
            )
            inserted += len(ids[start:end])

        logger.info("Upserted documents into ChromaDB | rows=%s | collection=%s", inserted, self.collection_name)
        return inserted

    def query(
        self,
        query_texts: str | Sequence[str],
        n_results: int = 5,
        where: dict[str, Any] | None = None,
        include: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Query ChromaDB by semantic similarity."""
        collection = self.get_or_create_collection()
        payload = [query_texts] if isinstance(query_texts, str) else list(query_texts)
        query_embeddings = self.embedder.encode(payload).tolist()
        active_include = list(include or ["documents", "metadatas", "distances"])
        results = collection.query(
            query_embeddings=query_embeddings,
            n_results=n_results,
            where=where,
            include=active_include,
        )
        logger.info("Queried ChromaDB | queries=%s | n_results=%s", len(payload), n_results)
        return results

    def query_to_dataframe(
        self,
        query_text: str,
        n_results: int = 5,
        where: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Run one semantic query and format the Chroma response as a dataframe."""
        results = self.query(query_texts=query_text, n_results=n_results, where=where)
        return self._results_to_dataframe(results, query_texts=[query_text])

    def multi_query_to_dataframe(
        self,
        query_texts: Sequence[str],
        n_results: int = 5,
        where: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Run multiple semantic queries in one Chroma API call and flatten the results."""
        results = self.query(query_texts=query_texts, n_results=n_results, where=where)
        return self._results_to_dataframe(results, query_texts=query_texts)

    @staticmethod
    def _results_to_dataframe(results: dict[str, Any], query_texts: Sequence[str]) -> pd.DataFrame:
        """Flatten Chroma query results into a single dataframe."""
        rows: list[dict[str, Any]] = []

        ids_batches = results.get("ids", [])
        document_batches = results.get("documents", [])
        metadata_batches = results.get("metadatas", [])
        distance_batches = results.get("distances", [])

        for query_index, query_text in enumerate(query_texts):
            ids = ids_batches[query_index] if query_index < len(ids_batches) else []
            documents = document_batches[query_index] if query_index < len(document_batches) else []
            metadatas = metadata_batches[query_index] if query_index < len(metadata_batches) else []
            distances = distance_batches[query_index] if query_index < len(distance_batches) else []

            for rank_index, (item_id, document, metadata, distance) in enumerate(zip(ids, documents, metadatas, distances), start=1):
                row = {
                    "query_text": query_text,
                    "rank": rank_index,
                    "_id": item_id,
                    "document": document,
                    "distance": distance,
                }
                row.update(metadata or {})
                rows.append(row)

        return pd.DataFrame(rows)


ChromaMovieStore = ChromaBrandStore
