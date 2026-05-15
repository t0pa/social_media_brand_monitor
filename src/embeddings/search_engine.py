"""High-level keyword, semantic, and hybrid search helpers."""

from __future__ import annotations

import re
from typing import Any, Sequence

import pandas as pd

from src.embeddings.chroma_store import ChromaBrandStore, prepare_embedding_dataframe
from src.embeddings.embedder import build_documents_from_dataframe, cosine_similarity_scores, get_embedder
from src.embeddings.hybrid_search import reciprocal_rank_fusion


DEFAULT_TEXT_COLUMNS = ["title", "overview", "description", "content"]


def _prepare_search_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare a dataframe for keyword and local semantic search."""
    prepared = prepare_embedding_dataframe(df)
    if "document" not in prepared.columns:
        prepared["document"] = build_documents_from_dataframe(
            prepared,
            title_column="title",
            overview_column="overview",
            keywords_column="primary_keyword",
            extra_columns=["description", "content", "source", "document_type", "language"],
        )
    return prepared


def _normalise_text_columns(df: pd.DataFrame, text_columns: Sequence[str] | None = None) -> list[str]:
    """Resolve the requested text columns to columns that actually exist."""
    return [column for column in (text_columns or DEFAULT_TEXT_COLUMNS) if column in df.columns]


def _enrich_with_source_rows(
    results_df: pd.DataFrame,
    source_df: pd.DataFrame,
    id_column: str = "_id",
) -> pd.DataFrame:
    """Merge search results back with the source dataframe for readable output."""
    if results_df.empty or id_column not in results_df.columns or id_column not in source_df.columns:
        return results_df

    source_columns = [
        column
        for column in source_df.columns
        if column != id_column and column not in results_df.columns
    ]
    if not source_columns:
        return results_df

    merged = results_df.merge(
        source_df[[id_column] + source_columns].drop_duplicates(subset=[id_column]),
        on=id_column,
        how="left",
    )
    return merged


def semantic_search(
    query: str,
    store: ChromaBrandStore,
    n_results: int = 5,
    where: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Search Apple brand-monitor documents by natural-language meaning."""
    results = store.query_to_dataframe(query_text=query, n_results=n_results, where=where)
    if not results.empty:
        results["semantic_rank"] = results["rank"]
        if "distance" in results.columns:
            results["semantic_score"] = 1.0 - results["distance"]
    return results


def keyword_search(
    query: str,
    df: pd.DataFrame,
    text_columns: Sequence[str] | None = None,
    n_results: int = 5,
    id_column: str = "_id",
) -> pd.DataFrame:
    """Perform exact-token keyword search across selected text columns."""
    if df.empty:
        return pd.DataFrame()

    prepared = _prepare_search_dataframe(df)
    active_columns = _normalise_text_columns(prepared, text_columns=text_columns)
    if not active_columns:
        return pd.DataFrame()

    keywords = [token for token in re.findall(r"\w+", query.lower()) if token]
    if not keywords:
        return pd.DataFrame()

    working = prepared.copy()
    matched_columns: list[list[str]] = []
    exact_match_counts: list[int] = []

    for _, row in working.iterrows():
        row_matches: list[str] = []
        row_score = 0
        for column in active_columns:
            text = str(row.get(column, "") or "").lower()
            column_matched = False
            for keyword in keywords:
                if re.search(rf"\b{re.escape(keyword)}\b", text):
                    row_score += 1
                    column_matched = True
            if column_matched:
                row_matches.append(column)
        matched_columns.append(row_matches)
        exact_match_counts.append(row_score)

    working["keyword_score"] = exact_match_counts
    working["matched_columns"] = [", ".join(columns) for columns in matched_columns]
    matches = working[working["keyword_score"] > 0].copy()
    matches = matches.sort_values(
        by=["keyword_score", "mention_year", id_column] if "mention_year" in matches.columns else ["keyword_score", id_column],
        ascending=[False, False, True] if "mention_year" in matches.columns else [False, True],
    ).head(n_results).reset_index(drop=True)
    matches["keyword_rank"] = range(1, len(matches) + 1)
    return matches


def semantic_search_from_dataframe(
    query: str,
    df: pd.DataFrame,
    document_column: str = "document",
    n_results: int = 5,
) -> pd.DataFrame:
    """Lightweight semantic search without ChromaDB, useful for quick demos."""
    if df.empty:
        return pd.DataFrame()

    prepared = _prepare_search_dataframe(df)
    if document_column not in prepared.columns:
        return pd.DataFrame()

    embedder = get_embedder()
    query_embedding = embedder.encode(query)
    document_embeddings = embedder.encode(prepared[document_column].fillna("").astype(str).tolist())
    scores = cosine_similarity_scores(query_embedding, document_embeddings)

    result = prepared.copy()
    result["semantic_score"] = scores
    result = result.sort_values("semantic_score", ascending=False).head(n_results).reset_index(drop=True)
    result["semantic_rank"] = range(1, len(result) + 1)
    return result


def hybrid_search(
    query: str,
    df: pd.DataFrame,
    store: ChromaBrandStore | None = None,
    text_columns: Sequence[str] | None = None,
    n_results: int = 5,
    where: dict[str, Any] | None = None,
    id_column: str = "_id",
    k: int = 60,
) -> pd.DataFrame:
    """Combine keyword and semantic search using Reciprocal Rank Fusion."""
    prepared = _prepare_search_dataframe(df)
    keyword_results = keyword_search(
        query,
        prepared,
        text_columns=text_columns,
        n_results=n_results * 2,
        id_column=id_column,
    )

    if store is not None:
        semantic_results = semantic_search(query, store=store, n_results=n_results * 2, where=where)
        semantic_results = _enrich_with_source_rows(semantic_results, prepared, id_column=id_column)
    else:
        semantic_results = semantic_search_from_dataframe(query, prepared, document_column="document", n_results=n_results * 2)

    fused = reciprocal_rank_fusion(
        [keyword_results, semantic_results],
        id_column=id_column,
        score_column="rrf_score",
        k=k,
    )
    fused = _enrich_with_source_rows(fused, prepared, id_column=id_column)
    return fused.head(n_results).reset_index(drop=True)


def compare_search(
    query: str,
    df: pd.DataFrame,
    store: ChromaBrandStore | None = None,
    text_columns: Sequence[str] | None = None,
    n_results: int = 5,
    where: dict[str, Any] | None = None,
) -> dict[str, pd.DataFrame]:
    """Return keyword, semantic, and hybrid search results for one query."""
    prepared = _prepare_search_dataframe(df)
    keyword_results = keyword_search(query, prepared, text_columns=text_columns, n_results=n_results)
    if store is not None:
        semantic_results = semantic_search(query, store=store, n_results=n_results, where=where)
        semantic_results = _enrich_with_source_rows(semantic_results, prepared)
    else:
        semantic_results = semantic_search_from_dataframe(query, prepared, document_column="document", n_results=n_results)
    hybrid_results = hybrid_search(
        query,
        df=prepared,
        store=store,
        text_columns=text_columns,
        n_results=n_results,
        where=where,
    )
    return {
        "keyword": keyword_results,
        "semantic": semantic_results,
        "hybrid": hybrid_results,
    }


def compare_search_side_by_side(
    query: str,
    df: pd.DataFrame,
    store: ChromaBrandStore | None = None,
    text_columns: Sequence[str] | None = None,
    n_results: int = 5,
    where: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Return a notebook-friendly side-by-side comparison table."""
    comparisons = compare_search(
        query=query,
        df=df,
        store=store,
        text_columns=text_columns,
        n_results=n_results,
        where=where,
    )

    frames: list[pd.DataFrame] = []
    for method_name, result_df in comparisons.items():
        if result_df.empty:
            continue
        frame = result_df.copy().head(n_results)
        frame.insert(0, "search_method", method_name)
        frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def calculate_result_overlap(
    first_results: pd.DataFrame,
    second_results: pd.DataFrame,
    id_column: str = "_id",
) -> dict[str, Any]:
    """Measure overlap between two ranked result sets."""
    first_ids = set(first_results.get(id_column, pd.Series(dtype=str)).astype(str))
    second_ids = set(second_results.get(id_column, pd.Series(dtype=str)).astype(str))
    overlap_ids = first_ids & second_ids
    union_ids = first_ids | second_ids

    overlap_count = len(overlap_ids)
    overlap_ratio = overlap_count / len(union_ids) if union_ids else 0.0
    return {
        "overlap_count": overlap_count,
        "overlap_ratio": overlap_ratio,
        "overlap_ids": sorted(overlap_ids),
    }


def compare_synonym_query_pairs(
    query_pairs: Sequence[tuple[str, str]],
    df: pd.DataFrame,
    store: ChromaBrandStore,
    text_columns: Sequence[str] | None = None,
    n_results: int = 5,
) -> pd.DataFrame:
    """Compare synonym-style query pairs for keyword and semantic consistency."""
    prepared = _prepare_search_dataframe(df)
    rows: list[dict[str, Any]] = []

    for query_a, query_b in query_pairs:
        keyword_a = keyword_search(query_a, prepared, text_columns=text_columns, n_results=n_results)
        keyword_b = keyword_search(query_b, prepared, text_columns=text_columns, n_results=n_results)
        semantic_a = semantic_search(query_a, store=store, n_results=n_results)
        semantic_b = semantic_search(query_b, store=store, n_results=n_results)

        keyword_overlap = calculate_result_overlap(keyword_a, keyword_b)
        semantic_overlap = calculate_result_overlap(semantic_a, semantic_b)

        rows.append(
            {
                "query_a": query_a,
                "query_b": query_b,
                "keyword_overlap_count": keyword_overlap["overlap_count"],
                "keyword_overlap_ratio": keyword_overlap["overlap_ratio"],
                "semantic_overlap_count": semantic_overlap["overlap_count"],
                "semantic_overlap_ratio": semantic_overlap["overlap_ratio"],
            }
        )

    return pd.DataFrame(rows)
