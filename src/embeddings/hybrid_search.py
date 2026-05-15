"""Hybrid search ranking helpers."""

from __future__ import annotations

from typing import Hashable, Sequence

import pandas as pd


def reciprocal_rank_fusion(
    rankings: Sequence[pd.DataFrame],
    id_column: str = "_id",
    score_column: str = "rrf_score",
    k: int = 60,
) -> pd.DataFrame:
    """Combine ranked result lists using Reciprocal Rank Fusion."""
    fused_scores: dict[Hashable, float] = {}
    exemplar_rows: dict[Hashable, dict] = {}

    for ranking in rankings:
        if ranking is None or ranking.empty or id_column not in ranking.columns:
            continue

        ordered = ranking.reset_index(drop=True)
        for rank_index, row in ordered.iterrows():
            item_id = row[id_column]
            fused_scores[item_id] = fused_scores.get(item_id, 0.0) + (1.0 / (k + rank_index + 1))
            exemplar_rows[item_id] = row.to_dict()

    if not fused_scores:
        return pd.DataFrame()

    fused_rows = []
    for item_id, fused_score in fused_scores.items():
        row = exemplar_rows[item_id].copy()
        row[score_column] = fused_score
        fused_rows.append(row)

    return pd.DataFrame(fused_rows).sort_values(score_column, ascending=False).reset_index(drop=True)
