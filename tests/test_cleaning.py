import pandas as pd
import pytest

from src.analytics.regex_ops import (
    detect_invalid_date_formats,
    detect_invalid_language_codes,
    extract_numeric_values_from_text,
    flag_short_overviews,
)
from src.cleaning.clean_pipeline import run_cleaning_pipeline
from src.cleaning.deduplicator import (
    count_duplicates,
    drop_duplicate_ids,
    drop_duplicate_title_date_pairs,
    remove_exact_duplicates,
)
from src.cleaning.missing_handler import (
    drop_high_missing_columns,
    fill_numeric_medians,
    handle_missing_values,
    replace_unrealistic_zeros,
)
from src.cleaning.string_cleaner import clean_brand_strings
from src.cleaning.type_converter import convert_brand_types, memory_report
from src.cleaning.validator import validate_brand_dataset


@pytest.fixture
def sample_brand_df():
    return pd.DataFrame(
        [
            {
                "_id": "a1",
                "title": "  iPhone 15 launch  ",
                "author": None,
                "description": None,
                "content": "Apple sold 120 units in 2026.",
                "source": " NewsAPI ",
                "document_type": "json",
                "type": "article",
                "publishedAt": "2026-04-24T19:59:00Z",
                "date": None,
                "url": "https://example.com/apple-story",
                "language": "EN ",
                "rating": "0",
                "content_length": None,
                "content_hash": "hash-1",
                "record_date": "2026-04-25",
                "query_params.year": "2026",
            },
            {
                "_id": "a1",
                "title": "iPhone 15 launch",
                "author": "Reporter",
                "description": "Apple event recap",
                "content": "Apple sold 120 units in 2026.",
                "source": "NewsAPI",
                "document_type": "json",
                "type": "article",
                "publishedAt": "2026-04-24T19:59:00Z",
                "date": None,
                "url": "https://example.com/apple-story",
                "language": "EN",
                "rating": "4.5",
                "content_length": "30",
                "content_hash": "hash-1",
                "record_date": "2026-04-25",
                "query_params.year": "2026",
            },
            {
                "_id": "a2",
                "title": "  iPhone 15 launch ",
                "author": "Reporter",
                "description": "Apple event recap",
                "content": "Apple sold 120 units in 2026.",
                "source": "NewsAPI",
                "document_type": "json",
                "type": "article",
                "publishedAt": "2026-04-24T19:59:00Z",
                "date": None,
                "url": "https://example.com/apple-story-2",
                "language": "english",
                "rating": "3.0",
                "content_length": "30",
                "content_hash": "hash-2",
                "record_date": "2026-04-25",
                "query_params.year": "2026",
            },
            {
                "_id": "a3",
                "title": "Short",
                "author": "Another",
                "description": "tiny",
                "content": None,
                "source": "CSV",
                "document_type": "csv",
                "type": "article",
                "publishedAt": "bad-date",
                "date": None,
                "url": "invalid-url",
                "language": "fr",
                "rating": "2.5",
                "content_length": "0",
                "content_hash": "hash-3",
                "record_date": "2026-04-25",
                "query_params.year": "2026",
                "all_missing": None,
            },
        ]
    )


def test_replace_unrealistic_zeros(sample_brand_df):
    cleaned = replace_unrealistic_zeros(sample_brand_df, ["rating", "content_length"])
    assert pd.isna(cleaned.loc[0, "rating"])
    assert pd.isna(cleaned.loc[3, "content_length"])


def test_fill_numeric_medians(sample_brand_df):
    cleaned = replace_unrealistic_zeros(sample_brand_df, ["rating"])
    cleaned = fill_numeric_medians(cleaned, ["rating"])
    assert cleaned["rating"].isna().sum() == 0
    assert cleaned["rating"].dtype.kind in {"f", "i"}


def test_drop_high_missing_columns(sample_brand_df):
    cleaned = drop_high_missing_columns(sample_brand_df, threshold=0.7)
    assert "all_missing" not in cleaned.columns


def test_handle_missing_values(sample_brand_df):
    cleaned = handle_missing_values(
        sample_brand_df,
        critical_columns=["_id", "title"],
        text_columns=["author", "description"],
        zero_as_missing_columns=["rating"],
        numeric_columns=["rating"],
    )
    assert cleaned["author"].notna().all()
    assert cleaned["description"].notna().all()
    assert cleaned["rating"].isna().sum() == 0


def test_clean_brand_strings(sample_brand_df):
    cleaned = clean_brand_strings(sample_brand_df)
    assert cleaned.loc[0, "title"] == "iPhone 15 launch"
    assert cleaned.loc[0, "language"] == "en"
    assert cleaned.loc[0, "overview"] == "Apple sold 120 units in 2026."
    assert cleaned.loc[0, "mention_year"] == "2026"


def test_regex_helpers(sample_brand_df):
    invalid_dates = detect_invalid_date_formats(sample_brand_df, "publishedAt")
    invalid_languages = detect_invalid_language_codes(sample_brand_df, "language")
    numeric_df = extract_numeric_values_from_text(sample_brand_df, "content")
    flagged_df = flag_short_overviews(
        clean_brand_strings(sample_brand_df),
        column="overview",
        min_length=10,
    )

    assert invalid_dates == 1
    assert invalid_languages == 1
    assert numeric_df.loc[0, "content_numeric_value"] == "120"
    assert bool(flagged_df.loc[3, "overview_is_too_short"]) is True


def test_deduplicator_steps(sample_brand_df):
    duplicate_ids_before = count_duplicates(sample_brand_df, "_id")
    cleaned = drop_duplicate_ids(sample_brand_df, id_columns=["_id"])
    cleaned = drop_duplicate_title_date_pairs(
        clean_brand_strings(cleaned).assign(mention_date="2026-04-24"),
    )

    assert duplicate_ids_before == 1
    assert cleaned["_id"].duplicated().sum() == 0
    assert len(cleaned) == 2


def test_remove_exact_duplicates():
    df = pd.DataFrame([{"_id": "a1", "title": "Apple"}, {"_id": "a1", "title": "Apple"}])
    cleaned = remove_exact_duplicates(df)
    assert len(cleaned) == 1


def test_convert_brand_types_and_memory_report(sample_brand_df):
    cleaned = clean_brand_strings(sample_brand_df)
    converted = convert_brand_types(cleaned)
    report = memory_report(cleaned, converted)

    assert str(converted["publishedAt"].dtype).startswith("datetime64")
    assert str(converted["rating"].dtype) == "float32"
    assert str(converted["document_type"].dtype) == "category"
    assert set(report["stage"]) == {"before", "after", "saved"}


def test_validate_brand_dataset_passes_on_clean_sample():
    df = pd.DataFrame(
        [
            {
                "_id": "ok-1",
                "title": "iPhone launch",
                "url": "https://example.com/iphone",
                "language": "en",
                "rating": 4.5,
                "content_length": 120.0,
                "publishedAt": pd.Timestamp("2026-04-24T19:59:00Z"),
                "mention_date": pd.Timestamp("2026-04-24T00:00:00Z"),
            }
        ]
    )
    validated = validate_brand_dataset(df)
    assert validated.equals(df)


def test_validate_brand_dataset_raises_for_bad_url():
    df = pd.DataFrame(
        [
            {
                "_id": "ok-1",
                "title": "iPhone launch",
                "url": "bad-url",
                "language": "en",
                "publishedAt": pd.Timestamp("2026-04-24T19:59:00Z"),
                "mention_date": pd.Timestamp("2026-04-24T00:00:00Z"),
            }
        ]
    )
    with pytest.raises(AssertionError):
        validate_brand_dataset(df)


def test_run_cleaning_pipeline(sample_brand_df, tmp_path):
    output_path = tmp_path / "cleaned_data.csv"
    cleaned = run_cleaning_pipeline(sample_brand_df, output_path=output_path)

    assert output_path.exists()
    assert cleaned["_id"].duplicated().sum() == 0
    assert "overview" in cleaned.columns
    assert "mention_year" in cleaned.columns
