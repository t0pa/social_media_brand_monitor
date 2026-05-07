"""MySQL helpers for Apple brand-monitor analytics."""

from __future__ import annotations

import os
from contextlib import closing
from pathlib import Path

import pandas as pd
import pymysql
from sqlalchemy import create_engine

from src.utils.logger import get_logger


logger = get_logger(__name__)

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "social_media_brand_monitor")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "apple_article_metrics")


def get_mysql_connection(
    host: str = MYSQL_HOST,
    port: int = MYSQL_PORT,
    user: str = MYSQL_USER,
    password: str = MYSQL_PASSWORD,
    database: str = MYSQL_DATABASE,
):
    """Open a reusable PyMySQL connection."""
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    logger.info("Opened MySQL connection | host=%s | port=%s | database=%s", host, port, database)
    return connection


def get_sqlalchemy_engine(
    host: str = MYSQL_HOST,
    port: int = MYSQL_PORT,
    user: str = MYSQL_USER,
    password: str = MYSQL_PASSWORD,
    database: str = MYSQL_DATABASE,
):
    """Create a SQLAlchemy engine for pandas read_sql."""
    password_part = password or ""
    connection_url = f"mysql+pymysql://{user}:{password_part}@{host}:{port}/{database}"
    return create_engine(connection_url)


def create_article_metrics_table(connection, table_name: str = MYSQL_TABLE) -> None:
    """Create the MySQL table used for Lab 10 relational analytics."""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        mention_id VARCHAR(64) PRIMARY KEY,
        title TEXT NOT NULL,
        source_name VARCHAR(255),
        source_domain VARCHAR(255),
        author_name VARCHAR(255),
        document_type VARCHAR(64),
        content_type VARCHAR(64),
        language_code VARCHAR(32),
        mention_year INT,
        rating_value FLOAT,
        title_length INT,
        overview_length INT,
        published_at DATETIME NULL,
        mention_date DATETIME NULL
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    with closing(connection.cursor()) as cursor:
        cursor.execute(create_sql)
    logger.info("Ensured MySQL table exists: %s", table_name)


def _prepare_article_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Project cleaned Apple data into the relational metrics schema."""
    working = df.copy()
    working["source_domain"] = (
        working.get("url", pd.Series(index=working.index, dtype="string"))
        .astype("string")
        .str.extract(r"https?://([^/]+)", expand=False)
    )
    working["title_length"] = working.get("title", pd.Series(index=working.index, dtype="string")).astype("string").str.len()
    working["overview_length"] = (
        working.get("overview", pd.Series(index=working.index, dtype="string"))
        .astype("string")
        .str.len()
    )

    metrics = pd.DataFrame(
        {
            "mention_id": working.get("_id"),
            "title": working.get("title"),
            "source_name": working.get("source"),
            "source_domain": working.get("source_domain"),
            "author_name": working.get("author"),
            "document_type": working.get("document_type"),
            "content_type": working.get("type"),
            "language_code": working.get("language"),
            "mention_year": pd.to_numeric(working.get("mention_year"), errors="coerce"),
            "rating_value": pd.to_numeric(working.get("rating"), errors="coerce"),
            "title_length": pd.to_numeric(working.get("title_length"), errors="coerce"),
            "overview_length": pd.to_numeric(working.get("overview_length"), errors="coerce"),
            "published_at": pd.to_datetime(working.get("publishedAt"), errors="coerce"),
            "mention_date": pd.to_datetime(working.get("mention_date"), errors="coerce"),
        }
    )
    return metrics


def populate_article_metrics(
    connection,
    cleaned_df: pd.DataFrame,
    table_name: str = MYSQL_TABLE,
) -> int:
    """Insert cleaned Apple mention metrics into MySQL with upsert semantics."""
    metrics = _prepare_article_metrics(cleaned_df)
    metrics = metrics.dropna(subset=["mention_id", "title"]).copy()

    insert_sql = f"""
    INSERT INTO `{table_name}` (
        mention_id, title, source_name, source_domain, author_name,
        document_type, content_type, language_code, mention_year, rating_value,
        title_length, overview_length, published_at, mention_date
    ) VALUES (
        %(mention_id)s, %(title)s, %(source_name)s, %(source_domain)s, %(author_name)s,
        %(document_type)s, %(content_type)s, %(language_code)s, %(mention_year)s, %(rating_value)s,
        %(title_length)s, %(overview_length)s, %(published_at)s, %(mention_date)s
    )
    ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        source_name = VALUES(source_name),
        source_domain = VALUES(source_domain),
        author_name = VALUES(author_name),
        document_type = VALUES(document_type),
        content_type = VALUES(content_type),
        language_code = VALUES(language_code),
        mention_year = VALUES(mention_year),
        rating_value = VALUES(rating_value),
        title_length = VALUES(title_length),
        overview_length = VALUES(overview_length),
        published_at = VALUES(published_at),
        mention_date = VALUES(mention_date)
    """

    inserted_rows = 0
    with closing(connection.cursor()) as cursor:
        for row in metrics.to_dict(orient="records"):
            safe_row = {
                key: (None if pd.isna(value) else value.to_pydatetime() if hasattr(value, "to_pydatetime") else value)
                for key, value in row.items()
            }
            cursor.execute(insert_sql, safe_row)
            inserted_rows += 1

    logger.info("Populated MySQL article metrics | rows_processed=%s | table=%s", inserted_rows, table_name)
    return inserted_rows


def query_article_metrics(
    table_name: str = MYSQL_TABLE,
    where_clause: str | None = None,
) -> pd.DataFrame:
    """Query Apple article metrics back into pandas using pd.read_sql."""
    sql = f"SELECT * FROM `{table_name}`"
    if where_clause:
        sql = f"{sql} WHERE {where_clause}"

    engine = get_sqlalchemy_engine()
    dataframe = pd.read_sql(sql, engine)
    logger.info("Queried MySQL article metrics | rows=%s | table=%s", len(dataframe), table_name)
    return dataframe


def load_cleaned_csv(csv_path: Path | str) -> pd.DataFrame:
    """Convenience loader for the cleaned Apple CSV."""
    dataframe = pd.read_csv(csv_path)
    logger.info("Loaded cleaned CSV for MySQL population | path=%s | rows=%s", csv_path, len(dataframe))
    return dataframe
