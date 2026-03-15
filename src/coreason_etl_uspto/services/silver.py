# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

"""
AGENT INSTRUCTION: This module provides the Silver Layer transformation pipeline,
normalizing raw JSON soup into structured DTD-aware schemas using Polars.
"""

import uuid

import polars as pl

from coreason_etl_uspto.utils.logger import logger


def clean_org_name(name: pl.Expr) -> pl.Expr:
    """
    AGENT INSTRUCTION: Normalize organization names (upper case, remove punctuation).
    """
    return name.str.to_uppercase().str.replace_all(r"[.,;'\"\(\)\[\]\{\}\-\\_]", "").str.strip_chars()


def compute_coreason_id(namespace: str, name_expr: pl.Expr) -> pl.Expr:
    """
    AGENT INSTRUCTION: Compute a UUID5 deterministic hash using the namespace and normalized string.
    Actually, Polars doesn't have a native UUID5 function. We will use a Python map or write a custom
    plugin. For simplicity without a custom rust plugin, we can map elements,
    but map_elements is slow. However, for the scope of this implementation, we can use map_elements
    or native hashing like SHA256 truncated, but the requirement specifies UUID5(NAMESPACE, CLEAN_NAME).
    """
    ns_uuid = uuid.UUID(namespace)

    def hash_uuid(name: str | None) -> str | None:
        if not name:
            return None
        return str(uuid.uuid5(ns_uuid, name))

    return name_expr.map_elements(hash_uuid, return_dtype=pl.String)


def normalize_silver(df: pl.DataFrame) -> pl.DataFrame:
    """
    AGENT INSTRUCTION: Normalize distinct DTD columns into Silver Schema.
     Implements the "Strategy Pattern" using Polars coalesce based on the detected root element paths.
    """
    logger.info("Normalizing dataframe to silver schema")

    # Check if necessary columns exist, if not create them full of nulls to allow coalesce to work
    expected_cols = [
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.doc-number",
        "PATDOC.SDOBI.B100.B110",
        "PATDOC.WKU",
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.date",
        "PATDOC.SDOBI.B100.B140",
        "us-patent-grant.us-bibliographic-data-grant.invention-title.#text",
        "PATDOC.SDOBI.B500.B540.STEXT.PDAT",
        "PATDOC.TTL",
        "abstract.p",
        "PATDOC.SDOAB.BTEXT.PARA",
        "PATDOC.ABST",
    ]

    df_cols = df.columns
    for col in expected_cols:
        if col not in df_cols:
            df = df.with_columns(pl.lit(None).cast(pl.String).alias(col))

    # FederatedEnvironmentPolicy imported but not used, we can remove or use it for namespace if needed here.
    # Currently not used in this function, so let's remove it to appease linters and coverage.

    # 1. Patent Number Normalization
    doc_number = (
        pl.coalesce(
            [
                pl.col("us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.doc-number"),
                pl.col("PATDOC.SDOBI.B100.B110"),
                pl.col("PATDOC.WKU"),
            ]
        )
        .str.strip_chars("0")
        .alias("patent_number")
    )

    # 2. Date Normalization
    issue_date = (
        pl.coalesce(
            [
                pl.col("us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.date"),
                pl.col("PATDOC.SDOBI.B100.B140"),
            ]
        )
        .str.strptime(pl.Date, "%Y%m%d", strict=False)
        .alias("issue_date")
    )

    # 3. Title Normalization
    title = (
        pl.coalesce(
            [
                pl.col("us-patent-grant.us-bibliographic-data-grant.invention-title.#text"),
                pl.col("PATDOC.SDOBI.B500.B540.STEXT.PDAT"),
                pl.col("PATDOC.TTL"),
            ]
        )
        .str.strip_chars()
        .alias("title")
    )

    # 4. Abstract Normalization
    abstract = pl.coalesce(
        [
            pl.col("abstract.p"),
            pl.col("PATDOC.SDOAB.BTEXT.PARA"),
            pl.col("PATDOC.ABST"),
        ]
    ).alias("abstract")

    return df.with_columns([doc_number, issue_date, title, abstract])
