# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

import uuid
from datetime import date

import polars as pl
from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.services.silver import clean_org_name, compute_coreason_id, normalize_silver


def test_clean_org_name() -> None:
    df = pl.DataFrame({"name": ["I.B.M.", "International Business Machines", "Apple Inc.", "foo-bar_baz"]})
    cleaned = df.with_columns(clean_org_name(pl.col("name")).alias("cleaned_name"))

    expected = ["IBM", "INTERNATIONAL BUSINESS MACHINES", "APPLE INC", "FOOBARBAZ"]
    assert cleaned["cleaned_name"].to_list() == expected


def test_compute_coreason_id() -> None:
    policy = FederatedEnvironmentPolicy()
    ns_uuid = uuid.UUID(policy.coreason_entity_namespace)

    df = pl.DataFrame({"name": ["IBM", None, "APPLE INC"]})
    df_with_id = df.with_columns(compute_coreason_id(policy.coreason_entity_namespace, pl.col("name")).alias("id"))

    # Verify UUID5 logic
    expected_ibm = str(uuid.uuid5(ns_uuid, "IBM"))
    expected_apple = str(uuid.uuid5(ns_uuid, "APPLE INC"))

    ids = df_with_id["id"].to_list()
    assert ids[0] == expected_ibm
    assert ids[1] is None
    assert ids[2] == expected_apple


def test_compute_coreason_id_empty_string() -> None:
    policy = FederatedEnvironmentPolicy()
    df = pl.DataFrame({"name": [""]})
    df_with_id = df.with_columns(compute_coreason_id(policy.coreason_entity_namespace, pl.col("name")).alias("id"))

    ids = df_with_id["id"].to_list()
    assert ids[0] is None


def test_normalize_silver_red_book() -> None:
    data = {
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.doc-number": ["01234567"],
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.date": ["20240101"],
        "us-patent-grant.us-bibliographic-data-grant.invention-title.#text": ["  AI SYSTEM  "],
        "abstract.p": ["This is an AI system."],
    }
    df = pl.DataFrame(data)
    normalized = normalize_silver(df)

    assert normalized["patent_number"].to_list() == ["1234567"]
    assert normalized["issue_date"].to_list() == [date(2024, 1, 1)]
    assert normalized["title"].to_list() == ["AI SYSTEM"]
    assert normalized["abstract"].to_list() == ["This is an AI system."]


def test_normalize_silver_green_book() -> None:
    data = {
        "PATDOC.SDOBI.B100.B110": ["00012345"],
        "PATDOC.SDOBI.B100.B140": ["20051231"],
        "PATDOC.SDOBI.B500.B540.STEXT.PDAT": ["  OLD SYSTEM  "],
        "PATDOC.SDOAB.BTEXT.PARA": ["This is an old system."],
    }
    df = pl.DataFrame(data)
    normalized = normalize_silver(df)

    assert normalized["patent_number"].to_list() == ["12345"]
    assert normalized["issue_date"].to_list() == [date(2005, 12, 31)]
    assert normalized["title"].to_list() == ["OLD SYSTEM"]
    assert normalized["abstract"].to_list() == ["This is an old system."]


def test_normalize_silver_missing_columns() -> None:
    # Ensure it handles an empty DataFrame or one missing most columns gracefully
    df = pl.DataFrame({"other_col": ["test"]})
    normalized = normalize_silver(df)

    assert "patent_number" in normalized.columns
    assert "issue_date" in normalized.columns
    assert "title" in normalized.columns
    assert "abstract" in normalized.columns

    assert normalized["patent_number"].to_list() == [None]
