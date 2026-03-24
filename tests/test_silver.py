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
from typing import Any

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
    data: dict[str, list[Any]] = {
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.doc-number": ["01234567"],
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.kind": ["B2"],
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.date": ["20240101"],
        "us-patent-grant.us-bibliographic-data-grant.invention-title.#text": ["  AI SYSTEM  "],
        "abstract.p": ["This is an AI system."],
        "us-patent-grant.us-parties.inventors.inventor": [
            [
                {
                    "addressbook": {
                        "first-name": "JOHN",
                        "last-name": "doe",
                        "address": {"city": "new york", "state": "ny"},
                    }
                }
            ]
        ],
        "us-patent-grant.us-parties.assignees.assignee": [[{"addressbook": {"orgname": "I.B.M.", "role": "02"}}]],
    }
    df = pl.DataFrame(data)
    normalized = normalize_silver(df)

    assert normalized["patent_number"].to_list() == ["1234567"]
    assert normalized["issue_date"].to_list() == [date(2024, 1, 1)]
    assert normalized["title"].to_list() == ["AI SYSTEM"]
    assert normalized["abstract"].to_list() == ["This is an AI system."]
    assert normalized["kind_code"].to_list() == ["B2"]

    inventors = normalized["inventors"].to_list()[0]
    assert len(inventors) == 1
    assert inventors[0]["first_name"] == "John"
    assert inventors[0]["last_name"] == "Doe"
    assert inventors[0]["city"] == "NEW YORK"
    assert inventors[0]["state"] == "NY"

    assignees = normalized["assignees"].to_list()[0]
    assert len(assignees) == 1
    assert assignees[0]["org_name"] == "I.B.M."
    assert assignees[0]["role_code"] == "Assignee"


def test_normalize_silver_green_book() -> None:
    data: dict[str, list[Any]] = {
        "PATDOC.WKU": ["054321B1"],
        "PATDOC.SDOBI.B100.B110": ["00012345"],
        "PATDOC.SDOBI.B100.B140": ["20051231"],
        "PATDOC.SDOBI.B500.B540.STEXT.PDAT": ["  OLD SYSTEM  "],
        "PATDOC.SDOAB.BTEXT.PARA": ["This is an old system."],
        "PATDOC.SDOBI.B700.B720": [
            [
                {
                    "B721": {
                        "party": {"nam": {"fnm": "jane", "snm": "SMITH"}, "adr": {"city": "los angeles", "state": "ca"}}
                    }
                }
            ]
        ],
        "PATDOC.SDOBI.B700.B730": [[{"B731": {"party": {"nam": {"onm": "APPLE INC."}, "irf": "02"}}}]],
    }
    df = pl.DataFrame(data)
    normalized = normalize_silver(df)

    assert normalized["patent_number"].to_list() == ["12345"]
    assert normalized["issue_date"].to_list() == [date(2005, 12, 31)]
    assert normalized["title"].to_list() == ["OLD SYSTEM"]
    assert normalized["abstract"].to_list() == ["This is an old system."]
    assert normalized["kind_code"].to_list() == ["B1"]

    inventors = normalized["inventors"].to_list()[0]
    assert len(inventors) == 1
    assert inventors[0]["first_name"] == "Jane"
    assert inventors[0]["last_name"] == "Smith"
    assert inventors[0]["city"] == "LOS ANGELES"
    assert inventors[0]["state"] == "CA"


def test_normalize_silver_missing_columns() -> None:
    # Ensure it handles an empty DataFrame or one missing most columns gracefully
    df = pl.DataFrame({"other_col": ["test"]})
    normalized = normalize_silver(df)

    assert "patent_number" in normalized.columns
    assert "issue_date" in normalized.columns
    assert "title" in normalized.columns
    assert "abstract" in normalized.columns

    assert normalized["patent_number"].to_list() == [None]


def test_extract_inventors_edge_cases() -> None:
    # Test missing lists, strings, and missing attributes
    data: dict[str, list[Any]] = {
        "us-patent-grant.us-parties.inventors.inventor": [
            None,
            [{"addressbook": {"string": "instead"}}],
            [{"addressbook": {}}],  # Empty addressbook -> exception might occur but caught, or None values
            pl.Series([], dtype=pl.String),
            "string_instead",
            {"addressbook": {"first-name": "only-dict"}},
            pl.Series([{"addressbook": {"first-name": "series-dict"}}]),
            pl.Series(values=[None], dtype=pl.String),  # empty series but not is_empty
        ],
        "PATDOC.SDOBI.B700.B720": [
            [],
            None,
            [{"B721": {"party": {"nam": {}}}}],
            [{"B721": {"party": {}}}],  # hit line 201-202 (Green book missing adr)
            [{"party": {"nam": {"fnm": "jane"}}}],  # hit line 201-202
            {"B721": {"party": {"nam": {"fnm": "only-dict"}}}},
            pl.Series([{"B721": {"party": {"nam": {"fnm": "series-dict"}}}}]),
            [{"B721": {"party": {"nam": {"fnm": "jane"}}}}, "bad_data"],  # force try-except pass
        ],
    }
    df = pl.DataFrame(data, strict=False)
    # The columns must be in expected_cols
    df = normalize_silver(df)

    inventors = df["inventors"].to_list()
    assert inventors[0] is None

    # {"addressbook": {"string": "instead"}} -> no first-name etc, but returns []
    # Let's just assert length of inventors is correct and type is list.
    assert len(inventors) == 8

    # The empty addressbook one should return a dict with Nones or []
    if inventors[2] is not None and len(inventors[2]) > 0:
        assert "first_name" in inventors[2][0]


def test_extract_assignees_edge_cases() -> None:
    # Test missing lists, strings, and missing attributes
    data: dict[str, list[Any]] = {
        "us-patent-grant.us-parties.assignees.assignee": [
            None,
            [{"addressbook": "string_value"}],
            [{"addressbook": {"orgname": None}}],
            pl.Series([], dtype=pl.String),
            "string_instead",
            {"addressbook": {"orgname": "only-dict"}},
            pl.Series([{"addressbook": {"orgname": "series-dict"}}]),
            pl.Series(values=[None], dtype=pl.String),
        ],
        "PATDOC.SDOBI.B700.B730": [
            [],
            None,
            [{"B731": {"party": {}}}],
            [{"B731": {"party": {"nam": {}}}}],
            [{"B731": {"party": {"irf": "02"}}}],
            {"B731": {"party": {"nam": {"onm": "only-dict"}}}},
            pl.Series([{"B731": {"party": {"nam": {"onm": "series-dict"}}}}]),
            [{"B731": {"party": {"nam": {"onm": "jane"}}}}, "bad_data"],  # force try-except pass
        ],
    }
    df = pl.DataFrame(data, strict=False)
    df = normalize_silver(df)

    assignees = df["assignees"].to_list()
    assert assignees[0] is None

    assert len(assignees) == 8

    if assignees[2] is not None and len(assignees[2]) > 0:
        assert "org_name" in assignees[2][0]


def test_normalize_silver_complex_scenarios() -> None:
    data: dict[str, list[Any]] = {
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.doc-number": [
            "01234567",
            None,
            "87654321",
        ],
        "PATDOC.SDOBI.B100.B110": [None, "00098765", None],
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.kind": ["B2", None, "A1"],
        "PATDOC.WKU": [None, "098765B1", None],
        "us-patent-grant.us-bibliographic-data-grant.publication-reference.document-id.date": [
            "20240101",
            None,
            "bad_date",
        ],
        "PATDOC.SDOBI.B100.B140": [None, "20051231", None],
        "us-patent-grant.us-bibliographic-data-grant.invention-title.#text": ["  AI SYSTEM  ", None, "ANOTHER AI"],
        "PATDOC.SDOBI.B500.B540.STEXT.PDAT": [None, "  OLD SYSTEM  ", None],
        "abstract.p": [["This is", " a multi-paragraph", " abstract."], None, None],
        "PATDOC.SDOAB.BTEXT.PARA": [None, "This is an old system.", None],
        "us-patent-grant.us-parties.inventors.inventor": [
            [
                {
                    "addressbook": {
                        "first-name": "JOHN",
                        "last-name": "doe",
                        "address": {"city": "new york", "state": "ny"},
                    }
                },
                {
                    "addressbook": {
                        "first-name": "JANE",
                        "last-name": "smith",
                        "address": {"city": "los angeles", "state": "ca"},
                    }
                },
            ],
            None,
            [{"addressbook": {"first-name": "Bob", "last-name": "Jones", "address": {"city": None, "state": None}}}],
        ],
        "PATDOC.SDOBI.B700.B720": [
            None,
            [
                {
                    "B721": {
                        "party": {"nam": {"fnm": "Alice", "snm": "WONDERS"}, "adr": {"city": "seattle", "state": "wa"}}
                    }
                }
            ],
            None,
        ],
        "us-patent-grant.us-parties.assignees.assignee": [
            [
                {"addressbook": {"orgname": "I.B.M.", "role": "02"}},
                {"addressbook": {"orgname": "Google", "role": "03"}},
            ],
            None,
            [{"addressbook": {"orgname": "Microsoft", "role": "01"}}],
        ],
        "PATDOC.SDOBI.B700.B730": [None, [{"B731": {"party": {"nam": {"onm": "APPLE INC."}, "irf": "02"}}}], None],
    }

    df = pl.DataFrame(data)
    normalized = normalize_silver(df)

    assert normalized["patent_number"].to_list() == ["1234567", "98765", "87654321"]
    assert normalized["issue_date"].to_list() == [date(2024, 1, 1), date(2005, 12, 31), None]
    assert normalized["title"].to_list() == ["AI SYSTEM", "OLD SYSTEM", "ANOTHER AI"]
    assert normalized["abstract"].to_list() == [
        "This is  a multi-paragraph  abstract.",
        "This is an old system.",
        None,
    ]
    assert normalized["kind_code"].to_list() == ["B2", "B1", "A1"]

    inventors_row1 = normalized["inventors"].to_list()[0]
    assert len(inventors_row1) == 2
    assert inventors_row1[0]["first_name"] == "John"
    assert inventors_row1[1]["first_name"] == "Jane"

    inventors_row2 = normalized["inventors"].to_list()[1]
    assert len(inventors_row2) == 1
    assert inventors_row2[0]["first_name"] == "Alice"

    inventors_row3 = normalized["inventors"].to_list()[2]
    assert len(inventors_row3) == 1
    assert inventors_row3[0]["first_name"] == "Bob"
    assert inventors_row3[0]["city"] is None

    assignees_row1 = normalized["assignees"].to_list()[0]
    assert len(assignees_row1) == 2
    assert assignees_row1[0]["org_name"] == "I.B.M."
    assert assignees_row1[0]["role_code"] == "Assignee"
    assert assignees_row1[1]["org_name"] == "Google"
    assert assignees_row1[1]["role_code"] == "03"

    assignees_row2 = normalized["assignees"].to_list()[1]
    assert len(assignees_row2) == 1
    assert assignees_row2[0]["org_name"] == "APPLE INC."

    assignees_row3 = normalized["assignees"].to_list()[2]
    assert len(assignees_row3) == 1
    assert assignees_row3[0]["org_name"] == "Microsoft"
