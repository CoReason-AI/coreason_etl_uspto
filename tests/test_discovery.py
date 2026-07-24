# Copyright (c) 2026 CoReason, Inc.
from unittest.mock import MagicMock
import requests
from coreason_etl_uspto.services.discovery import fetch_zip_links

def test_fetch_zip_links_synthetic_fallback_grants() -> None:
    session = MagicMock(spec=requests.Session)
    url = "https://data.uspto.gov/bulkdata/datasets/ptgrxml"
    links = fetch_zip_links(url, session)
    assert len(links) == 1
    assert "file://" in links[0]
    assert "downloads/ipg240102.zip" in links[0]

def test_fetch_zip_links_synthetic_fallback_apps() -> None:
    session = MagicMock(spec=requests.Session)
    url = "https://data.uspto.gov/bulkdata/datasets/pba"
    links = fetch_zip_links(url, session)
    assert len(links) == 1
    assert "file://" in links[0]
    assert "downloads/ipa240102.zip" in links[0]
