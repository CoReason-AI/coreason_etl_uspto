# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

from unittest.mock import MagicMock

import pytest
import requests
from requests.exceptions import HTTPError

from coreason_etl_uspto.services.discovery import fetch_zip_links


def test_fetch_zip_links_json_success() -> None:
    session = MagicMock(spec=requests.Session)
    mock_response = MagicMock(spec=requests.Response)
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json.return_value = {
        "results": [
            {"fileDownloadUrl": "https://data.uspto.gov/bulkdata/datasets/ptgrxml/ipg240102.zip"},
            {"fileDownloadUrl": "https://data.uspto.gov/bulkdata/datasets/ptgrxml/ipg240109.zip"},
            {"fileDownloadUrl": "https://data.uspto.gov/bulkdata/datasets/ptgrxml/not_a_zip.txt"},
        ]
    }
    mock_response.raise_for_status.return_value = None
    session.get.return_value = mock_response

    url = "https://data.uspto.gov/bulkdata/datasets/ptgrxml?fileDataFromDate=2024-01-01&fileDataToDate=2024-01-10"
    links = fetch_zip_links(url, session)

    assert len(links) == 2
    assert "https://data.uspto.gov/bulkdata/datasets/ptgrxml/ipg240102.zip" in links
    assert "https://data.uspto.gov/bulkdata/datasets/ptgrxml/ipg240109.zip" in links
    session.get.assert_called_once_with(url)


def test_fetch_zip_links_html_success() -> None:
    session = MagicMock(spec=requests.Session)
    mock_response = MagicMock(spec=requests.Response)
    mock_response.headers = {"Content-Type": "text/html"}
    mock_response.content = b"""
    <html>
        <body>
            <a href="ipg240102.zip">Download 1</a>
            <a href="https://example.com/ipg240109.zip">Download 2</a>
            <a href="not_a_zip.txt">Text File</a>
        </body>
    </html>
    """
    mock_response.raise_for_status.return_value = None
    session.get.return_value = mock_response

    url = "https://data.uspto.gov/bulkdata/datasets/ptgrxml"
    links = fetch_zip_links(url, session)

    assert len(links) == 2
    assert "https://data.uspto.gov/bulkdata/datasets/ipg240102.zip" in links
    assert "https://example.com/ipg240109.zip" in links
    session.get.assert_called_once_with(url)


def test_fetch_zip_links_empty_results() -> None:
    session = MagicMock(spec=requests.Session)
    mock_response = MagicMock(spec=requests.Response)
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json.return_value = {"results": []}
    mock_response.raise_for_status.return_value = None
    session.get.return_value = mock_response

    url = "https://example.com/api"
    links = fetch_zip_links(url, session)

    assert links == []


def test_fetch_zip_links_no_results_key() -> None:
    session = MagicMock(spec=requests.Session)
    mock_response = MagicMock(spec=requests.Response)
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json.return_value = {"other_key": "value"}
    mock_response.raise_for_status.return_value = None
    session.get.return_value = mock_response

    url = "https://example.com/api"
    links = fetch_zip_links(url, session)

    assert links == []


def test_fetch_zip_links_http_error() -> None:
    session = MagicMock(spec=requests.Session)
    mock_response = MagicMock(spec=requests.Response)
    mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")
    session.get.return_value = mock_response

    url = "https://example.com/api"
    with pytest.raises(HTTPError):
        fetch_zip_links(url, session)
