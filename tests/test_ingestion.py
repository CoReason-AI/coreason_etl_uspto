# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

import io
import zipfile
from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
import requests
from requests.exceptions import HTTPError

from coreason_etl_uspto.services.ingestion import FakeRootStream, parse_uspto_stream, stream_uspto_zip


def create_mock_zip(xml_content: bytes, filename: str = "data.xml") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(filename, xml_content)
    return buffer.getvalue()


def test_stream_uspto_zip_success() -> None:
    session = MagicMock(spec=requests.Session)
    mock_response = MagicMock(spec=requests.Response)

    xml_data = b"<us-patent-grant><id>1</id></us-patent-grant>"
    zip_bytes = create_mock_zip(xml_data)

    def iter_content(chunk_size: int = 1) -> Iterator[bytes]:
        _ = chunk_size
        yield zip_bytes

    mock_response.iter_content = iter_content
    mock_response.raise_for_status.return_value = None
    session.get.return_value = mock_response

    url = "https://example.com/test.zip"
    stream = stream_uspto_zip(url, session)

    chunks = list(stream)
    assert b"".join(chunks) == xml_data


def test_stream_uspto_zip_no_xml() -> None:
    session = MagicMock(spec=requests.Session)
    mock_response = MagicMock(spec=requests.Response)

    zip_bytes = create_mock_zip(b"some text", "data.txt")

    def iter_content(chunk_size: int = 1) -> Iterator[bytes]:
        _ = chunk_size
        yield zip_bytes

    mock_response.iter_content = iter_content
    mock_response.raise_for_status.return_value = None
    session.get.return_value = mock_response

    url = "https://example.com/test.zip"
    stream = stream_uspto_zip(url, session)

    chunks = list(stream)
    assert chunks == []


def test_stream_uspto_zip_http_error() -> None:
    session = MagicMock(spec=requests.Session)
    mock_response = MagicMock(spec=requests.Response)
    mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")
    session.get.return_value = mock_response

    url = "https://example.com/test.zip"
    with pytest.raises(HTTPError):
        list(stream_uspto_zip(url, session))


def test_fake_root_stream() -> None:
    def stream_gen() -> Iterator[bytes]:
        yield b"<child>1</child>"
        yield b"<child>2</child>"

    fake_stream = FakeRootStream(stream_gen())
    data = fake_stream.read()
    assert data == b"<root>\n<child>1</child><child>2</child>\n</root>"


def test_fake_root_stream_chunks() -> None:
    def stream_gen() -> Iterator[bytes]:
        yield b"<child>1</child>"
        yield b"<child>2</child>"

    fake_stream = FakeRootStream(stream_gen())
    chunk1 = fake_stream.read(4)
    assert chunk1 == b"<roo"
    chunk2 = fake_stream.read(4)
    assert chunk2 == b"t>\n<"
    chunk3 = fake_stream.read()
    assert chunk3 == b"child>1</child><child>2</child>\n</root>"
    chunk4 = fake_stream.read()
    assert chunk4 == b""


def test_parse_uspto_stream_success() -> None:
    def stream_gen() -> Iterator[bytes]:
        yield b'<?xml version="1.0" encoding="UTF-8"?>\n'
        yield b"<us-patent-grant><id>1</id></us-patent-grant>"
        yield b'<?xml version="1.0" encoding="UTF-8"?>\n'
        yield b"<us-patent-grant><id>2</id></us-patent-grant>"

    url = "https://example.com/test.zip"
    dicts = list(parse_uspto_stream(stream_gen(), "us-patent-grant", url))

    assert len(dicts) == 2
    assert dicts[0]["us-patent-grant"]["id"] == "1"
    assert dicts[0]["_root_element"] == "us-patent-grant"
    assert dicts[0]["ingestion_meta"]["source_file"] == url

    assert dicts[1]["us-patent-grant"]["id"] == "2"
    assert dicts[1]["_root_element"] == "us-patent-grant"


def test_parse_uspto_stream_invalid_xml_handling() -> None:
    def stream_gen() -> Iterator[bytes]:
        # Missing closing tag
        yield b"<us-patent-grant><id>1</id>"

    url = "https://example.com/test.zip"
    # Using lxml recover=True might still parse it partially or it might fail at etree.tostring
    # but the fake root should at least ensure it doesn't crash entirely.
    dicts = list(parse_uspto_stream(stream_gen(), "us-patent-grant", url))

    # Actually, lxml won't yield events for incomplete tags if it's strictly the main tag,
    # but since we close root, the tag might be auto-closed.
    assert len(dicts) == 1
    if "_error" not in dicts[0]:
        assert dicts[0]["us-patent-grant"]["id"] == "1"


def test_parse_uspto_stream_failure() -> None:
    # We can trigger an exception in the inner loop by mocking xmltodict to raise
    def stream_gen() -> Iterator[bytes]:
        yield b"<us-patent-grant><id>1</id></us-patent-grant>"

    url = "https://example.com/test.zip"

    from typing import Any

    import xmltodict

    original_parse = xmltodict.parse

    def failing_parse(*args: Any, **kwargs: Any) -> Any:
        _ = args, kwargs
        raise ValueError("Simulated parsing error")

    try:
        xmltodict.parse = failing_parse
        dicts = list(parse_uspto_stream(stream_gen(), "us-patent-grant", url))
        assert len(dicts) == 1
        assert dicts[0]["_error"] is True
        assert dicts[0]["error_message"] == "Simulated parsing error"
        assert dicts[0]["source_url"] == url
    finally:
        xmltodict.parse = original_parse
