# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

from coreason_etl_uspto.services.bronze import uspto_applications, uspto_bulk, uspto_grants


def test_uspto_bulk_source() -> None:
    source = uspto_bulk(start_date="2024-01-01", end_date="2024-01-07")

    assert len(source.resources) == 2
    assert "uspto_grants" in source.resources
    assert "uspto_applications" in source.resources


@patch("coreason_etl_uspto.services.bronze.dlt.current.resource_state")
@patch("coreason_etl_uspto.services.bronze.fetch_zip_links")
@patch("coreason_etl_uspto.services.bronze.stream_uspto_zip")
@patch("coreason_etl_uspto.services.bronze.parse_uspto_stream")
@patch("coreason_etl_uspto.services.bronze.establish_epistemic_network_policy")
def test_uspto_grants_resource(
    _mock_network: MagicMock,
    mock_parse: MagicMock,
    mock_stream: MagicMock,
    mock_fetch: MagicMock,
    mock_state: MagicMock,
) -> None:
    mock_state.return_value = {"processed_files": []}
    mock_fetch.return_value = ["https://example.com/test1.zip"]

    def fake_stream() -> Iterator[bytes]:
        yield b""

    mock_stream.return_value = fake_stream()

    mock_parse.return_value = iter([{"us-patent-grant": {"id": "1"}, "_error": False}])

    generator = uspto_grants("2024-01-01", "2024-01-07")
    items = list(generator)

    assert len(items) == 1
    assert items[0]["us-patent-grant"]["id"] == "1"


@patch("coreason_etl_uspto.services.bronze.dlt.current.resource_state")
@patch("coreason_etl_uspto.services.bronze.fetch_zip_links")
@patch("coreason_etl_uspto.services.bronze.stream_uspto_zip")
@patch("coreason_etl_uspto.services.bronze.parse_uspto_stream")
@patch("coreason_etl_uspto.services.bronze.establish_epistemic_network_policy")
def test_uspto_applications_resource(
    _mock_network: MagicMock,
    mock_parse: MagicMock,
    mock_stream: MagicMock,
    mock_fetch: MagicMock,
    mock_state: MagicMock,
) -> None:
    mock_state.return_value = {"processed_files": ["https://example.com/test1.zip"]}
    mock_fetch.return_value = ["https://example.com/test1.zip", "https://example.com/test2.zip"]

    def fake_stream() -> Iterator[bytes]:
        yield b""

    mock_stream.return_value = fake_stream()

    mock_parse.return_value = iter([{"_error": True, "error_message": "test error"}])

    generator = uspto_applications("2024-01-01", "2024-01-07")
    items = list(generator)

    # test2.zip is new, parse yields error
    assert len(items) == 1


@patch("coreason_etl_uspto.services.bronze.dlt.current.resource_state")
@patch("coreason_etl_uspto.services.bronze.fetch_zip_links")
@patch("coreason_etl_uspto.services.bronze.stream_uspto_zip")
@patch("coreason_etl_uspto.services.bronze.establish_epistemic_network_policy")
def test_uspto_grants_resource_exception(
    _mock_network: MagicMock, mock_stream: MagicMock, mock_fetch: MagicMock, mock_state: MagicMock
) -> None:
    mock_state.return_value = {"processed_files": []}
    mock_fetch.return_value = ["https://example.com/test1.zip"]

    mock_stream.side_effect = Exception("Streaming failed")

    generator = uspto_grants("2024-01-01", "2024-01-07")
    items = list(generator)

    assert len(items) == 1


@patch("coreason_etl_uspto.services.bronze.dlt.current.resource_state")
@patch("coreason_etl_uspto.services.bronze.fetch_zip_links")
@patch("coreason_etl_uspto.services.bronze.stream_uspto_zip")
@patch("coreason_etl_uspto.services.bronze.parse_uspto_stream")
@patch("coreason_etl_uspto.services.bronze.establish_epistemic_network_policy")
def test_uspto_grants_resource_error_record(
    _mock_network: MagicMock,
    mock_parse: MagicMock,
    mock_stream: MagicMock,
    mock_fetch: MagicMock,
    mock_state: MagicMock,
) -> None:
    mock_state.return_value = {"processed_files": []}
    mock_fetch.return_value = ["https://example.com/test1.zip"]

    def fake_stream() -> Iterator[bytes]:
        yield b""

    mock_stream.return_value = fake_stream()

    mock_parse.return_value = iter([{"_error": True, "error_message": "test error"}])

    generator = uspto_grants("2024-01-01", "2024-01-07")
    items = list(generator)

    assert len(items) == 1


@patch("coreason_etl_uspto.services.bronze.dlt.current.resource_state")
@patch("coreason_etl_uspto.services.bronze.fetch_zip_links")
@patch("coreason_etl_uspto.services.bronze.stream_uspto_zip")
@patch("coreason_etl_uspto.services.bronze.establish_epistemic_network_policy")
def test_uspto_applications_resource_exception(
    _mock_network: MagicMock, mock_stream: MagicMock, mock_fetch: MagicMock, mock_state: MagicMock
) -> None:
    mock_state.return_value = {"processed_files": []}
    mock_fetch.return_value = ["https://example.com/test1.zip"]

    mock_stream.side_effect = Exception("Streaming failed")

    generator = uspto_applications("2024-01-01", "2024-01-07")
    items = list(generator)

    assert len(items) == 1


@patch("coreason_etl_uspto.services.bronze.dlt.current.resource_state")
@patch("coreason_etl_uspto.services.bronze.fetch_zip_links")
@patch("coreason_etl_uspto.services.bronze.stream_uspto_zip")
@patch("coreason_etl_uspto.services.bronze.parse_uspto_stream")
@patch("coreason_etl_uspto.services.bronze.establish_epistemic_network_policy")
def test_uspto_applications_resource_success(
    _mock_network: MagicMock,
    mock_parse: MagicMock,
    mock_stream: MagicMock,
    mock_fetch: MagicMock,
    mock_state: MagicMock,
) -> None:
    mock_state.return_value = {"processed_files": []}
    mock_fetch.return_value = ["https://example.com/test1.zip"]

    def fake_stream() -> Iterator[bytes]:
        yield b""

    mock_stream.return_value = fake_stream()

    mock_parse.return_value = iter([{"us-patent-application": {"id": "1"}, "_error": False}])

    generator = uspto_applications("2024-01-01", "2024-01-07")
    items = list(generator)

    assert len(items) == 1
    assert items[0]["us-patent-application"]["id"] == "1"
