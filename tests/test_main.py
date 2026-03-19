# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

import sys
from unittest.mock import ANY, MagicMock, patch

import polars as pl
import pytest
from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.main import _refine_bronze_table, main, run_pipeline


@patch("coreason_etl_uspto.main.dlt.pipeline")
@patch("coreason_etl_uspto.main.uspto_bulk")
@patch("coreason_etl_uspto.main._refine_bronze_table")
def test_run_pipeline_success(
    mock_refine_bronze_table: MagicMock,
    mock_uspto_bulk: MagicMock,
    mock_dlt_pipeline: MagicMock,
) -> None:
    # Setup mocks
    mock_pipeline_instance = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline_instance
    mock_pipeline_instance.run.return_value = "Mock Load Info"
    mock_source = MagicMock()
    mock_uspto_bulk.return_value = mock_source

    run_pipeline("2024-01-01", "2024-01-02")

    mock_dlt_pipeline.assert_called_once()
    mock_uspto_bulk.assert_called_once_with(start_date="2024-01-01", end_date="2024-01-02")
    mock_pipeline_instance.run.assert_called_once_with(mock_source)
    assert mock_refine_bronze_table.call_count == 2
    mock_refine_bronze_table.assert_any_call(mock_pipeline_instance, ANY, "uspto_grants")
    mock_refine_bronze_table.assert_any_call(mock_pipeline_instance, ANY, "uspto_applications")


@patch("coreason_etl_uspto.main.dlt.pipeline")
@patch("coreason_etl_uspto.main.uspto_bulk")
@patch("coreason_etl_uspto.main._refine_bronze_table")
def test_run_pipeline_refine_exception_handled(
    mock_refine_bronze_table: MagicMock,
    mock_uspto_bulk: MagicMock,  # noqa: ARG001
    mock_dlt_pipeline: MagicMock,
) -> None:
    # Setup mocks
    mock_pipeline_instance = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline_instance

    # Force _refine_bronze_table to raise an exception for the first table,
    # run_pipeline should catch it, log warning, and continue
    mock_refine_bronze_table.side_effect = Exception("Test refine error")

    # Should not raise
    run_pipeline("2024-01-01", "2024-01-02")

    assert mock_refine_bronze_table.call_count == 2


@patch("coreason_etl_uspto.main.dlt.pipeline")
def test_run_pipeline_outer_exception(mock_dlt_pipeline: MagicMock) -> None:
    # Setup mock to raise error during client context
    mock_pipeline_instance = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline_instance
    mock_pipeline_instance.sql_client.side_effect = Exception("Critical SQL Client Error")

    with pytest.raises(Exception, match="Critical SQL Client Error"):
        run_pipeline()


@patch("coreason_etl_uspto.main.pl.read_database")
@patch("coreason_etl_uspto.main.normalize_silver")
def test_refine_bronze_table_success(mock_normalize_silver: MagicMock, mock_read_database: MagicMock) -> None:
    mock_pipeline = MagicMock()
    mock_client = MagicMock()
    mock_pipeline.sql_client.return_value = mock_client

    mock_conn = MagicMock()
    mock_client.native_connection = mock_conn

    # Setup the dataframe returned by read_database
    mock_df_bronze = pl.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    mock_read_database.return_value = mock_df_bronze

    # Setup the silver df returned by normalize_silver
    mock_df_silver = pl.DataFrame({"clean_id": [1, 2], "name": ["A", "B"]})
    mock_normalize_silver.return_value = mock_df_silver

    policy = FederatedEnvironmentPolicy()

    result = _refine_bronze_table(mock_pipeline, policy, "uspto_grants")

    assert result is mock_df_silver
    mock_read_database.assert_called_once()
    assert mock_read_database.call_args[1]["connection"] == mock_conn
    mock_normalize_silver.assert_called_once_with(mock_df_bronze)


@patch("coreason_etl_uspto.main.pl.read_database")
@patch("coreason_etl_uspto.main.normalize_silver")
def test_refine_bronze_table_empty(mock_normalize_silver: MagicMock, mock_read_database: MagicMock) -> None:
    mock_pipeline = MagicMock()
    mock_client = MagicMock()
    mock_pipeline.sql_client.return_value = mock_client

    # Setup an empty dataframe
    mock_df_bronze = pl.DataFrame({"id": [], "name": []}, schema={"id": pl.Int64, "name": pl.String})
    mock_read_database.return_value = mock_df_bronze

    policy = FederatedEnvironmentPolicy()

    result = _refine_bronze_table(mock_pipeline, policy, "uspto_grants")

    assert result is None
    mock_read_database.assert_called_once()
    mock_normalize_silver.assert_not_called()


def test_refine_bronze_table_exception() -> None:
    mock_pipeline = MagicMock()
    mock_pipeline.sql_client.side_effect = Exception("DB Connection Error")

    policy = FederatedEnvironmentPolicy()

    with pytest.raises(Exception, match="DB Connection Error"):
        _refine_bronze_table(mock_pipeline, policy, "uspto_grants")


@patch("coreason_etl_uspto.main.run_pipeline")
def test_main_cli_success(mock_run_pipeline: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["main.py", "--start-date", "2024-02-01", "--end-date", "2024-02-15"])

    main()

    mock_run_pipeline.assert_called_once_with(start_date="2024-02-01", end_date="2024-02-15")


@patch("coreason_etl_uspto.main.run_pipeline")
def test_main_cli_exception(mock_run_pipeline: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["main.py"])
    mock_run_pipeline.side_effect = Exception("Pipeline Failed Horribly")

    with pytest.raises(SystemExit) as excinfo:
        main()

    assert excinfo.value.code == 1
