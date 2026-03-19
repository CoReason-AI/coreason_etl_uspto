# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

import pytest
from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError


def test_default_federated_environment_policy() -> None:
    policy = FederatedEnvironmentPolicy()
    assert policy.app_env == "development"
    assert policy.debug is False
    assert policy.log_level == "INFO"
    assert policy.uspto_grants_api_endpoint == "https://data.uspto.gov/bulkdata/datasets/ptgrxml"
    assert policy.uspto_apps_api_endpoint == "https://data.uspto.gov/bulkdata/datasets/pba"
    assert policy.uspto_user_agent == "CoReason-Bot (contact: admin@coreason.com)"
    assert policy.dlt_pipeline_name == "uspto_pipeline"
    assert policy.dlt_dataset_name == "bronze"
    assert policy.silver_schema == "silver"
    assert policy.gold_schema == "gold"
    assert policy.pghost == "localhost"
    assert policy.pgport == 5432
    assert policy.pguser == "postgres"
    assert policy.pgpassword == "postgres"
    assert policy.pgdatabase == "coreason_etl_uspto"
    assert policy.coreason_entity_namespace == "1b671a64-40d5-491e-99b0-da01ff1f3341"
    assert policy.uspto_stream_chunk_size == 8192
    assert policy.uspto_max_memory_mb == 10
    assert policy.uspto_http_timeout == 60
    assert policy.uspto_max_retries == 3
    assert policy.uspto_max_stream_size_mb == 5120


@given(
    app_env=st.sampled_from(["development", "testing", "production"]),
    debug=st.booleans(),
    secret_key=st.text(min_size=1),
    log_level=st.sampled_from(["DEBUG", "INFO", "WARNING", "ERROR"]),
    uspto_grants_api_endpoint=st.text(min_size=1),
    uspto_apps_api_endpoint=st.text(min_size=1),
    uspto_user_agent=st.text(min_size=1),
    dlt_pipeline_name=st.text(min_size=1),
    dlt_dataset_name=st.text(min_size=1),
    silver_schema=st.text(min_size=1),
    gold_schema=st.text(min_size=1),
    pghost=st.text(min_size=1),
    pgport=st.integers(min_value=1, max_value=65535),
    pguser=st.text(min_size=1),
    pgpassword=st.text(min_size=1),
    pgdatabase=st.text(min_size=1),
    coreason_entity_namespace=st.text(min_size=1),
    uspto_stream_chunk_size=st.integers(min_value=1),
    uspto_max_memory_mb=st.integers(min_value=1),
    uspto_http_timeout=st.integers(min_value=1),
    uspto_max_retries=st.integers(min_value=0),
    uspto_max_stream_size_mb=st.integers(min_value=1),
)
def test_federated_environment_policy_valid(
    app_env: str,
    debug: bool,
    secret_key: str,
    log_level: str,
    uspto_grants_api_endpoint: str,
    uspto_apps_api_endpoint: str,
    uspto_user_agent: str,
    dlt_pipeline_name: str,
    dlt_dataset_name: str,
    silver_schema: str,
    gold_schema: str,
    pghost: str,
    pgport: int,
    pguser: str,
    pgpassword: str,
    pgdatabase: str,
    coreason_entity_namespace: str,
    uspto_stream_chunk_size: int,
    uspto_max_memory_mb: int,
    uspto_http_timeout: int,
    uspto_max_retries: int,
    uspto_max_stream_size_mb: int,
) -> None:
    policy = FederatedEnvironmentPolicy(
        app_env=app_env,
        debug=debug,
        secret_key=secret_key,
        log_level=log_level,
        uspto_grants_api_endpoint=uspto_grants_api_endpoint,
        uspto_apps_api_endpoint=uspto_apps_api_endpoint,
        uspto_user_agent=uspto_user_agent,
        dlt_pipeline_name=dlt_pipeline_name,
        dlt_dataset_name=dlt_dataset_name,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
        pghost=pghost,
        pgport=pgport,
        pguser=pguser,
        pgpassword=pgpassword,
        pgdatabase=pgdatabase,
        coreason_entity_namespace=coreason_entity_namespace,
        uspto_stream_chunk_size=uspto_stream_chunk_size,
        uspto_max_memory_mb=uspto_max_memory_mb,
        uspto_http_timeout=uspto_http_timeout,
        uspto_max_retries=uspto_max_retries,
        uspto_max_stream_size_mb=uspto_max_stream_size_mb,
    )
    assert policy.app_env == app_env
    assert policy.debug is debug
    assert policy.secret_key == secret_key
    assert policy.log_level == log_level
    assert policy.uspto_grants_api_endpoint == uspto_grants_api_endpoint
    assert policy.uspto_apps_api_endpoint == uspto_apps_api_endpoint
    assert policy.uspto_user_agent == uspto_user_agent
    assert policy.dlt_pipeline_name == dlt_pipeline_name
    assert policy.dlt_dataset_name == dlt_dataset_name
    assert policy.silver_schema == silver_schema
    assert policy.gold_schema == gold_schema
    assert policy.pghost == pghost
    assert policy.pgport == pgport
    assert policy.pguser == pguser
    assert policy.pgpassword == pgpassword
    assert policy.pgdatabase == pgdatabase
    assert policy.coreason_entity_namespace == coreason_entity_namespace
    assert policy.uspto_stream_chunk_size == uspto_stream_chunk_size
    assert policy.uspto_max_memory_mb == uspto_max_memory_mb
    assert policy.uspto_http_timeout == uspto_http_timeout
    assert policy.uspto_max_retries == uspto_max_retries
    assert policy.uspto_max_stream_size_mb == uspto_max_stream_size_mb


def test_invalid_app_env() -> None:
    with pytest.raises(ValidationError):
        FederatedEnvironmentPolicy(app_env="staging")


def test_invalid_log_level() -> None:
    with pytest.raises(ValidationError):
        FederatedEnvironmentPolicy(log_level="TRACE")
