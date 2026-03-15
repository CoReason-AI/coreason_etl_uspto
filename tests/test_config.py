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
    assert policy.uspto_grants_api_endpoint == "https://data.uspto.gov/api/v1/datasets/products/ptgrxml"
    assert policy.uspto_apps_api_endpoint == "https://data.uspto.gov/api/v1/datasets/products/pba"
    assert policy.uspto_user_agent == "CoReason-Bot (contact: admin@coreason.com)"
    assert policy.pghost == "localhost"
    assert policy.pgport == 5432
    assert policy.pguser == "postgres"
    assert policy.pgpassword == "postgres"
    assert policy.pgdatabase == "coreason_etl_uspto"


@given(
    app_env=st.sampled_from(["development", "testing", "production"]),
    debug=st.booleans(),
    secret_key=st.text(min_size=1),
    log_level=st.sampled_from(["DEBUG", "INFO", "WARNING", "ERROR"]),
    uspto_grants_api_endpoint=st.text(min_size=1),
    uspto_apps_api_endpoint=st.text(min_size=1),
    uspto_user_agent=st.text(min_size=1),
    pghost=st.text(min_size=1),
    pgport=st.integers(min_value=1, max_value=65535),
    pguser=st.text(min_size=1),
    pgpassword=st.text(min_size=1),
    pgdatabase=st.text(min_size=1),
)
def test_federated_environment_policy_valid(
    app_env: str,
    debug: bool,
    secret_key: str,
    log_level: str,
    uspto_grants_api_endpoint: str,
    uspto_apps_api_endpoint: str,
    uspto_user_agent: str,
    pghost: str,
    pgport: int,
    pguser: str,
    pgpassword: str,
    pgdatabase: str,
) -> None:
    policy = FederatedEnvironmentPolicy(
        app_env=app_env,
        debug=debug,
        secret_key=secret_key,
        log_level=log_level,
        uspto_grants_api_endpoint=uspto_grants_api_endpoint,
        uspto_apps_api_endpoint=uspto_apps_api_endpoint,
        uspto_user_agent=uspto_user_agent,
        pghost=pghost,
        pgport=pgport,
        pguser=pguser,
        pgpassword=pgpassword,
        pgdatabase=pgdatabase,
    )
    assert policy.app_env == app_env
    assert policy.debug is debug
    assert policy.secret_key == secret_key
    assert policy.log_level == log_level
    assert policy.uspto_grants_api_endpoint == uspto_grants_api_endpoint
    assert policy.uspto_apps_api_endpoint == uspto_apps_api_endpoint
    assert policy.uspto_user_agent == uspto_user_agent
    assert policy.pghost == pghost
    assert policy.pgport == pgport
    assert policy.pguser == pguser
    assert policy.pgpassword == pgpassword
    assert policy.pgdatabase == pgdatabase


def test_invalid_app_env() -> None:
    with pytest.raises(ValidationError):
        FederatedEnvironmentPolicy(app_env="staging")


def test_invalid_log_level() -> None:
    with pytest.raises(ValidationError):
        FederatedEnvironmentPolicy(log_level="TRACE")
