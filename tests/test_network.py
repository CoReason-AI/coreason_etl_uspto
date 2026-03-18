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
import requests
import responses
from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.utils.network import establish_epistemic_network_policy


def test_establish_epistemic_network_policy() -> None:
    policy = FederatedEnvironmentPolicy(uspto_user_agent="Test-Agent-X")
    session = establish_epistemic_network_policy(policy)
    assert session.headers.get("User-Agent") == "Test-Agent-X"

    adapter = session.get_adapter("https://data.uspto.gov")
    from requests.adapters import HTTPAdapter

    assert isinstance(adapter, HTTPAdapter)
    assert adapter.max_retries.total == policy.uspto_max_retries  # type: ignore
    assert adapter.max_retries.backoff_factor == 1  # type: ignore
    assert list(adapter.max_retries.status_forcelist) == [429, 500, 502, 503, 504]  # type: ignore
    assert list(adapter.max_retries.allowed_methods) == ["HEAD", "GET", "OPTIONS"]  # type: ignore


@responses.activate  # type: ignore
def test_network_policy_retries() -> None:
    policy = FederatedEnvironmentPolicy(uspto_user_agent="Test-Agent-X", uspto_max_retries=2)
    session = establish_epistemic_network_policy(policy)

    url = "https://data.uspto.gov/test"

    # Mock to return 500 Internal Server Error twice, then 200 OK
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, json={"status": "success"}, status=200)

    response = session.get(url)
    assert response.status_code == 200
    assert response.json() == {"status": "success"}
    assert len(responses.calls) == 3


@responses.activate  # type: ignore
def test_network_policy_max_retries_exceeded() -> None:
    policy = FederatedEnvironmentPolicy(uspto_user_agent="Test-Agent-X", uspto_max_retries=1)
    session = establish_epistemic_network_policy(policy)

    url = "https://data.uspto.gov/test-fail"

    # Mock to constantly return 500
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)

    with pytest.raises(requests.exceptions.RetryError):
        session.get(url)

    # Should be called 1 (initial) + 1 (retry) = 2 times
    assert len(responses.calls) == 2
