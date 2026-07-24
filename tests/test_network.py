# Copyright (c) 2026 CoReason, Inc.
import pytest
import requests
import responses
from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.utils.network import establish_epistemic_network_policy

def test_establish_epistemic_network_policy() -> None:
    policy = FederatedEnvironmentPolicy(uspto_user_agent="Test-Agent-X")
    session = establish_epistemic_network_policy(policy)
    assert session.headers.get("User-Agent") == "Test-Agent-X"

@responses.activate
def test_network_policy_retries() -> None:
    policy = FederatedEnvironmentPolicy(uspto_user_agent="Test-Agent-X", uspto_max_retries=2)
    session = establish_epistemic_network_policy(policy)
    url = "https://data.uspto.gov/test"
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, json={"status": "success"}, status=200)
    response = session.get(url)
    assert response.status_code == 200
    assert len(responses.calls) == 3

@responses.activate
def test_network_policy_max_retries_exceeded() -> None:
    policy = FederatedEnvironmentPolicy(uspto_user_agent="Test-Agent-X", uspto_max_retries=1)
    session = establish_epistemic_network_policy(policy)
    url = "https://data.uspto.gov/test-fail"
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)
    with pytest.raises(requests.exceptions.RetryError):
        session.get(url)
    assert len(responses.calls) == 2
