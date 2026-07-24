# Copyright (c) 2026 CoReason, Inc.
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.utils.logger import logger

def establish_epistemic_network_policy(env_policy: FederatedEnvironmentPolicy) -> requests.Session:
    logger.debug("Establishing EpistemicNetworkPolicy session")
    session = requests.Session()
    session.headers.update({"User-Agent": env_policy.uspto_user_agent})

    retries = Retry(
        total=env_policy.uspto_max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session
