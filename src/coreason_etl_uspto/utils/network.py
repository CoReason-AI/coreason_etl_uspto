# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.utils.logger import logger


def establish_epistemic_network_policy(env_policy: FederatedEnvironmentPolicy) -> requests.Session:
    """
    AGENT INSTRUCTION: This function defines the mathematical boundaries for external network topologies.
    It returns a rigorously configured requests.Session bound by the FederatedEnvironmentPolicy.
    """
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
