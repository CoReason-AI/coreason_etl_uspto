# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

"""
AGENT INSTRUCTION: This module provides the Discovery Service for fetching USPTO dataset links.
"""

import requests

from coreason_etl_uspto.utils.logger import logger


def fetch_zip_links(discovery_url: str, session: requests.Session) -> list[str]:
    """
    AGENT INSTRUCTION: This function queries the parameterized USPTO API endpoint to list ZIP files.
    It parses the JSON response to extract the actual .zip download links for the targeted date range.

    Args:
        discovery_url: The parameterized URL to query.
        session: The configured requests.Session to use.

    Returns:
        A list of string URLs pointing to the ZIP files.
    """
    logger.info(f"Fetching ZIP links from discovery URL: {discovery_url}")
    response = session.get(discovery_url)
    response.raise_for_status()

    data = response.json()

    # Assuming the USPTO API returns a JSON structure containing results with fileDownloadUrl
    zip_links = [
        result["fileDownloadUrl"]
        for result in data.get("results", [])
        if "fileDownloadUrl" in result and result["fileDownloadUrl"].endswith(".zip")
    ]

    logger.info(f"Discovered {len(zip_links)} ZIP links")
    return zip_links
