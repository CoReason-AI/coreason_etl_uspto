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
AGENT INSTRUCTION: This module provides the Bronze Layer data ingestion resources using dlt.
"""

from collections.abc import Iterator
from typing import Any

import dlt

from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.services.discovery import fetch_zip_links
from coreason_etl_uspto.services.ingestion import parse_uspto_stream, stream_uspto_zip
from coreason_etl_uspto.utils.logger import logger
from coreason_etl_uspto.utils.network import establish_epistemic_network_policy


@dlt.resource(write_disposition="append", primary_key="file_id")  # type: ignore
def uspto_grants(start_date: str = "2024-01-01", end_date: str = "2024-12-31") -> Iterator[Any]:
    """
    AGENT INSTRUCTION: This generator streams weekly USPTO Red Book (Grants) files.
    """
    logger.info(f"Starting USPTO Grants ingestion from {start_date} to {end_date}")
    policy = FederatedEnvironmentPolicy()
    session = establish_epistemic_network_policy(policy)

    discovery_url = f"{policy.uspto_grants_api_endpoint}?fileDataFromDate={start_date}&fileDataToDate={end_date}"
    all_files = fetch_zip_links(discovery_url, session)

    state = dlt.current.resource_state()
    processed_files = state.setdefault("processed_files", [])

    new_files = [f for f in all_files if f not in processed_files]
    logger.info(f"Found {len(new_files)} new grant files to process")

    for url in new_files:
        try:
            stream = stream_uspto_zip(url, session, policy)
            # Enumerate to get an index for our deterministic primary key
            for i, doc in enumerate(parse_uspto_stream(stream, tag="us-patent-grant", source_url=url)):
                doc["file_id"] = f"{url}_{i}"  # <--- INJECTING THE PRIMARY KEY
                
                if doc.get("_error"):
                    yield dlt.mark.with_table_name(doc, "coreason_etl_uspto_bronze_grants_error")
                    continue

                yield dlt.mark.with_table_name(doc, "coreason_etl_uspto_bronze_grants")

            processed_files.append(url)

        except Exception as e:
            logger.error(f"Failed to process file {url}: {e}")
            yield dlt.mark.with_table_name(
                {"file_id": f"{url}_error", "_error": True, "error_message": str(e), "source_url": url, "context": "file_level"},
                "coreason_etl_uspto_bronze_grants_error",
            )


@dlt.resource(write_disposition="append", primary_key="file_id")  # type: ignore
def uspto_applications(start_date: str = "2024-01-01", end_date: str = "2024-12-31") -> Iterator[Any]:
    """
    AGENT INSTRUCTION: This generator streams weekly USPTO Yellow Book (Applications) files.
    """
    logger.info(f"Starting USPTO Applications ingestion from {start_date} to {end_date}")
    policy = FederatedEnvironmentPolicy()
    session = establish_epistemic_network_policy(policy)

    discovery_url = f"{policy.uspto_apps_api_endpoint}?fileDataFromDate={start_date}&fileDataToDate={end_date}"
    all_files = fetch_zip_links(discovery_url, session)

    state = dlt.current.resource_state()
    processed_files = state.setdefault("processed_files", [])

    new_files = [f for f in all_files if f not in processed_files]
    logger.info(f"Found {len(new_files)} new application files to process")

    for url in new_files:
        try:
            stream = stream_uspto_zip(url, session, policy)
            for i, doc in enumerate(parse_uspto_stream(stream, tag="us-patent-application", source_url=url)):
                doc["file_id"] = f"{url}_{i}"  # <--- INJECTING THE PRIMARY KEY
                
                if doc.get("_error"):
                    yield dlt.mark.with_table_name(doc, "coreason_etl_uspto_bronze_applications_error")
                    continue

                yield dlt.mark.with_table_name(doc, "coreason_etl_uspto_bronze_applications")

            processed_files.append(url)

        except Exception as e:
            logger.error(f"Failed to process file {url}: {e}")
            yield dlt.mark.with_table_name(
                {"file_id": f"{url}_error", "_error": True, "error_message": str(e), "source_url": url, "context": "file_level"},
                "coreason_etl_uspto_bronze_applications_error",
            )


@dlt.source  # type: ignore
def uspto_bulk(start_date: str = "2024-01-01", end_date: str = "2024-12-31") -> Any:
    """
    AGENT INSTRUCTION: This source orchestrates the USPTO pipeline fetching both Grants and Applications.
    """
    return [
        uspto_grants(start_date=start_date, end_date=end_date),
        uspto_applications(start_date=start_date, end_date=end_date),
    ]
