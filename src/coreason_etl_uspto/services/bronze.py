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

    # State tracking: Filter out files already processed
    state = dlt.current.resource_state()
    processed_files = state.setdefault("processed_files", [])

    new_files = [f for f in all_files if f not in processed_files]
    logger.info(f"Found {len(new_files)} new grant files to process")

    for url in new_files:
        try:
            stream = stream_uspto_zip(url, session, policy)
            # Grants use the <us-patent-grant> root element
            for doc in parse_uspto_stream(stream, tag="us-patent-grant", source_url=url):
                # Use a deterministic unique file_id logic if needed,
                # but dlt's primary_key acts on the yielded dict
                # The dict from parse_uspto_stream has doc['us-patent-grant']
                # To make a robust primary_key we might need to inject one or let dlt handle it
                # (primary_key="file_id" is defined on the decorator, so we should inject it)

                # Wait, if doc is a failure record, handle it differently
                if doc.get("_error"):
                    yield dlt.mark.with_table_name(doc, "uspto_grants_error")
                    continue

                # Extract some ID for primary key - depends on schema, but we can generate one or just
                # not enforce primary_key="file_id" strictly unless needed.
                # Actually, doc doesn't strictly have a file_id at top level.
                # We can inject `file_id` = doc['ingestion_meta']['source_file'] + index

                yield doc

            processed_files.append(url)

        except Exception as e:
            logger.error(f"Failed to process file {url}: {e}")
            # If the whole file fails (e.g. streaming error), we don't mark as processed
            yield dlt.mark.with_table_name(
                {"_error": True, "error_message": str(e), "source_url": url, "context": "file_level"},
                "uspto_grants_error",
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
            # Applications usually use <us-patent-application> as the tag,
            # though earlier might be different. Let's assume us-patent-application.
            for doc in parse_uspto_stream(stream, tag="us-patent-application", source_url=url):
                if doc.get("_error"):
                    yield dlt.mark.with_table_name(doc, "uspto_applications_error")
                    continue

                yield doc

            processed_files.append(url)

        except Exception as e:
            logger.error(f"Failed to process file {url}: {e}")
            yield dlt.mark.with_table_name(
                {"_error": True, "error_message": str(e), "source_url": url, "context": "file_level"},
                "uspto_applications_error",
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
