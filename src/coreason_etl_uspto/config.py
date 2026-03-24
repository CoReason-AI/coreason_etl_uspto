# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FederatedEnvironmentPolicy(BaseSettings):
    """
    AGENT INSTRUCTION: This class defines the mathematical boundaries of the environment configuration.
    It represents the immutable historical facts of the system configuration.
    """

    app_env: Literal["development", "testing", "production"] = Field(
        default="development", description="The environment stage the application is currently running in."
    )
    debug: bool = Field(default=False, description="Whether debug mode is enabled.")
    secret_key: str = Field(default="secret", description="Cryptographic key for signing or sessions.")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", description="The verbosity level for the centralized logging architecture."
    )

    uspto_grants_api_endpoint: str = Field(
        default="https://data.uspto.gov/bulkdata/datasets/ptgrxml",
        description="USPTO API endpoint for USPTO Grants (Red Book) bulk datasets.",
    )

    uspto_apps_api_endpoint: str = Field(
        default="https://data.uspto.gov/bulkdata/datasets/pba",
        description="USPTO API endpoint for USPTO Applications (Yellow Book) bulk datasets.",
    )

    uspto_user_agent: str = Field(
        default="CoReason-Bot (contact: admin@coreason.com)",
        description="User-Agent for USPTO Open Data Portal Endpoint",
    )

    dlt_pipeline_name: str = Field(
        default="uspto_pipeline",
        description="The name of the dlt pipeline.",
    )

    dlt_dataset_name: str = Field(
        default="bronze",
        description="The dataset name (schema) for the Bronze layer.",
    )

    silver_schema: str = Field(
        default="silver",
        description="The dataset name (schema) for the Silver layer.",
    )

    gold_schema: str = Field(
        default="gold",
        description="The dataset name (schema) for the Gold layer.",
    )

    pghost: str = Field(default="localhost", description="PostgreSQL host.")
    pgport: int = Field(default=5432, description="PostgreSQL port.")
    pguser: str = Field(default="postgres", description="PostgreSQL user.")
    pgpassword: str = Field(default="postgres", description="PostgreSQL password.")
    pgdatabase: str = Field(default="coreason_etl_uspto", description="PostgreSQL database.")

    coreason_entity_namespace: str = Field(
        default="1b671a64-40d5-491e-99b0-da01ff1f3341",
        description="UUID Namespace for deterministic entity hashing (UUID5).",
    )

    uspto_stream_chunk_size: int = Field(
        default=8192,
        description="Chunk size in bytes for reading HTTP stream. Must be deterministic.",
    )

    uspto_max_memory_mb: int = Field(
        default=10,
        description="Maximum memory in MB to allocate before spilling to disk for zip decompression.",
    )

    uspto_http_timeout: int = Field(
        default=60,
        description="HTTP request timeout in seconds.",
    )

    uspto_max_retries: int = Field(
        default=3,
        description="Maximum number of HTTP retries for resilient requests.",
    )

    uspto_max_stream_size_mb: int = Field(
        default=5120,
        description="Maximum expected size for streaming ZIP files in MB.",
    )

    @property
    def get_postgres_uri(self) -> str:
        """
        AGENT INSTRUCTION: Constructs a strictly valid, URL-encoded PostgreSQL connection URI.
        """
        import urllib.parse

        encoded_password = urllib.parse.quote_plus(self.pgpassword)
        encoded_user = urllib.parse.quote_plus(self.pguser)
        return f"postgresql://{encoded_user}:{encoded_password}@{self.pghost}:{self.pgport}/{self.pgdatabase}"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
