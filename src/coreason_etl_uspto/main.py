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
AGENT INSTRUCTION: This module acts as the CLI Pipeline Orchestrator, tying the completed Bronze
ingestion and Silver refinement layers together into a single executable workflow.
"""

import sys
import json

import dlt
import polars as pl

from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.services.bronze import uspto_bulk
from coreason_etl_uspto.services.silver import normalize_silver
from coreason_etl_uspto.utils.logger import logger


def run_pipeline(start_date: str = "2024-01-01", end_date: str = "2024-12-31") -> None:
    """
    AGENT INSTRUCTION: Entrypoint that connects the Bronze ingestion to the Silver refinement layer.
    It runs the dlt pipeline, then uses Polars to fetch from the destination and normalize.
    """
    logger.info(f"Starting USPTO ETL Pipeline execution for period: {start_date} to {end_date}")

    policy = FederatedEnvironmentPolicy()

    # Define the target PostgreSQL credentials dynamically
    credentials = {
        "drivername": "postgresql",
        "host": policy.pghost,
        "port": policy.pgport,
        "username": policy.pguser,
        "password": policy.pgpassword,
        "database": policy.pgdatabase,
    }

    # 1. Initialize dlt pipeline (Orchestrator Layer -> Bronze Storage)
    pipeline = dlt.pipeline(
        pipeline_name=policy.dlt_pipeline_name,
        destination=dlt.destinations.postgres(credentials),
        dataset_name=policy.dlt_dataset_name,
    )

    # 2. Run Bronze Ingestion (Ingestion Component)
    source = uspto_bulk(start_date=start_date, end_date=end_date)
    load_info = pipeline.run(source)
    logger.info(f"Bronze Ingestion completed. Load Info: {load_info}")

    # 3. Process Silver Layer (Refinery Component)
    try:
        client = pipeline.sql_client()
        with client:
            for base_name in ["grants", "applications"]:
                table_name = f"coreason_etl_uspto_bronze_{base_name}"
                try:
                    _refine_bronze_table(pipeline, policy, table_name, base_name)
                except Exception as e:
                    logger.warning(f"Failed to refine table {table_name}: {e}")

    except Exception as e:
        logger.error(f"Error executing Silver refinement: {e}")
        raise e

    logger.info("USPTO ETL Pipeline execution finished successfully.")


def _refine_bronze_table(
    pipeline: dlt.Pipeline, policy: FederatedEnvironmentPolicy, table_name: str, base_name: str
) -> pl.DataFrame | None:
    """
    AGENT INSTRUCTION: Helper function to fetch a table and refine it.
    Serializes nested Arrow types to JSON strings and dynamically sets the write mode.
    """
    try:
        client = pipeline.sql_client()
        with client:
            query = f'SELECT * FROM "{policy.dlt_dataset_name}"."{table_name}"'  # noqa: S608
            conn = client.native_connection

            df_bronze = pl.read_database(query, connection=conn)

            if df_bronze.is_empty():
                logger.info(f"Table {table_name} is empty.")
                return None

            logger.info(f"Loaded {len(df_bronze)} records from Bronze {table_name}.")

            # Apply silver normalization
            df_silver = normalize_silver(df_bronze)

            # --- Serialize complex Arrow types (Lists/Structs) to JSON strings for ADBC ---
            complex_cols = []
            for col_name, dtype in zip(df_silver.columns, df_silver.dtypes):
                if "List" in str(dtype) or "Struct" in str(dtype):
                    complex_cols.append(col_name)

            if complex_cols:
                df_silver = df_silver.with_columns(
                    [
                        pl.col(c).map_elements(
                            lambda x: json.dumps(x, default=str) if x is not None else None,
                            return_dtype=pl.String
                        )
                        for c in complex_cols
                    ]
                )

            silver_table_name = f"coreason_etl_uspto_silver_{base_name}"
            gold_table_name = f"coreason_etl_uspto_gold_{base_name}"
            uri = policy.get_postgres_uri

            with client.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{policy.silver_schema}"'):
                pass
            with client.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{policy.gold_schema}"'):
                pass

            # --- FIX: Check if the destination tables already exist ---
            with conn.cursor() as cur:
                cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{policy.silver_schema}' AND table_name = '{silver_table_name}')")
                silver_exists = cur.fetchone()[0]
                
                cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{policy.gold_schema}' AND table_name = '{gold_table_name}')")
                gold_exists = cur.fetchone()[0]

            # Write back to PostgreSQL into the silver schema
            logger.info(f"Writing {len(df_silver)} records to {policy.silver_schema}.{silver_table_name}")
            df_silver.write_database(
                table_name=f"{policy.silver_schema}.{silver_table_name}",
                connection=uri,
                if_table_exists="append" if silver_exists else "replace",
                engine="adbc",
            )
            logger.info(f"Refinement of {table_name} into Silver Layer complete.")

            # Create and write to the Gold schema table
            logger.info(f"Writing {len(df_silver)} records to {policy.gold_schema}.{gold_table_name}")
            df_silver.write_database(
                table_name=f"{policy.gold_schema}.{gold_table_name}",
                connection=uri,
                if_table_exists="append" if gold_exists else "replace",
                engine="adbc",
            )
            logger.info(f"Refinement of {table_name} into Gold Layer complete.")

            return df_silver

    except Exception as e:
        logger.error(f"Failed to refine table {table_name}: {e}")
        raise e


def main() -> None:
    """
    AGENT INSTRUCTION: CLI entrypoint.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Run the CoReason USPTO ETL Pipeline.")
    parser.add_argument("--start-date", default="2024-01-01", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", default="2024-12-31", help="End date in YYYY-MM-DD format.")

    args = parser.parse_args()

    try:
        run_pipeline(start_date=args.start_date, end_date=args.end_date)
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
