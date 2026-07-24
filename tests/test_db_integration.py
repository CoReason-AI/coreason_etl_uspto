import polars as pl
import pytest
from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.main import run_pipeline

@pytest.fixture(scope="module")
def db_config():
    """Fixture to load the active configuration."""
    return FederatedEnvironmentPolicy()

def test_pipeline_populates_postgres(db_config):
    """
    Integration test to run the pipeline and verify SQL tables 
    in the Bronze, Silver, and Gold schemas.
    """
    # 1. Run the pipeline for a small, restricted date range to limit data volume
    run_pipeline("2024-01-01", "2024-01-02")
    
    uri = db_config.get_postgres_uri
    
    # 2. Verify the Bronze schema (Raw ingestion via dlt)
    bronze_schema = db_config.dlt_dataset_name
    query_bronze = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{bronze_schema}'
    """
    df_bronze = pl.read_database_uri(query=query_bronze, uri=uri)
    assert not df_bronze.is_empty(), f"No tables found in bronze schema '{bronze_schema}'"
    
    # 3. Verify the Silver schema (Normalized data)
    silver_schema = db_config.silver_schema
    query_silver = f"""
        SELECT count(*) as row_count 
        FROM "{silver_schema}"."coreason_etl_uspto_silver_grants"
    """
    try:
        df_silver = pl.read_database_uri(query=query_silver, uri=uri)
        assert df_silver["row_count"][0] > 0, "Silver grants table is empty"
    except Exception as e:
        pytest.fail(f"Failed to query silver schema: {e}")

    # 4. Verify the Gold schema (Business-level aggregations)
    gold_schema = db_config.gold_schema
    query_gold = f"""
        SELECT count(*) as row_count 
        FROM "{gold_schema}"."coreason_etl_uspto_gold_grants"
    """
    try:
        df_gold = pl.read_database_uri(query=query_gold, uri=uri)
        assert df_gold["row_count"][0] > 0, "Gold grants table is empty"
    except Exception as e:
        pytest.fail(f"Failed to query gold schema: {e}")
