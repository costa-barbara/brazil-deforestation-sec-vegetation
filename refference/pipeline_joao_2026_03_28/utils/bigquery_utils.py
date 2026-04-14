#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQuery utilities for managing tables and schemas.

Provides functions to:
- Check if tables exist
- Create tables with proper schemas
- List tables in a dataset
"""

from google.cloud import bigquery
from typing import Optional, Dict, List


def get_client(project: Optional[str] = None) -> bigquery.Client:
    """Get a BigQuery client instance."""
    return bigquery.Client(project=project)


def table_exists(table_id: str, client: Optional[bigquery.Client] = None) -> bool:
    """
    Check if a BigQuery table exists.

    Args:
        table_id (str): Full table ID in format 'project.dataset.table'
        client (bigquery.Client): Optional BigQuery client. If None, creates a new one.

    Returns:
        bool: True if table exists, False otherwise
    """
    if client is None:
        client = get_client()

    try:
        client.get_table(table_id)
        return True
    except Exception:
        return False


def ensure_dataset_exists(dataset_id: str, project: Optional[str] = None,
                          location: str = "US", client: Optional[bigquery.Client] = None) -> str:
    """
    Ensure a dataset exists, creating it if necessary.

    Args:
        dataset_id (str): Dataset ID (e.g., 'mapbiomas_brazil_statistics')
        project (str): Project ID. If None, uses client's default.
        location (str): Dataset location (default: "US")
        client (bigquery.Client): Optional BigQuery client. If None, creates a new one.

    Returns:
        str: Full dataset path in format 'project.dataset'
    """
    if client is None:
        client = get_client(project)

    if project is None:
        project = client.project

    full_dataset_id = f"{project}.{dataset_id}"
    dataset = bigquery.Dataset(full_dataset_id)
    dataset.location = location

    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"✅ Dataset {full_dataset_id} verified/created")
        return full_dataset_id
    except Exception as e:
        print(f"❌ Error with dataset {full_dataset_id}: {e}")
        raise


def create_table(table_id: str, schema: List[bigquery.SchemaField],
                 description: str = "", client: Optional[bigquery.Client] = None) -> bigquery.Table:
    """
    Create a BigQuery table with the given schema.

    Args:
        table_id (str): Full table ID in format 'project.dataset.table'
        schema (list): List of bigquery.SchemaField objects
        description (str): Optional table description
        client (bigquery.Client): Optional BigQuery client. If None, creates a new one.

    Returns:
        bigquery.Table: The created or existing table
    """
    if client is None:
        client = get_client()

    table = bigquery.Table(table_id, schema=schema)
    if description:
        table.description = description

    try:
        table = client.create_table(table, exists_ok=True)
        print(f"✅ Table {table_id} verified/created")
        return table
    except Exception as e:
        print(f"❌ Error creating table {table_id}: {e}")
        raise


def ensure_table_exists(table_id: str, schema: List[bigquery.SchemaField],
                       description: str = "", client: Optional[bigquery.Client] = None) -> bool:
    """
    Ensure a BigQuery table exists, creating it with the given schema if necessary.

    Args:
        table_id (str): Full table ID in format 'project.dataset.table'
        schema (list): List of bigquery.SchemaField objects
        description (str): Optional table description
        client (bigquery.Client): Optional BigQuery client. If None, creates a new one.

    Returns:
        bool: True if table exists or was created successfully
    """
    if client is None:
        client = get_client()

    if table_exists(table_id, client):
        print(f"✅ Table {table_id} already exists")
        return True

    try:
        create_table(table_id, schema, description, client)
        return True
    except Exception as e:
        print(f"❌ Failed to ensure table {table_id}: {e}")
        return False


def list_tables(dataset_id: str, project: Optional[str] = None,
                client: Optional[bigquery.Client] = None) -> List[str]:
    """
    List all tables in a dataset.

    Args:
        dataset_id (str): Dataset ID (e.g., 'mapbiomas_brazil_statistics')
        project (str): Project ID. If None, uses client's default.
        client (bigquery.Client): Optional BigQuery client. If None, creates a new one.

    Returns:
        list: List of table IDs
    """
    if client is None:
        client = get_client(project)

    if project is None:
        project = client.project

    full_dataset_id = f"{project}.{dataset_id}"
    tables = []

    try:
        for table in client.list_tables(full_dataset_id):
            tables.append(table.table_id)
    except Exception as e:
        print(f"❌ Error listing tables in {full_dataset_id}: {e}")
        return []

    return tables


def deduplicate_table(
    table_id: str,
    partition_by: List[str],
    client: Optional[bigquery.Client] = None,
    order_by: Optional[str] = None,
) -> Dict[str, int]:
    """
    Remove duplicate rows from a BigQuery table in-place using ROW_NUMBER().

    Uses CREATE OR REPLACE TABLE ... WHERE rn = 1 — the standard BigQuery
    deduplication pattern. Idempotent: running twice returns removed=0.

    Args:
        table_id:     Full table ID in format 'project.dataset.table'
        partition_by: Columns that define a unique row (use get_unique_columns)
        client:       Optional BigQuery client. If None, creates a new one.
        order_by:     Optional ORDER BY clause. Default: 'area' for statistics tables.
                      Use empty string '' for tables without area column.

    Returns:
        Dict with keys 'before', 'after', 'removed' (all int)
    """
    if client is None:
        client = get_client()

    if order_by is None:
        order_by = "area"  # Default for statistics tables

    print(f"⏳ Counting rows in {table_id} before deduplication...")
    total_before = next(client.query(f"SELECT COUNT(*) AS n FROM `{table_id}`").result()).n
    print(f"   Rows before: {total_before:,}")

    partition_cols = ", ".join(partition_by)
    order_clause = f"ORDER BY {order_by}" if order_by else ""

    dedup_query = f"""
    CREATE OR REPLACE TABLE `{table_id}` AS
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY {partition_cols}
          {order_clause}
        ) AS rn
      FROM `{table_id}`
    )
    WHERE rn = 1
    """

    print(f"⏳ Running deduplication on {table_id}...")
    try:
        client.query(dedup_query).result()
        print(f"✅ Deduplication completed for {table_id}")
    except Exception as e:
        print(f"❌ Deduplication failed for {table_id}: {e}")
        raise

    total_after = next(client.query(f"SELECT COUNT(*) AS n FROM `{table_id}`").result()).n
    removed = total_before - total_after
    print(f"   Rows after:  {total_after:,}  (removed {removed:,} duplicates)\n")

    return {"before": total_before, "after": total_after, "removed": removed}


def get_schema_water_annual() -> List[bigquery.SchemaField]:
    """Get schema for water_annual table."""
    return [
        bigquery.SchemaField("feature_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("category_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("class", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("area", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("collection", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
    ]


def get_schema_water_monthly() -> List[bigquery.SchemaField]:
    """Get schema for water_monthly table."""
    return [
        bigquery.SchemaField("feature_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("category_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("month", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("class", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("area", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("collection", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
    ]


def get_schema_coverage() -> List[bigquery.SchemaField]:
    """Get schema for coverage table."""
    return [
        bigquery.SchemaField("feature_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("class", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("area", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("collection", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
    ]


def get_schema_deforestation() -> List[bigquery.SchemaField]:
    """Get schema for deforestation_annual table."""
    return [
        bigquery.SchemaField("feature_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("transition", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("class", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("area", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("collection", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
    ]


def get_schema_transitions() -> List[bigquery.SchemaField]:
    """Get schema for transitions table."""
    return [
        bigquery.SchemaField("feature_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("year_from", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("year_to", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("class_from", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("class_to", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("area", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("collection", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
    ]


def get_schema_territories() -> List[bigquery.SchemaField]:
    """Get schema for territories table."""
    return [
        bigquery.SchemaField("CATEGORY", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CATEG_ID", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("FEATURE_ID", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("GEOCODE", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("LEVEL_1", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("LEVEL_2", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("LEVEL_3", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("LEVEL_4", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("NAME", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("NAME_STD", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SOURCE", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("VERSION", "INTEGER", mode="NULLABLE"),
    ]


def get_unique_columns(schema: List[bigquery.SchemaField]) -> List[str]:
    """
    Return column names from a schema excluding 'area'.

    'area' is the only measure column in all statistics tables; all other
    fields form the unique key for deduplication purposes.

    Args:
        schema: List of bigquery.SchemaField (from any get_schema_*())

    Returns:
        List of column names in schema declaration order
    """
    return [field.name for field in schema if field.name != "area"]
