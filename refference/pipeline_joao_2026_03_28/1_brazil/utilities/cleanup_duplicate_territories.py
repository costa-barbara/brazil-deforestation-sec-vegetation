#!/usr/bin/env python3
"""
Deduplication script for territories table in BigQuery.

Table: mapbiomas.mapbiomas_brazil_statistics.territories_c10_1

Background:
  Territory features are exported via GEE to BigQuery. Repeated script execution
  without skipping logic caused duplicate inserts. This script identifies and
  removes exact duplicates, keeping only the first occurrence.

Idempotent: running twice on a clean table returns removed=0.

Runtime:
  /Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python
"""

import sys
sys.path.insert(0, '/Users/joaosiqueira/Documents/Projects/mapbiomas-pipeline')

from utils.bigquery_utils import (
    get_client,
    deduplicate_table,
    get_unique_columns,
    get_schema_territories,
)

PROJECT = "mapbiomas"
REGION  = "brazil"
DATASET = f"mapbiomas_{REGION}_statistics"
TABLE_ID = f"{PROJECT}.{DATASET}.territories_c10_1"


def main():
    client = get_client(project=PROJECT)

    print("=" * 80)
    print("🗑️  Territory Deduplication Script")
    print("=" * 80)

    schema = get_schema_territories()
    partition_by = get_unique_columns(schema)

    print(f"\n📋 Unique key columns: {', '.join(partition_by)}")
    print(f"📊 Table: {TABLE_ID}\n")

    result = deduplicate_table(TABLE_ID, partition_by=partition_by, client=client, order_by="")

    print("=" * 80)
    print("=== SUMMARY ===")
    print(f"Total rows before: {result['before']:,}")
    print(f"Total rows after:  {result['after']:,}")
    print(f"Total removed:     {result['removed']:,} duplicates")

    if result["removed"] > 0:
        print(f"✅ Deduplication successful ({result['removed']/result['before']*100:.1f}% removed)")
    else:
        print("ℹ️  No duplicates found (already clean)")


if __name__ == "__main__":
    main()
