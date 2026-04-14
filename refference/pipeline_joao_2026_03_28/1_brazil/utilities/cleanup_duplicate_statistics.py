#!/usr/bin/env python3
"""
Deduplication script for all Brazil statistics tables in BigQuery.

Tables covered:
  - coverage
  - deforestation_annual
  - transitions
  - water_annual
  - water_monthly

Background:
  The water_annual table received duplicate records because check_existing_entries
  had a version mismatch bug (queried "9-1.1" but records stored with version "v1").
  This caused the export to run 3 times without skipping, inserting 2 extra copies.
  The bug has been fixed; this script cleans up all statistics tables to prevent
  future inconsistencies.

Idempotent: running twice on a clean table returns removed=0 for all tables.

Runtime:
  /Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python
"""

import sys
sys.path.insert(0, '/Users/joaosiqueira/Documents/Projects/mapbiomas-pipeline')

from utils.bigquery_utils import (
    get_client,
    deduplicate_table,
    get_unique_columns,
    get_schema_coverage,
    get_schema_deforestation,
    get_schema_transitions,
    get_schema_water_annual,
    get_schema_water_monthly,
)

PROJECT = "mapbiomas"
REGION  = "brazil"
DATASET = f"mapbiomas_{REGION}_statistics"

TABLE_SCHEMAS = {
    f"{PROJECT}.{DATASET}.coverage":             get_schema_coverage,
    f"{PROJECT}.{DATASET}.deforestation_annual": get_schema_deforestation,
    f"{PROJECT}.{DATASET}.transitions":          get_schema_transitions,
    f"{PROJECT}.{DATASET}.water_annual":         get_schema_water_annual,
    f"{PROJECT}.{DATASET}.water_monthly":        get_schema_water_monthly,
}


def main():
    client = get_client(project=PROJECT)
    totals = {"before": 0, "after": 0, "removed": 0}

    for table_id, schema_fn in TABLE_SCHEMAS.items():
        partition_by = get_unique_columns(schema_fn())
        result = deduplicate_table(table_id, partition_by=partition_by, client=client)
        for k in totals:
            totals[k] += result[k]

    print("=" * 80)
    print("=== SUMMARY ===")
    print(f"Tables processed: {len(TABLE_SCHEMAS)}")
    print(f"Total rows before: {totals['before']:,}")
    print(f"Total rows after:  {totals['after']:,}")
    print(f"Total removed:     {totals['removed']:,} duplicates")

    if totals["removed"] > 0:
        print("✅ Deduplication successful")
    else:
        print("ℹ️  No duplicates found (already clean)")


if __name__ == "__main__":
    main()
