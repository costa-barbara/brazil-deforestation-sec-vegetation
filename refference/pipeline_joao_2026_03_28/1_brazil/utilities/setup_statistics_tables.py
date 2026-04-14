#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Setup script for Brazil statistics pipeline.

- Creates BigQuery dataset and tables
- Verifies territory assets exist
- Displays configuration info

Usage:
    python 1_brazil/setup_statistics_tables.py
"""

import sys
sys.path.insert(0, '/Users/joaosiqueira/Documents/Projects/mapbiomas-pipeline')

from utils.bigquery_utils import (
    ensure_dataset_exists,
    ensure_table_exists,
    get_schema_coverage,
    get_schema_deforestation,
    get_schema_transitions,
    get_schema_water_annual,
    get_schema_water_monthly,
)

# Configuration
PROJECT = "mapbiomas"
REGION = "brazil"
DATASET = "mapbiomas_brazil_statistics"

TABLES = {
    "coverage": get_schema_coverage(),
    "deforestation_annual": get_schema_deforestation(),
    "transitions": get_schema_transitions(),
    "water_annual": get_schema_water_annual(),
    "water_monthly": get_schema_water_monthly(),
}

TERRITORY_ASSETS = {
    'POLITICAL_LEVEL_1': 'projects/mapbiomas-territories/assets/TERRITORIES/BRAZIL/WORKSPACE/POLITICAL_LEVEL_1/POLITICAL_LEVEL_1_v1',
}


def main():
    print("=" * 80)
    print(f"🔧 Setup: Brazil Statistics Pipeline")
    print("=" * 80)

    # 1. Ensure dataset exists
    print(f"\n📊 BigQuery Configuration:")
    print(f"  Project: {PROJECT}")
    print(f"  Dataset: {DATASET}")

    try:
        dataset_id = ensure_dataset_exists(DATASET, project=PROJECT)
        print(f"  ✅ Dataset ready: {dataset_id}")
    except Exception as e:
        print(f"  ❌ Failed to setup dataset: {e}")
        sys.exit(1)

    # 2. Ensure tables exist
    print(f"\n📋 Creating/Verifying Tables:")
    all_ok = True

    for table_name, schema in TABLES.items():
        full_table_id = f"{PROJECT}.{DATASET}.{table_name}"
        success = ensure_table_exists(full_table_id, schema)
        if not success:
            all_ok = False

if __name__ == "__main__":
    main()
