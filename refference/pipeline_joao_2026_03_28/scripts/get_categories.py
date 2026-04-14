#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script: get_categories.py
Purpose: Query BigQuery to fetch all territory categories for statistics exports
Usage: python scripts/get_categories.py

Output: Prints list of categories as Python dict format (ready to copy/paste into pipeline)
"""

from google.cloud import bigquery

def get_categories():
    """
    Consulta BigQuery para obter todas as categorias de território.

    Query a tabela `mapbiomas.mapbiomas_brazil_statistics.territories` para listar
    todas as CATEGORY + CATEG_ID únicas, ordenadas por CATEG_ID.

    Returns:
        list: Lista de dicts com chaves 'CATEGORY' e 'CATEG_ID'
    """
    client = bigquery.Client(project="mapbiomas-brazil")

    query = """
        SELECT DISTINCT territories.CATEGORY, territories.CATEG_ID
        FROM `mapbiomas.mapbiomas_brazil_statistics.territories` AS territories
        ORDER BY CATEG_ID
    """

    print("🔄 Querying BigQuery for territory categories...")
    rows = client.query(query).result()

    categories = [{"CATEGORY": row.CATEGORY, "CATEG_ID": str(row.CATEG_ID)} for row in rows]

    return categories


if __name__ == "__main__":
    try:
        categories = get_categories()

        if not categories:
            print("❌ No categories found in territories table.")
            exit(1)

        print(f"\n✅ Found {len(categories)} categories:\n")

        # Print as Python dict list (ready to copy into pipeline)
        print("WATER_CATEGORIES = [")
        for cat in categories:
            print(f'    {{"CATEGORY": "{cat["CATEGORY"]}", "CATEG_ID": "{cat["CATEG_ID"]}"}},')
        print("]")

        print(f"\n📊 Summary:")
        print(f"   Total categories: {len(categories)}")
        print(f"   First: {categories[0]['CATEGORY']} (ID: {categories[0]['CATEG_ID']})")
        print(f"   Last:  {categories[-1]['CATEGORY']} (ID: {categories[-1]['CATEG_ID']})")

    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)
