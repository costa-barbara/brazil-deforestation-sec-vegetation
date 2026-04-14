#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exporta coleções de territórios do Brasil para BigQuery via Earth Engine.
"""

import ee
import sys

sys.path.insert(0, "/Users/joaosiqueira/Documents/Projects/mapbiomas-pipeline")

from utils.export_utils import resolve_territory_asset, wait_until_tasks_finish

ASSET_FOLDER = "projects/mapbiomas-territories/assets/TERRITORIES/BRAZIL/WORKSPACE"
TABLE = "mapbiomas.mapbiomas_brazil_statistics.territories_c10_1"
DEFAULT_VERSION = "v1"
POLLING_INTERVAL = 30

ASSETS = [
    {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
    {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
    {"CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": "3"},
    {"CATEGORY": "BIOMES", "CATEG_ID": "4"},
    {"CATEGORY": "BASIN_LEVEL_1_PNRH", "CATEG_ID": "10"},
    {"CATEGORY": "BASIN_LEVEL_2_PNRH", "CATEG_ID": "11"},
    {"CATEGORY": "COASTAL_MARINE_ZONE", "CATEG_ID": "5"},
    {"CATEGORY": "BASIN_LEVEL_1_DNAEE", "CATEG_ID": "12"},
    {"CATEGORY": "BASIN_LEVEL_2_DNAEE", "CATEG_ID": "13"},
    {"CATEGORY": "UGRHS", "CATEG_ID": "14"},
    {"CATEGORY": "DHN250_LEVEL_1", "CATEG_ID": "15"},
    {"CATEGORY": "DHN250_LEVEL_2", "CATEG_ID": "16"},
    {"CATEGORY": "DHN250_LEVEL_3", "CATEG_ID": "17"},
    {"CATEGORY": "AMACRO", "CATEG_ID": "20"},
    {"CATEGORY": "ATLANTIC_FOREST_LAW", "CATEG_ID": "21"},
    {"CATEGORY": "LEGAL_AMAZON", "CATEG_ID": "22"},
    {"CATEGORY": "SEMIARID", "CATEG_ID": "23"},
    {"CATEGORY": "MATOPIBA", "CATEG_ID": "24"},
    {"CATEGORY": "FLORESTAS_PUBLICAS_NAO_DESTINADAS", "CATEG_ID": "25"},
    {"CATEGORY": "AREAS_PRIORITARIAS_DO_MMA_2018", "CATEG_ID": "40"},
    {"CATEGORY": "GEOPARQUES", "CATEG_ID": "41"},
    {"CATEGORY": "RESERVA_DA_BIOSFERA", "CATEG_ID": "42"},
    {"CATEGORY": "PROTECTED_AREA", "CATEG_ID": "60"},
    {"CATEGORY": "INDIGENOUS_TERRITORIES", "CATEG_ID": "61"},
    {"CATEGORY": "QUILOMBOS", "CATEG_ID": "62"},
    {"CATEGORY": "SETTLEMENTS", "CATEG_ID": "63"},
    {"CATEGORY": "CONCESSOES_FLORESTAIS", "CATEG_ID": "71"},
    {"CATEGORY": "METROPOLITAN_REGIONS", "CATEG_ID": "91"},
    {"CATEGORY": "POPULATION_ARRANGEMENT", "CATEG_ID": "92"},
    {"CATEGORY": "URBAN_CONCENTRATION", "CATEG_ID": "93"},
    {"CATEGORY": "URBAN_PERIMETER", "CATEG_ID": "94"},
]

SELECTORS = [
    "CATEGORY",
    "CATEG_ID",
    "FEATURE_ID",
    "GEOCODE",
    "LEVEL_1",
    "LEVEL_2",
    "LEVEL_3",
    "LEVEL_4",
    "NAME",
    "NAME_STD",
    "SOURCE",
    "VERSION",
]


def map_feature(feature, category, categ_id):
    # Handle potentially missing or null GEOCODE property
    geocode = ee.Algorithms.If(
        feature.propertyNames().contains("GEOCODE"),
        ee.Algorithms.If(
            feature.get("GEOCODE"),
            ee.String(feature.get("GEOCODE")),
            ""
        ),
        ""
    )

    # Handle potentially missing VERSION property (including typo VESION)
    version_value = ee.Algorithms.If(
        feature.propertyNames().contains("VERSION"),
        ee.Number.parse(ee.String(feature.get("VERSION"))),
        ee.Algorithms.If(
            feature.propertyNames().contains("VESION"),
            ee.Number.parse(ee.String(feature.get("VESION"))),
            1
        )
    )

    return (
        feature
        .set("GEOCODE", geocode)
        .set("VERSION", version_value)
        .set("CATEGORY", category)
        .set("CATEG_ID", int(categ_id))
    )


def export_category(category_obj):
    category = category_obj["CATEGORY"]
    categ_id = category_obj["CATEG_ID"]
    asset_id = resolve_territory_asset(
        f"{ASSET_FOLDER}/{category}/{category}_{DEFAULT_VERSION}"
    )

    collection = ee.FeatureCollection(asset_id).map(
        lambda f: map_feature(f, category, categ_id)
    )

    print(f"{category}: {collection.size().getInfo()} feições")
    # Omit detailed feature dump to avoid massive terminal output

    description = f"territories_{category.lower()}"[:99]
    task = ee.batch.Export.table.toBigQuery(
        collection=collection,
        description=description,
        table=TABLE,
        overwrite=False,
        append=True,
        selectors=SELECTORS,
    )
    task.start()
    print(f"✅ Task enviada: {description}")
    return task


def main():
    ee.Initialize(project="mapbiomas-brazil")
    export_tasks = []

    for category_obj in ASSETS:
        task = export_category(category_obj)
        if task:
            export_tasks.append(task)

    wait_until_tasks_finish(
        export_tasks=export_tasks,
        polling_interval=POLLING_INTERVAL,
    )


if __name__ == "__main__":
    main()
