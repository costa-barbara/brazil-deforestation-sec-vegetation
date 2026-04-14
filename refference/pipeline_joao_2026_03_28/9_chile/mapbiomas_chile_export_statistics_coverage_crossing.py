# -*- coding: utf-8 -*-
import ee
import sys
import os
from google.cloud import bigquery
from pprint import pprint

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.region_utils import RegionUtils
from utils.export_utils import wait_until_tasks_finish

# ================================
# AUTHENTICATION AND INITIALIZATION
# ================================
REGION = 'chile'

try:
    ee.Initialize(project=f'mapbiomas-{REGION}')
except Exception:
    print("Authenticating Earth Engine...")
    ee.Authenticate()
    ee.Initialize(project=f'mapbiomas-{REGION}')

client = bigquery.Client(project="mapbiomas")

region_utils = RegionUtils()

# ================================
# GLOBAL CONFIGURATION
# ================================
REGION_ISO3    = 'CHL'
COLLECTION_ID  = 2

ASSET_IMAGE = (
    f'projects/mapbiomas-public/assets/{REGION}/lulc'
    f'/collection{str(COLLECTION_ID).replace(".", "_")}'
    f'/mapbiomas_{REGION}_collection{str(COLLECTION_ID).replace(".", "_")}_coverage_v2'
)

ASSET_TERRITORIES = f'projects/mapbiomas-territories/assets/TERRITORIES/{REGION.upper()}/WORKSPACE/territory-collection'

INPUT_VERSION      = 'v2'
STATISTICS_VERSION = f'{INPUT_VERSION}.1'

TABLE_ID        = 'coverage_crossing'
BIG_QUERY_TABLE = f"mapbiomas.mapbiomas_{REGION}_statistics.{TABLE_ID}"

BAND_PATTERN = 'classification_{year}'

REGION_BBOX = region_utils.get_bbox(iso3=REGION_ISO3)

# ================================
# CATEGORIES CROSSING
# ================================
CATEGORIES_CROSSING = [
    # País x Región x Provincia
    # (
    #     {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
    #     {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
    #     {"CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": "3"},
    # ),
    # País x Región x Comuna
    (
        {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
        {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
        {"CATEGORY": "POLITICAL_LEVEL_4", "CATEG_ID": "95"},
    ),
    # País x Provincia x Comuna
    (
        {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
        {"CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": "3"},
        {"CATEGORY": "POLITICAL_LEVEL_4", "CATEG_ID": "95"},
    ),
    # Región x Provincia x Comuna
    (
        {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
        {"CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": "3"},
        {"CATEGORY": "POLITICAL_LEVEL_4", "CATEG_ID": "95"},
    ),
    # País x Región x Ecorregión
    (
        {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
        {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
        {"CATEGORY": "ECORREGION",        "CATEG_ID": "34"},
    ),
    # País x Región x Cuenca Principal
    (
        {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
        {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
        {"CATEGORY": "BASIN_LEVEL_1",     "CATEG_ID": "73"},
    ),
    # País x Región x Sub-cuenca
    (
        {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
        {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
        {"CATEGORY": "BASIN_LEVEL_2",     "CATEG_ID": "74"},
    ),
    # País x Cuenca Principal x Sub-cuenca
    (
        {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
        {"CATEGORY": "BASIN_LEVEL_1",     "CATEG_ID": "73"},
        {"CATEGORY": "BASIN_LEVEL_2",     "CATEG_ID": "74"},
    ),
    # Región x Cuenca Principal x Sub-cuenca
    (
        {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
        {"CATEGORY": "BASIN_LEVEL_1",     "CATEG_ID": "73"},
        {"CATEGORY": "BASIN_LEVEL_2",     "CATEG_ID": "74"},
    ),
    # País x Región x Áreas Protegidas
    (
        {"CATEGORY": "POLITICAL_LEVEL_1",        "CATEG_ID": "1"},
        {"CATEGORY": "POLITICAL_LEVEL_2",        "CATEG_ID": "2"},
        {"CATEGORY": "NATIONAL_PROTECTED_AREAS", "CATEG_ID": "119"},
    ),
    # País x Ecorregión x Cuenca Principal
    (
        {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
        {"CATEGORY": "ECORREGION",        "CATEG_ID": "34"},
        {"CATEGORY": "BASIN_LEVEL_1",     "CATEG_ID": "73"},
    ),
    # País x Ecorregión x Áreas Protegidas
    (
        {"CATEGORY": "POLITICAL_LEVEL_1",        "CATEG_ID": "1"},
        {"CATEGORY": "ECORREGION",               "CATEG_ID": "34"},
        {"CATEGORY": "NATIONAL_PROTECTED_AREAS", "CATEG_ID": "119"},
    ),
    # Región x Ecorregión x Áreas Protegidas
    (
        {"CATEGORY": "POLITICAL_LEVEL_2",        "CATEG_ID": "2"},
        {"CATEGORY": "ECORREGION",               "CATEG_ID": "34"},
        {"CATEGORY": "NATIONAL_PROTECTED_AREAS", "CATEG_ID": "119"},
    ),
    # Región x Ecorregión x Cuenca Principal
    (
        {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
        {"CATEGORY": "ECORREGION",        "CATEG_ID": "34"},
        {"CATEGORY": "BASIN_LEVEL_1",     "CATEG_ID": "73"},
    ),
    # Ecorregión x Cuenca Principal x Sub-cuenca
    (
        {"CATEGORY": "ECORREGION",    "CATEG_ID": "34"},
        {"CATEGORY": "BASIN_LEVEL_1", "CATEG_ID": "73"},
        {"CATEGORY": "BASIN_LEVEL_2", "CATEG_ID": "74"},
    ),
]

YEARS = [
    1999,
    2000, 2001, 2002,
    2003, 2004, 2005,
    2006, 2007, 2008,
    2009, 2010, 2011,
    2012, 2013, 2014,
    2015, 2016, 2017,
    2018, 2019, 2020,
    2021, 2022, 2023,
    2024
]

nTasks = len(CATEGORIES_CROSSING) * len(YEARS)

print('number of tasks: ', nTasks)

PIXEL_AREA = ee.Image.pixelArea().divide(1000000)


# ================================
# HELPER FUNCTIONS
# ================================

def check_existing_years(category1, category2, category3, version, collection, table):
    query = f"""
    SELECT
        version,
        ARRAY_AGG(DISTINCT year) AS years
    FROM
        `{table}`
    WHERE
        category_1 = '{category1}' AND
        category_2 = '{category2}' AND
        category_3 = '{category3}' AND
        version = '{version}' AND
        collection = '{collection}'
    GROUP BY
        version
    ORDER BY
        version;
    """

    query_job = client.query(query)
    results = query_job.result()

    result_dict = {row.version: row.years for row in results}

    if version in result_dict:
        results = result_dict[version]
    else:
        results = []

    pprint(results)
    return results


def convert2table(obj):
    """Convert a reducer output dictionary to a FeatureCollection."""
    obj = ee.Dictionary(obj)

    territory = ee.Number(obj.get('feature_id'))
    classesAndAreas = ee.List(obj.get('groups'))

    def insertClassAndArea(classAndArea):
        classAndArea = ee.Dictionary(classAndArea)
        classId = classAndArea.get('class')
        area = classAndArea.get('sum')

        tableColumns = ee.Feature(None) \
            .set('feature_id_1', territory.divide(100000000).int16()) \
            .set('feature_id_2', territory.divide(10000).floor().mod(10000).int16()) \
            .set('feature_id_3', territory.mod(10000).int16()) \
            .set('class', classId) \
            .set('area', area)

        return tableColumns

    tableRows = classesAndAreas.map(insertClassAndArea)
    return ee.FeatureCollection(ee.List(tableRows))


def calculateArea(image, territory, geometry, year):
    reducer = ee.Reducer.sum().group(1, 'class').group(1, 'feature_id')

    areas = ee.Dictionary(
        PIXEL_AREA
        .addBands(territory)
        .addBands(image)
        .reduceRegion(
            reducer=reducer,
            geometry=geometry,
            scale=30,
            maxPixels=1e12
        ))

    collection = ee.FeatureCollection(
        ee.List(areas.get('groups')).map(convert2table)
    )

    collection = ee.FeatureCollection(collection).flatten()
    collection = collection.map(lambda feature: feature.set('year', int(year)))

    return collection


def calculateAreaUsingRaster(image, theme, year, geometry):
    areas = calculateArea(image, theme, geometry, year)
    return ee.FeatureCollection(areas)


def export(areas, name, table):
    task = ee.batch.Export.table.toBigQuery(
        collection=areas,
        description=name[0:99],
        table=table,
        overwrite=False,
        append=True,
        selectors=[
            'feature_id_1',
            'feature_id_2',
            'feature_id_3',
            'category_id_1',
            'category_id_2',
            'category_id_3',
            'year',
            'class',
            'area',
            'collection',
            'version',
            'category_1',
            'category_2',
            'category_3'
        ]
    )
    task.start()
    return task


# ================================
# MAIN
# ================================

image = ee.Image(ASSET_IMAGE)
export_tasks = []
task_counter = 0

for categories in CATEGORIES_CROSSING:

    category1, category2, category3 = categories

    existing_years = check_existing_years(
        category1=category1['CATEGORY'],
        category2=category2['CATEGORY'],
        category3=category3['CATEGORY'],
        version=STATISTICS_VERSION,
        collection=str(COLLECTION_ID),
        table=BIG_QUERY_TABLE)

    print(categories, existing_years)

    for year in YEARS:

        name = "collection-{}-{}-{}.crossing-{}.{}.{}-{}-{}".format(
            str(COLLECTION_ID).replace(".", "_"),
            REGION,
            TABLE_ID,
            category1['CATEGORY'].lower(),
            category2['CATEGORY'].lower(),
            category3['CATEGORY'].lower(),
            year,
            STATISTICS_VERSION)

        task_counter += 1

        if year not in existing_years:
            territories1 = ee.ImageCollection(ASSET_TERRITORIES).filter(f"CATEG_ID=={category1['CATEG_ID']}").max().selfMask()
            territories2 = ee.ImageCollection(ASSET_TERRITORIES).filter(f"CATEG_ID=={category2['CATEG_ID']}").max().selfMask()
            territories3 = ee.ImageCollection(ASSET_TERRITORIES).filter(f"CATEG_ID=={category3['CATEG_ID']}").max().selfMask()

            crossing = territories1.multiply(100000000).add(
                territories2.multiply(10000)).add(territories3)

            areas = calculateAreaUsingRaster(
                image=image.select([BAND_PATTERN.format(year=year)]),
                theme=crossing,
                geometry=REGION_BBOX,
                year=year)

            areas = areas.map(
                lambda feature: feature
                .set('collection',   str(COLLECTION_ID))
                .set('category_1',   category1['CATEGORY'])
                .set('category_2',   category2['CATEGORY'])
                .set('category_3',   category3['CATEGORY'])
                .set('category_id_1', int(category1['CATEG_ID']))
                .set('category_id_2', int(category2['CATEG_ID']))
                .set('category_id_3', int(category3['CATEG_ID']))
                .set('version', STATISTICS_VERSION)
            )

            print(f"Task {task_counter}/{nTasks}: {name}")
            export_tasks.append(export(areas, name, BIG_QUERY_TABLE))

        else:
            print(f"Task {task_counter}/{nTasks}: {name} already exists!")

wait_until_tasks_finish(export_tasks)
