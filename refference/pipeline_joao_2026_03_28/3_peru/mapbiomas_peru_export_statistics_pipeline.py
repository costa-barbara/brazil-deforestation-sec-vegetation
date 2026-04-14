# -*- coding: utf-8 -*-
import ee
import time
from google.cloud import bigquery
from datetime import timedelta


client = bigquery.Client(project="mapbiomas-peru")

# ================================
# AUTHENTICATION AND INITIALIZATION
# ================================
try:
    ee.Initialize(project='mapbiomas-peru')
except Exception:
    print("Authenticating Earth Engine...")
    ee.Authenticate()
    ee.Initialize(project='mapbiomas-peru')

# ---------------------------
# Inicialização e constantes
# ---------------------------

REGION = 'peru'
COLLECTION = '3'
SUB_COLLECTION = '0'

INPUT_VERSION = '9-1'
STATISTICS_VERSION = f"{INPUT_VERSION}.1"
TEHMES_VERSION = 2

ASSET_COVERAGE      = f"projects/mapbiomas-{REGION}/assets/LAND-COVER/COLLECTION{COLLECTION}/integracion-pais/PERU-9"
ASSET_DEFORESTATION = f"projects/mapbiomas-{REGION}/assets/LAND-COVER/COLLECTION{COLLECTION}/DEFORESTATION/deforestation-secondary-vegetation-annual"
ASSET_TRANSITIONS   = f"projects/mapbiomas-{REGION}/assets/LAND-COVER/COLLECTION{COLLECTION}/transitions"
ASSET_THEMES        = f"projects/mapbiomas-territories/assets/TERRITORIES/LULC/{REGION.upper()}/COLLECTION{COLLECTION}/dashboard"

TABLE_COVERAGE      = f"mapbiomas.mapbiomas_{REGION}_statistics.coverage"
TABLE_DEFORESTATION = f"mapbiomas.mapbiomas_{REGION}_statistics.deforestation_annual"
TABLE_TRANSITIONS   = f"mapbiomas.mapbiomas_{REGION}_statistics.transitions"

YEARS_COVERAGE = list(range(1985, 2025))

YEARS_DEFORESTATION = list(range(2000, 2025))

PERIODS_TRANSITIONS = [
    (1985, 1986), (1986, 1987), (1987, 1988), (1988, 1989), (1989, 1990),
    (1990, 1991), (1991, 1992), (1992, 1993), (1993, 1994), (1994, 1995),
    (1995, 1996), (1996, 1997), (1997, 1998), (1998, 1999), (1999, 2000),
    (2000, 2001), (2001, 2002), (2002, 2003), (2003, 2004), (2004, 2005),
    (2005, 2006), (2006, 2007), (2007, 2008), (2008, 2009), (2009, 2010),
    (2010, 2011), (2011, 2012), (2012, 2013), (2013, 2014), (2014, 2015),
    (2015, 2016), (2016, 2017), (2017, 2018), (2018, 2019), (2019, 2020),
    (2020, 2021), (2021, 2022), (2022, 2023), (2023, 2024),
    (1985, 1990), (1990, 1995), (1995, 2000), (2000, 2005), (2005, 2010),
    (2010, 2015), (2015, 2020), (1990, 2000), (2000, 2010), (2010, 2020),
    (1985, 2024), (2008, 2017), (1994, 2002), (2002, 2010), (2010, 2016),
    (1986, 2015), (1990, 2024), (2000, 2024), (2008, 2024), (2010, 2024),
    (2012, 2024),
]

PIXEL_AREA = ee.Image.pixelArea().divide(1e6)

CATEGORIES = [
    # {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
    # {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
    # {"CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": "3"},
    # {"CATEGORY": "POLITICAL_LEVEL_4", "CATEG_ID": "95"},
    # {"CATEGORY": "BIOMES", "CATEG_ID": "4"},
    # {"CATEGORY": "ECORREGION", "CATEG_ID": "34"},
    # {"CATEGORY": "RESERVA_DA_BIOSFERA", "CATEG_ID": "42"},
    # {"CATEGORY": "BASIN_LEVEL_1", "CATEG_ID": "73"},
    # {"CATEGORY": "BASIN_LEVEL_2", "CATEG_ID": "74"},
    # {"CATEGORY": "BASIN_LEVEL_3", "CATEG_ID": "75"},
    # {"CATEGORY": "RESGUARDOS_INDIGENAS", "CATEG_ID": "79"},
    # {"CATEGORY": "BASIN_LEVEL_4", "CATEG_ID": "98"},
    # {"CATEGORY": "EXPANSION_OR_TITLING_APPLICATION", "CATEG_ID": "115"},
    # {"CATEGORY": "LAND_RESERVE", "CATEG_ID": "116"},
    # {"CATEGORY": "RECOGNIZED_PEASANT_COMMUNITY", "CATEG_ID": "118"},
    # {"CATEGORY": "NATIONAL_PROTECTED_AREAS", "CATEG_ID": "119"},
    # {"CATEGORY": "REGIONAL_CONSERVATION_AREAS", "CATEG_ID": "120"},
    # {"CATEGORY": "PRIVATE_CONSERVATION_AREAS", "CATEG_ID": "121"},
    # {"CATEGORY": "ECOZONE", "CATEG_ID": "122"},
    # {"CATEGORY": "NPA_BUFFER_ZONE", "CATEG_ID": "143"},
    # {"CATEGORY": "FRAGILE_ECOSYSTEM", "CATEG_ID": "144"},
    # {"CATEGORY": "PERMANENT_PRODUCTION_FOREST", "CATEG_ID": "145"},
    # {"CATEGORY": "LOCAL_FOREST", "CATEG_ID": "146"},
    # {"CATEGORY": "WILDLIFE_MANAGEMENT_AREA_CONCESSION", "CATEG_ID": "147"},
    # {"CATEGORY": "CONSERVATION_CONCESSION", "CATEG_ID": "148"},
    # {"CATEGORY": "CONCESSION_FOR_NON-TIMBER_FOREST_PRODUCTS", "CATEG_ID": "149"},
    # {"CATEGORY": "ECOTOURISM_CONCESSION", "CATEG_ID": "150"},
    # {"CATEGORY": "AFFORESTATION_AND_REFORESTATION_CONCESSION", "CATEG_ID": "151"},
    {"CATEGORY": "TITLED_NATIVE_COMMUNITY", "CATEG_ID": "114"},
    {"CATEGORY": "TITLED_PEASANT_COMMUNITY", "CATEG_ID": "117"},
]

# ---------------------------
# Funções utilitárias
# ---------------------------

def wait_until_tasks_finish(export_tasks=[], polling_interval=60):
    """
    Waits until all export tasks created by this script have finished.

    Continues monitoring even if some tasks fail. Displays status and duration at the end.
    Output is formatted with consistent alignment.
    Skips processing if no tasks were submitted.
    """
    if not export_tasks:
        print("⚠️  No export tasks to monitor.")
        return

    start_time = time.time()
    print(f"\n⏳ Waiting for {len(export_tasks)} task(s) to finish...")

    completed = set()
    failed_tasks = []

    # Calculate max width for formatting
    max_len = max(len(task.status().get('description', '')) for task in export_tasks)
    label_pad = max(40, max_len + 4)

    while True:
        all_done = True

        for task in export_tasks:
            status = task.status()
            description = status.get('description', '[No Description]')
            state = status['state']

            if description in completed:
                continue

            if state in ['READY', 'RUNNING']:
                all_done = False
                print(f"🚀 {description.ljust(label_pad)} | {time.strftime('%Y-%m-%dT%H:%M:%S')} | Status: {state}")
            elif state == 'FAILED':
                error_message = status.get('error_message', 'No error message provided.')
                failed_tasks.append((description, error_message))
                completed.add(description)
                print(f"❌ {description.ljust(label_pad)} | {time.strftime('%Y-%m-%dT%H:%M:%S')} | Status: {state} | Error: {error_message} ")
            elif state == 'COMPLETED':
                completed.add(description)
                print(f"✅ {description.ljust(label_pad)} | {time.strftime('%Y-%m-%dT%H:%M:%S')} | Status: {state}")
            elif state == 'CANCELLED':
                completed.add(description)
                print(f"⚠️ {description.ljust(label_pad)} | {time.strftime('%Y-%m-%dT%H:%M:%S')} | Status: {state}")

        if all_done and len(completed) == len(export_tasks):
            total_time = time.time() - start_time
            formatted_time = str(timedelta(seconds=int(total_time)))
            print(f"\n✅ All tasks finished. Total time: {formatted_time}")
            if failed_tasks:
                print(f"\n❌ {len(failed_tasks)} task(s) failed:\n")
                for description, error in failed_tasks:
                    print(f"❌ {description}\n→ {error}\n")
            break

        time.sleep(polling_interval)


def check_existing_entries(category, table, collection, version, stat_type):
    
    if stat_type == 'transitions':
        query = f"""
        WITH unique_pairs AS (
            SELECT DISTINCT version, year_from, year_to
            FROM `{table}`
            WHERE 
                category = '{category}' AND 
                version = '{version}' AND 
                collection = '{collection}')
        SELECT 
            version, 
            ARRAY_AGG(STRUCT(year_from, year_to)) AS unique_years
        FROM unique_pairs 
        GROUP BY 
        version
        """
        result = client.query(query).result()
        
        return {row.version: row.unique_years for row in result}.get(version, [])
    else:
        query = f"""
        SELECT version, ARRAY_AGG(DISTINCT year) AS years
        FROM `{table}` 
        WHERE 
            category = '{category}' AND 
            version = '{version}' AND 
            collection = '{collection}'
        GROUP BY
        version
        """
        result = client.query(query).result()
        
        return {row.version: row.years for row in result}.get(version, [])

def convert2table(stat_type):
    
    def inner(obj):
        obj = ee.Dictionary(obj)
        territory = obj.get('feature_id')
        groups = ee.List(obj.get('groups'))

        def format_feature(class_area):
            d = ee.Dictionary(class_area)
            class_id = ee.Number(d.get('class'))
            feature = ee.Feature(None).set('feature_id', territory)
            area = d.get('sum')
            if stat_type == 'coverage':
                return feature.set({'class': class_id, 'area': area})
            elif stat_type == 'deforestation_annual':
                return feature.set({
                    'transition': class_id.divide(100).int(),
                    'class': class_id.mod(100).int(),
                    'area': area
                })
            else:
                return feature.set({
                    'class_from': class_id.divide(100).int16(),
                    'class_to': class_id.mod(100).int16(),
                    'area': area
                })
        return ee.FeatureCollection(groups.map(format_feature))
    return inner

def calculate_area(image, territory, geometry, stat_type):
    
    reducer = ee.Reducer.sum().group(1, 'class').group(1, 'feature_id')
    
    areas = ee.Dictionary(
        PIXEL_AREA.addBands(territory).addBands(image)
            .reduceRegion(
                reducer=reducer, 
                geometry=geometry, 
                scale=30, 
                maxPixels=1e12
        )
    )
    return ee.FeatureCollection(ee.List(areas.get('groups')).map(convert2table(stat_type))).flatten()

def export_statistics(areas, name, table, stat_type):

    selectors = [
        'feature_id', 'category_id', 'year', 
        'class', 'area', 'collection', 'version', 'category']
    
    if stat_type == 'deforestation_annual':
        
        selectors = [
            'feature_id', 'category_id', 'year', 
            'transition', 'class', 'area', 'collection', 
            'version', 'category']
    
    elif stat_type == 'transitions':
        
        selectors = [
            'feature_id', 'category_id', 'year_from', 
            'year_to', 'class_from', 'class_to', 
            'area', 'collection', 'version', 'category']
    
    task = ee.batch.Export.table.toBigQuery(
        collection=areas, 
        description=name[:99], 
        table=table,
        overwrite=False, 
        append=True, 
        selectors=selectors
    )

    task.start()

    return task

# ---------------------------
# Funções principais de exportação
# ---------------------------

def export_coverage_statistics():
    
    stat_type = "coverage"
    image = ee.Image(ASSET_COVERAGE)

    tasks = []

    for category in CATEGORIES:
        existing = check_existing_entries(
            category=category["CATEGORY"], 
            table=TABLE_COVERAGE, 
            collection=f"{COLLECTION}.{SUB_COLLECTION}", 
            version=STATISTICS_VERSION, 
            stat_type=stat_type)
        
        territories = (
            ee.ImageCollection(ASSET_THEMES)
                .filter(ee.Filter.eq("version", int(TEHMES_VERSION)))
                .filter(ee.Filter.eq("CATEG_ID", int(category["CATEG_ID"])))
        )

        for year in YEARS_COVERAGE:
            if year in existing:
                continue
            
            name = f"collection-{COLLECTION}.{SUB_COLLECTION}-{REGION}-coverage-{category['CATEGORY']}-{year}-{STATISTICS_VERSION}"
            
            classified = image.select(f"classification_{year}")
            
            areas = (
                territories.map(
                    lambda t: calculate_area(
                                image=classified, 
                                territory=t, 
                                geometry=t.geometry().bounds(), 
                                stat_type=stat_type)
                ).flatten()
            )

            areas = areas.map(
                lambda f: f.set({
                    'collection': f"{COLLECTION}.{SUB_COLLECTION}",
                    'category': category["CATEGORY"],
                    'category_id': int(category["CATEG_ID"]),
                    'version': STATISTICS_VERSION,
                    'year': year
                }))
            
            print(f"Exportando: {name}")
            
            tasks.append(export_statistics(areas, name, TABLE_COVERAGE, stat_type))

    return tasks

def export_deforestation_statistics():

    stat_type = "deforestation_annual"

    image = ee.ImageCollection(ASSET_DEFORESTATION).filter(ee.Filter.eq("version", INPUT_VERSION)).mosaic()

    tasks = []

    for category in CATEGORIES:
        existing = check_existing_entries(category["CATEGORY"], TABLE_DEFORESTATION, f"{COLLECTION}.{SUB_COLLECTION}", STATISTICS_VERSION, stat_type)
        
        territories = (
            ee.ImageCollection(ASSET_THEMES)
                .filter(ee.Filter.eq("version", int(TEHMES_VERSION)))
                .filter(ee.Filter.eq("CATEG_ID", int(category["CATEG_ID"])))
        )

        for year in YEARS_DEFORESTATION:
            if year in existing:
                continue
            
            name = f"collection-{COLLECTION}.{SUB_COLLECTION}-{REGION}-deforestation.annual-{category['CATEGORY']}-{year}-{STATISTICS_VERSION}"
            
            classified = image.select(f"classification_{year}")
            
            areas = (
                territories
                    .map(lambda t: calculate_area(classified, t, t.geometry().bounds(), stat_type))
                    .flatten()
                    .map(lambda f: f.set({
                        'collection': f"{COLLECTION}.{SUB_COLLECTION}",
                        'category': category["CATEGORY"],
                        'category_id': int(category["CATEG_ID"]),
                        'version': STATISTICS_VERSION,
                        'year': year
                    }))
                )
            
            print(f"Exportando: {name}")
            
            tasks.append(export_statistics(areas, name, TABLE_DEFORESTATION, stat_type))

    return tasks

def export_transition_statistics():

    stat_type = "transitions"

    tasks = []

    for category in CATEGORIES:
        existing = check_existing_entries(category["CATEGORY"], TABLE_TRANSITIONS, f"{COLLECTION}.{SUB_COLLECTION}", STATISTICS_VERSION, stat_type)
        
        territories = (
            ee.ImageCollection(ASSET_THEMES)
                .filter(ee.Filter.eq("version", int(TEHMES_VERSION)))
                .filter(ee.Filter.eq("CATEG_ID", int(category["CATEG_ID"])))
        )

        for y0, y1 in PERIODS_TRANSITIONS:
            if {'year_from': y0, 'year_to': y1} in existing:
                continue
            
            name = f"collection-{COLLECTION}-{REGION}-transitions-{category['CATEGORY']}-{y0}.{y1}-{STATISTICS_VERSION}"
            
            image = ee.Image(f"{ASSET_TRANSITIONS}/mapbiomas_{REGION}_{y0}_{y1}_{INPUT_VERSION}")
            
            areas = (
                territories
                    .map(lambda t: calculate_area(image, t, t.geometry().bounds(), stat_type))
                    .flatten()
                    .map(lambda f: f.set({
                        'collection': f"{COLLECTION}.{SUB_COLLECTION}",
                        'category': category["CATEGORY"],
                        'category_id': int(category["CATEG_ID"]),
                        'version': STATISTICS_VERSION,
                        'year_from': y0,
                        'year_to': y1
                    }))
                )
            
            print(f"Exportando: {name}")
            
            tasks.append(export_statistics(areas, name, TABLE_TRANSITIONS, stat_type))

    return tasks

# ---------------------------
# Execução do pipeline
# ---------------------------
if __name__ == "__main__":

    coverage_tasks = export_coverage_statistics()
    wait_until_tasks_finish(export_tasks=coverage_tasks, polling_interval=60)

    deforestation_tasks = export_deforestation_statistics()
    wait_until_tasks_finish(export_tasks=deforestation_tasks, polling_interval=60)

    transitions_tasks = export_transition_statistics()
    wait_until_tasks_finish(export_tasks=transitions_tasks, polling_interval=60)
