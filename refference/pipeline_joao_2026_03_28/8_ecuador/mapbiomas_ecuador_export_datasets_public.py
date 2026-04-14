# -*- coding: utf-8 -*-
"""
Script for exporting MapBiomas datasets to a public folder using the Google Earth Engine Python API.

Includes:
- Transitions export
- Integrated map export
- Secondary vegetation/deforestation export
- Quality export
- Water surface and water bodies export


Execution of each export can be toggled using Boolean flags.
"""

import ee
import sys
import os


# Ensure the script can import modules from the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.region_utils import RegionUtils
from utils.export_utils import (
    wait_until_tasks_finish,
    export_image_to_asset as _export_image_to_asset,
)
from utils.export_public_utils import (
    export_coverage_gee_data_catalog,
    export_collection_product,
    export_quality_mosaic,
    export_water_surface,
    export_water_bodies,
    export_water_monthly,
)
# ================================
# AUTHENTICATION AND INITIALIZATION
# ================================
try:
    ee.Initialize(project='mapbiomas-ecuador')
except Exception:
    print("Authenticating Earth Engine...")
    ee.Authenticate()
    ee.Initialize(project='mapbiomas-ecuador')

region_utils = RegionUtils()

# ================================
# GLOBAL CONFIGURATION
# ================================

REGION_NAME = 'ecuador'
REGION_ISO3 = 'ECU'
COLLECTION_ID = 3
WATER_COLLECTION_ID = 3

# BBox of the region
REGION_BBOX = region_utils.get_bbox(iso3=REGION_ISO3)

PROJECT_REGION_PATH    = f'projects/mapbiomas-{REGION_NAME}/assets'
PROJECT_TERRITORY_PATH = 'projects/mapbiomas-territories/assets'
PROJECT_PUBLIC_PATH    = 'projects/mapbiomas-public/assets'

EXPORT_SCALE = 30
EXPORT_MAX_PIXELS = 1e13
EXPORT_PYRAMIDING_POLICY = 'MODE'

ASSET_TERRITORY = F'{PROJECT_TERRITORY_PATH}/TERRITORIES/{REGION_NAME.upper()}/WORKSPACE/POLITICAL_LEVEL_1/POLITICAL_LEVEL_1_v2'

REGION_MASK = ee.Image().paint(featureCollection=ee.FeatureCollection(ASSET_TERRITORY), color=1)

# ================================
# VERSIONING
# ================================

VERSION_INPUT_INTEGRATION      = '6'
VERSION_PUBLIC_INTEGRATION     = 'v3'

VERSION_INPUT_DEF_SEC_VEG      = '3'
VERSION_PUBLIC_DEF_SEC_VEG     = 'v4'

VERSION_INPUT_DEF_SEC_VEG_ANN  = '6-1'
VERSION_PUBLIC_DEF_SEC_VEG_ANN = 'v1'

VERSION_INPUT_DEF_SEC_VEG_ACC  = '6-1'
VERSION_PUBLIC_DEF_SEC_VEG_ACC = 'v1'

VERSION_INPUT_DEF_FREQ         = '6-1'
VERSION_PUBLIC_DEF_FREQ        = 'v1'

VERSION_INPUT_SEC_VEG_AGE      = '6-1'
VERSION_PUBLIC_SEC_VEG_AGE     = 'v1'

VERSION_INPUT_TRANSITIONS      = '6-1'
VERSION_PUBLIC_TRANSITIONS     = 'v1'

VERSION_INPUT_QUALITY          = '1'
VERSION_PUBLIC_QUALITY         = 'v2'

VERSION_INPUT_WATER_SURFACE     = 1
VERSION_PUBLIC_WATER_SURFACE    = 'v1'

VERSION_INPUT_WATER_BODIES      = 1
VERSION_PUBLIC_WATER_BODIES     = 'v2'

VERSION_INPUT_WATER_MONTHLY      = '1'
VERSION_PUBLIC_WATER_MONTHLY     = 'v1'
# ================================
# ASSET PATHS
# ================================

ASSET_COLLECTION      = f'projects/mapbiomas-{REGION_NAME}/assets/LAND-COVER/COLLECTION-{COLLECTION_ID}'
ASSET_INTEGRATION     = f'projects/mapbiomas-public/assets/ecuador/lulc/collection3/mapbiomas_ecuador_collection3_coverage_v3'
ASSET_TRANSITIONS     = f'{ASSET_COLLECTION}/INTEGRATION/transitions'
ASSET_DEF_SEC_VEG     = f'projects/mapbiomas-ecuador/assets/DEFORESTATION/deforestation-secondary-vegetation-ft'
ASSET_DEF_SEC_VEG_ANN = f'projects/mapbiomas-ecuador/assets/DEFORESTATION/deforestation-secondary-vegetation-annual'
ASSET_DEF_SEC_VEG_ACC = f'{ASSET_COLLECTION}/DEFORESTATION/deforestation-secondary-vegetation-accumulated'
ASSET_DEF_FREQ        = f'{ASSET_COLLECTION}/DEFORESTATION/deforestation-frequency'
ASSET_SEC_VEG_AGE     = f'{ASSET_COLLECTION}/DEFORESTATION/secondary-vegetation-age'
ASSET_WATER_SURFACE   = f'projects/mapbiomas-ecuador/assets/WATER/COLLECTION-3/01-SURFACE/E03-INTEGRATION/water-integracion-01'
ASSET_WATER_BODIES    = f'projects/mapbiomas-ecuador/assets/WATER/COLLECTION-3/02-BODIES/INTEGRATION/water-classification-integracion-01'
ASSET_WATER_MONTHLY   = f'projects/mapbiomas-ecuador/assets/WATER/COLLECTION-3/FINAL-ASSETS/water-monthly-01'

ASSET_QUALITY         = 'projects/mapbiomas-workspace/COLECAO7/qualidade'

PUBLIC_COLLECTION       = f'{PROJECT_PUBLIC_PATH}/{REGION_NAME}/lulc/collection{COLLECTION_ID}'
PUBLIC_WATER_COLLECTION = f'{PROJECT_PUBLIC_PATH}/{REGION_NAME}/water/collection{WATER_COLLECTION_ID}'

PUBLIC_INTEGRATION     = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_coverage_{VERSION_PUBLIC_INTEGRATION}'
PUBLIC_TRANSITIONS     = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_transitions_{VERSION_PUBLIC_TRANSITIONS}'
PUBLIC_DEF_SEC_VEG     = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_deforestation_secondary_vegetation_{VERSION_PUBLIC_DEF_SEC_VEG}'
PUBLIC_DEF_SEC_VEG_ANN = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_deforestation_secondary_vegetation_{VERSION_PUBLIC_DEF_SEC_VEG_ANN}'
PUBLIC_DEF_SEC_VEG_ACC = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_deforestation_secondary_vegetation_accumulated_{VERSION_PUBLIC_DEF_SEC_VEG_ACC}'
PUBLIC_DEF_FREQUENCY   = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_deforestation_frequency_{VERSION_PUBLIC_DEF_FREQ}'
PUBLIC_SEC_VEG_AGE     = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_secondary_vegetation_age_{VERSION_PUBLIC_SEC_VEG_AGE}'
PUBLIC_WATER_SURFACE   = f'{PUBLIC_WATER_COLLECTION}/mapbiomas_{REGION_NAME}_collection{WATER_COLLECTION_ID}_water_{VERSION_PUBLIC_WATER_SURFACE}'
PUBLIC_WATER_BODIES    = f'{PUBLIC_WATER_COLLECTION}/mapbiomas_{REGION_NAME}_collection{WATER_COLLECTION_ID}_water_bodies_{VERSION_PUBLIC_WATER_BODIES}'
PUBLIC_WATER_MONTHLY   = f'{PUBLIC_WATER_COLLECTION}/mapbiomas_{REGION_NAME}_collection{WATER_COLLECTION_ID}_water_monthly_{VERSION_PUBLIC_WATER_MONTHLY}'
PUBLIC_QUALITY                = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_quality_{VERSION_PUBLIC_QUALITY}'
PUBLIC_COVERAGE_DATA_CATALOG  = f'{PROJECT_PUBLIC_PATH}/{REGION_NAME}/lulc/v1'
ASSET_COVERAGE_CATALOG        = PUBLIC_INTEGRATION

# ================================
# YEARS QUALITY, TRANSITION PERIODS
# ================================
YEARS                 = list(range(1985, 2025))
YEARS_DEF_SEC_VEG     = list(range(2001, 2025))
YEARS_QUALITY         = list(range(1985, 2025))
YEARS_WATER_SURFACE   = list(range(1985, 2025))
YEARS_WATER_BODIES    = list(range(1985, 2025))
YEARS_WATER_MONTHLY   = list(range(1985, 2025))  # Monthly data available from 1985 to 2024

TRANSITION_PERIODS = [
    (1985, 1986), (1986, 1987), (1987, 1988), (1988, 1989), (1989, 1990),
    (1990, 1991), (1991, 1992), (1992, 1993), (1993, 1994), (1994, 1995),
    (1995, 1996), (1996, 1997), (1997, 1998), (1998, 1999), (1999, 2000),
    (2000, 2001), (2001, 2002), (2002, 2003), (2003, 2004), (2004, 2005),
    (2005, 2006), (2006, 2007), (2007, 2008), (2008, 2009), (2009, 2010),
    (2010, 2011), (2011, 2012), (2012, 2013), (2013, 2014), (2014, 2015),
    (2015, 2016), (2016, 2017), (2017, 2018), (2018, 2019), (2019, 2020),
    (2020, 2021), (2021, 2022), (2022, 2023), (2023, 2024), (1985, 2024),
    (2008, 2017), (1994, 2002), (2002, 2010), (2010, 2016), (1986, 2015), 
    (1990, 2024), (2000, 2024), (2008, 2024), (2010, 2024), (2012, 2024), 
]

# ================================
# EXECUTION CONTROL
# ================================
RUN_EXPORT_INTEGRATION     = False
RUN_EXPORT_DEF_SEC_VEG     = False
RUN_EXPORT_DEF_SEC_VEG_ANN = False
RUN_EXPORT_DEF_SEC_VEG_ACC = False
RUN_EXPORT_DEF_FREQ        = False
RUN_EXPORT_SEC_VEG_AGE     = False
RUN_EXPORT_TRANSITIONS     = False
RUN_EXPORT_QUALITY         = False
RUN_EXPORT_WATER_SURFACE   = False
RUN_EXPORT_WATER_BODIES    = False
RUN_EXPORT_WATER_MONTHLY            = False
RUN_EXPORT_COVERAGE_DATA_CATALOG    = True

# ================================
# EXPORT FUNCTIONS
# ================================

def export_image_to_asset(image, asset_path, version=None, scale=30,
                          additional_properties=None, region_mask=REGION_MASK):
    """Wrapper around export_utils.export_image_to_asset with region defaults."""
    return _export_image_to_asset(
        image, asset_path,
        region_mask=region_mask,
        region_bbox=REGION_BBOX,
        collection_id=COLLECTION_ID,
        region_name=REGION_NAME,
        max_pixels=EXPORT_MAX_PIXELS,
        version=version,
        scale=scale,
        additional_properties=additional_properties,
        pyramiding_policy=EXPORT_PYRAMIDING_POLICY,
    )


def export_public_transitions():
    """
    Creates and exports a multi-band image with all defined transition periods.
    Each band corresponds to one transition between two years.
    """
    images = []
    for start, end in TRANSITION_PERIODS:
        asset_id = f'{ASSET_TRANSITIONS}/mapbiomas_{REGION_NAME}_{start}_{end}_{VERSION_INPUT_TRANSITIONS}'
        band_name = f'transitions_{start}_{end}'
        images.append(ee.Image(asset_id).rename(band_name))

    transitions_image = ee.Image(images)
    return export_image_to_asset(transitions_image, PUBLIC_TRANSITIONS, version=VERSION_PUBLIC_TRANSITIONS)

# ================================
# EXECUTE EXPORTS AND MONITOR
# ================================
export_tasks = []

if RUN_EXPORT_INTEGRATION:
    export_tasks.append(export_collection_product(
        export_image_to_asset,
        ASSET_INTEGRATION, VERSION_INPUT_INTEGRATION,
        PUBLIC_INTEGRATION, VERSION_PUBLIC_INTEGRATION,
    ))

if RUN_EXPORT_TRANSITIONS:
    export_tasks.append(export_public_transitions())

if RUN_EXPORT_DEF_SEC_VEG:
    export_tasks.append(export_collection_product(
        export_image_to_asset,
        ASSET_DEF_SEC_VEG, VERSION_INPUT_DEF_SEC_VEG,
        PUBLIC_DEF_SEC_VEG, VERSION_PUBLIC_DEF_SEC_VEG,
        select_bands=[f'classification_{year}' for year in YEARS_DEF_SEC_VEG],
        additional_properties={
            "module": "Vegetation loss",
            "submodule": "Annual vegetation loss",
            "variable": "Annual vegetation loss",
            "format": "classification_multiband_image",
            "band_format": "classification_{year}",
            "data_type": "annual",
            "version": VERSION_PUBLIC_DEF_SEC_VEG,
        },
    ))

if RUN_EXPORT_DEF_SEC_VEG_ANN:
    export_tasks.append(export_collection_product(
        export_image_to_asset,
        ASSET_DEF_SEC_VEG_ANN, VERSION_INPUT_DEF_SEC_VEG_ANN,
        PUBLIC_DEF_SEC_VEG_ANN, VERSION_PUBLIC_DEF_SEC_VEG_ANN,
    ))

if RUN_EXPORT_DEF_SEC_VEG_ACC:
    export_tasks.append(export_collection_product(
        export_image_to_asset,
        ASSET_DEF_SEC_VEG_ACC, VERSION_INPUT_DEF_SEC_VEG_ACC,
        PUBLIC_DEF_SEC_VEG_ACC, VERSION_PUBLIC_DEF_SEC_VEG_ACC,
    ))

if RUN_EXPORT_DEF_FREQ:
    export_tasks.append(export_collection_product(
        export_image_to_asset,
        ASSET_DEF_FREQ, VERSION_INPUT_DEF_FREQ,
        PUBLIC_DEF_FREQUENCY, VERSION_PUBLIC_DEF_FREQ,
    ))

if RUN_EXPORT_SEC_VEG_AGE:
    export_tasks.append(export_collection_product(
        export_image_to_asset,
        ASSET_SEC_VEG_AGE, VERSION_INPUT_SEC_VEG_AGE,
        PUBLIC_SEC_VEG_AGE, VERSION_PUBLIC_SEC_VEG_AGE,
    ))

if RUN_EXPORT_QUALITY:
    export_tasks.append(export_quality_mosaic(
        export_image_to_asset,
        ASSET_QUALITY, YEARS_QUALITY, PUBLIC_QUALITY, VERSION_PUBLIC_QUALITY,
    ))

if RUN_EXPORT_WATER_SURFACE:
    export_tasks.append(export_water_surface(
        export_image_to_asset,
        ASSET_WATER_SURFACE, YEARS_WATER_SURFACE, PUBLIC_WATER_SURFACE, VERSION_PUBLIC_WATER_SURFACE,
        version_input=VERSION_INPUT_WATER_SURFACE,
    ))

if RUN_EXPORT_WATER_BODIES:
    export_tasks.append(export_water_bodies(
        export_image_to_asset,
        ASSET_WATER_BODIES, YEARS_WATER_BODIES, PUBLIC_WATER_BODIES, VERSION_PUBLIC_WATER_BODIES,
        version_input=VERSION_INPUT_WATER_BODIES,
        class_remap={'from': [1, 2, 3, 4, 5, 6], 'to': [1, 2, 3, 4, 0, 5]},
    ))

if RUN_EXPORT_WATER_MONTHLY:
    export_tasks.extend(export_water_monthly(
        export_image_to_asset,
        ASSET_WATER_MONTHLY, YEARS_WATER_MONTHLY, PUBLIC_WATER_MONTHLY, VERSION_PUBLIC_WATER_MONTHLY,
        version_input=VERSION_INPUT_WATER_MONTHLY,
        use_version_filter=True, month_as_string=True,
    ))

if RUN_EXPORT_COVERAGE_DATA_CATALOG:
    export_tasks.extend(export_coverage_gee_data_catalog(
        export_image_to_asset,
        YEARS, PUBLIC_COVERAGE_DATA_CATALOG, ASSET_COVERAGE_CATALOG,
        COLLECTION_ID, VERSION_PUBLIC_INTEGRATION, REGION_NAME,
    ))


wait_until_tasks_finish(export_tasks=export_tasks, polling_interval=30)
