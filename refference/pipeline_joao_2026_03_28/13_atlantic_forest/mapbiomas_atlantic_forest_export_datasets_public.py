# -*- coding: utf-8 -*-
"""
Script for exporting MapBiomas Atlantic Forest coverage data to the GEE Data Catalog.
"""

import ee
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.region_utils import RegionUtils
from utils.export_utils import (
    wait_until_tasks_finish,
    export_image_to_asset as _export_image_to_asset,
)
from utils.export_public_utils import export_coverage_gee_data_catalog

# ================================
# AUTHENTICATION AND INITIALIZATION
# ================================
try:
    ee.Initialize(project='mapbiomas-atlantic_forest')
except Exception:
    print("Authenticating Earth Engine...")
    ee.Authenticate()
    ee.Initialize(project='mapbiomas-atlantic_forest')

region_utils = RegionUtils()

# ================================
# GLOBAL CONFIGURATION
# ================================

REGION_NAME    = 'atlantic_forest'
REGION_ISO3    = 'AF'
COLLECTION_ID  = 4

PROJECT_PUBLIC_PATH    = 'projects/mapbiomas-public/assets'
PROJECT_TERRITORY_PATH = 'projects/mapbiomas-territories/assets'

EXPORT_SCALE             = 30
EXPORT_MAX_PIXELS        = 1e13
EXPORT_PYRAMIDING_POLICY = 'MODE'

# NOTE: territory folder uses 'ATLANTIC-FOREST' (hyphen), not 'ATLANTIC_FOREST'
ASSET_TERRITORY = f'{PROJECT_TERRITORY_PATH}/TERRITORIES/ATLANTIC-FOREST/WORKSPACE/POLITICAL_LEVEL_1/POLITICAL_LEVEL_1_v1'
REGION_MASK     = ee.Image().paint(featureCollection=ee.FeatureCollection(ASSET_TERRITORY), color=1)
REGION_BBOX     = region_utils.get_bbox(iso3=REGION_ISO3)

# ================================
# VERSIONING
# ================================

VERSION_PUBLIC_INTEGRATION = 'v1'

# ================================
# ASSET PATHS
# ================================

PUBLIC_COLLECTION            = f'{PROJECT_PUBLIC_PATH}/{REGION_NAME}/lulc/collection{COLLECTION_ID}'
PUBLIC_INTEGRATION           = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_coverage_{VERSION_PUBLIC_INTEGRATION}'
PUBLIC_COVERAGE_DATA_CATALOG = f'{PROJECT_PUBLIC_PATH}/{REGION_NAME}/lulc/v1'
ASSET_COVERAGE_CATALOG       = PUBLIC_INTEGRATION

# ================================
# YEARS
# ================================

YEARS = list(range(1985, 2024))

# ================================
# EXECUTION CONTROL
# ================================

RUN_EXPORT_COVERAGE_DATA_CATALOG = True

# ================================
# EXPORT FUNCTIONS
# ================================

def export_image_to_asset(image, asset_path, version=None, scale=EXPORT_SCALE,
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


# ================================
# EXECUTE EXPORTS AND MONITOR
# ================================

export_tasks = []

if RUN_EXPORT_COVERAGE_DATA_CATALOG:
    export_tasks.extend(export_coverage_gee_data_catalog(
        export_image_to_asset,
        YEARS, PUBLIC_COVERAGE_DATA_CATALOG, ASSET_COVERAGE_CATALOG,
        COLLECTION_ID, VERSION_PUBLIC_INTEGRATION, REGION_NAME,
    ))

wait_until_tasks_finish(export_tasks=export_tasks, polling_interval=30)
