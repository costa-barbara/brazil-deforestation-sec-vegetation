# -*- coding: utf-8 -*-
"""
Script for exporting MapBiomas datasets to a public folder using the Google Earth Engine Python API.

Includes:
- Transitions export
- Integrated map export
- Secondary vegetation/deforestation export
- Quality export

Execution of each export can be toggled using Boolean flags.
"""

import ee
import sys
import os
import json


# Ensure the script can import modules from the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.region_utils import RegionUtils
from utils.export_utils import (
    wait_until_tasks_finish,
    ensure_asset_exists,
    set_asset_properties,
    export_image_to_asset as _export_image_to_asset,
)
from utils.export_public_utils import (
    export_coverage_gee_data_catalog,
    export_collection_product,
    export_quality_mosaic,
    export_water_surface,
    export_water_bodies,
)
# ================================
# AUTHENTICATION AND INITIALIZATION
# ================================
try:
    ee.Initialize(project='mapbiomas-bolivia')
except Exception:
    print("Authenticating Earth Engine...")
    ee.Authenticate()
    ee.Initialize(project='mapbiomas-bolivia')


region_utils = RegionUtils()

# ================================
# GLOBAL CONFIGURATION
# ================================

REGION_NAME = 'brazil'
REGION_ISO3 = 'BRA'
COLLECTION_ID = 10
SOIL_COLLECTION_ID = 3
WATER_COLLECTION_ID = 4

TERRITORY_CATEGORY_ID = '1'
TERRITORY_FEATURE_ID  = '1'
TERRITORY_VERSION     = '2'
TERRITORY_IMAGE_ID    = f'{TERRITORY_CATEGORY_ID}_{TERRITORY_FEATURE_ID}_{TERRITORY_VERSION}'

# BBox of the region
REGION_BBOX = region_utils.get_bbox(iso3=REGION_ISO3)

PROJECT_REGION_PATH    = f'projects/mapbiomas-{REGION_NAME}/assets'
PROJECT_TERRITORY_PATH = 'projects/mapbiomas-territories/assets'
PROJECT_PUBLIC_PATH    = 'projects/mapbiomas-public/assets'

EXPORT_SCALE = 30
EXPORT_SCALE_10M = 10
EXPORT_MAX_PIXELS = 1e13
EXPORT_PYRAMIDING_POLICY = 'MODE'

# ASSET_TERRITORY = f'{PROJECT_TERRITORY_PATH}/TERRITORIES/LULC/{REGION_NAME.upper()}/COLLECTION{COLLECTION_ID}/dashboard/{TERRITORY_IMAGE_ID}'
ASSET_TERRITORY = f'{PROJECT_TERRITORY_PATH}/TERRITORIES/LULC/{REGION_NAME.upper()}/COLLECTION9/dashboard/{TERRITORY_IMAGE_ID}'
REGION_MASK = ee.Image(ASSET_TERRITORY)

# ================================
# VERSIONING
# ================================

VERSION_INPUT_INTEGRATION        = '0-7-tra-3'
VERSION_PUBLIC_INTEGRATION       = 'v2'

VERSION_INPUT_DEF_SEC_VEG        = '0-7-tra-3-2'
VERSION_PUBLIC_DEF_SEC_VEG       = 'v2'

VERSION_INPUT_DEF_SEC_VEG_ANN    = '0-7-tra-2'
VERSION_PUBLIC_DEF_SEC_VEG_ANN   = 'v2'

VERSION_INPUT_DEF_SEC_VEG_ACC    = '0-7-tra-3-2'
VERSION_PUBLIC_DEF_SEC_VEG_ACC   = 'v2'

VERSION_INPUT_DEF_FREQ           = '0-7-tra-3-2'
VERSION_PUBLIC_DEF_FREQ          = 'v2'

VERSION_INPUT_SEC_VEG_AGE        = '0-7-tra-3-2'
VERSION_PUBLIC_SEC_VEG_AGE       = 'v2'

VERSION_INPUT_TRANSITIONS        = '0-7-tra-3-2'
VERSION_PUBLIC_TRANSITIONS       = 'v2'

VERSION_INPUT_QUALITY            = '1'
VERSION_PUBLIC_QUALITY           = 'v1'

VERSION_INPUT_MINING_SUBSTANCES  = '3'
VERSION_PUBLIC_MINING_SUBSTANCES = 'v2'

VERSION_INPUT_AGRICULTURE_IRRIGATION = '1'
VERSION_PUBLIC_AGRICULTURE_IRRIGATION = 'v2'

VERSION_INPUT_AGRICULTURE_NUM_CYCLES       = '1'
VERSION_PUBLIC_AGRICULTURE_NUM_CYCLES      = 'v2'
VERSION_PUBLIC_AGRICULTURE_NUM_CYCLES_MEAN = 'v2'

VERSION_INPUT_PASTURE_VIGOR      = '1'
VERSION_PUBLIC_PASTURE_VIGOR     = 'v1'

VERSION_INPUT_PASTURE_BIOMASS    = '1'
VERSION_PUBLIC_PASTURE_BIOMASS   = 'v1'

VERSION_INPUT_URBAN_HAND        = '1'
VERSION_PUBLIC_URBAN_HAND       = 'v1'

VERSION_INPUT_URBAN_RISK        = '1'
VERSION_PUBLIC_URBAN_RISK       = 'v1'

VERSION_INPUT_URBAN_SLOPE       = '1'
VERSION_PUBLIC_URBAN_SLOPE      = 'v1'

VERSION_INPUT_URBAN_SLUM        = '1'
VERSION_PUBLIC_URBAN_SLUM       = 'v1'

VERSION_INPUT_URBAN_TRACTS      = '1'
VERSION_PUBLIC_URBAN_TRACTS     = 'v1'

VERSION_INPUT_URBAN_EPOCHS      = '1'
VERSION_PUBLIC_URBAN_EPOCHS     = 'v1'

VERSION_INPUT_WATER_SURFACE     = '11'
VERSION_PUBLIC_WATER_SURFACE    = 'v3'

VERSION_INPUT_WATER_BODIES      = '5'
VERSION_PUBLIC_WATER_BODIES     = 'v1'

VERSION_INPUT_SOIL_SAND_FRACTION = '1'
VERSION_PUBLIC_SOIL_SAND_FRACTION = 'v1'

VERSION_INPUT_SOIL_CLAY_FRACTION = '1'
VERSION_PUBLIC_SOIL_CLAY_FRACTION = 'v1'

VERSION_INPUT_SOIL_SILT_FRACTION = '1'
VERSION_PUBLIC_SOIL_SILT_FRACTION = 'v1'

VERSION_INPUT_SOIL_TEXTURAL_GROUP = '1'
VERSION_PUBLIC_SOIL_TEXTURAL_GROUP = 'v1'

VERSION_INPUT_SOIL_TEXTURAL_SUBGROUP = '1'
VERSION_PUBLIC_SOIL_TEXTURAL_SUBGROUP = 'v1'

VERSION_INPUT_SOIL_TEXTURAL_CLASS = 'v1'
VERSION_PUBLIC_SOIL_TEXTURAL_CLASS = 'v1'

VERSION_INPUT_SOIL_CARBON        = '1'
VERSION_PUBLIC_SOIL_CARBON       = 'v1'

VERSION_INPUT_DEG_EDGE_SIZE      = '1'
VERSION_PUBLIC_DEG_EDGE_SIZE     = 'v1'

VERSION_INPUT_DEG_PATCH_SIZE     = '1'
VERSION_PUBLIC_DEG_PATCH_SIZE    = 'v1'

VERSION_INPUT_CORAL_REEFS        = '1'
VERSION_PUBLIC_CORAL_REEFS       = 'v1'
# ================================
# ASSET PATHS
# ================================

ASSET_COLLECTION         = f'projects/mapbiomas-{REGION_NAME}/assets/LAND-COVER/COLLECTION-{COLLECTION_ID}'
ASSET_INTEGRATION        = f'{ASSET_COLLECTION}/INTEGRATION/classification-ft'
ASSET_TRANSITIONS        = f'{ASSET_COLLECTION}/INTEGRATION/transitions'
ASSET_DEF_SEC_VEG        = f'{ASSET_COLLECTION}/DEFORESTATION/deforestation-secondary-vegetation-ft'
ASSET_DEF_SEC_VEG_ANN    = f'{ASSET_COLLECTION}/DEFORESTATION/deforestation-secondary-vegetation-annual'
ASSET_DEF_SEC_VEG_ACC    = f'{ASSET_COLLECTION}/DEFORESTATION/deforestation-secondary-vegetation-accumulated'
ASSET_DEF_FREQ           = f'{ASSET_COLLECTION}/DEFORESTATION/deforestation-frequency'
ASSET_SEC_VEG_AGE        = f'{ASSET_COLLECTION}/DEFORESTATION/secondary-vegetation-age'
ASSET_MINING_SUBSTANCES  = f'projects/solved-mb10/assets/LANDSAT/MINING/col10_substances' # TODO: Update this path when the asset is available
ASSET_PASTURE_VIGOR      = f'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_pasture_vigor_v2'
ASSET_PASTURE_BIOMASS    = f'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_pasture_gpp_v1'
ASSET_URBAN_HAND         = f'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_urban_height_above_nearest_drainage_v1'
ASSET_URBAN_RISK         = f'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_urban_risk_v1'
ASSET_URBAN_SLOPE        = f'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_urban_slope_v1'
ASSET_URBAN_SLUM         = f'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_urban_slum_v1'
ASSET_URBAN_TRACTS       = f'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_urban_tracts_v1'
ASSET_URBAN_EPOCHS       = f'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_urban_epochs_v1'
ASSET_WATER_SURFACE      = f'projects/nexgenmap/TRANSVERSAIS/AGUA5-FT'
ASSET_WATER_BODIES       = f'projects/mapbiomas-workspace/AMOSTRAS/GTAGUA/OBJETOS/CLASSIFICADOS/TESTE_1_raster'
ASSET_SOIL_SAND_FRACTION = f'projects/mapbiomas-public/assets/brazil/soil/collection2/mapbiomas_soil_collection2_granulometry_sand_percentage'
ASSET_SOIL_CLAY_FRACTION = f'projects/mapbiomas-public/assets/brazil/soil/collection2/mapbiomas_soil_collection2_granulometry_clay_percentage'
ASSET_SOIL_SILT_FRACTION = f'projects/mapbiomas-public/assets/brazil/soil/collection2/mapbiomas_soil_collection2_granulometry_silt_percentage'
ASSET_SOIL_TEXTURAL_GROUP = f'projects/mapbiomas-public/assets/brazil/soil/collection2/mapbiomas_soil_collection2_textural_groups'
ASSET_SOIL_TEXTURAL_SUBGROUP = f'projects/mapbiomas-public/assets/brazil/soil/collection2/mapbiomas_soil_collection2_textural_subgroups'
ASSET_SOIL_TEXTURAL_CLASS = f'projects/mapbiomas-public/assets/brazil/soil/collection2/mapbiomas_soil_collection2_textural_classes'
ASSET_SOIL_CARBON        = f'projects/mapbiomas-public/assets/brazil/soil/collection2/mapbiomas_soil_collection2_soc_t_ha_000_030cm'
ASSET_AGRI_IRRIGATION    = f'{ASSET_COLLECTION}/AGRICULTURE/irrigation-systems'
ASSET_AGRI_NUM_CYCLES    = f'{ASSET_COLLECTION}/AGRICULTURE/number-of-cycles'
ASSET_DEG_EDGE_SIZE      = f'projects/mapbiomas-brazil/assets/DEGRADATION/COLLECTION-10/edge-area'
ASSET_DEG_PATCH_SIZE     = f'projects/mapbiomas-brazil/assets/DEGRADATION/COLLECTION-10/patch-size'
ASSET_CORAL_REEFS        = f'{ASSET_COLLECTION}/REEFS/classification'

ASSET_QUALITY             = 'projects/mapbiomas-workspace/COLECAO7/qualidade'

PUBLIC_LULC_COLLECTION    = f'{PROJECT_PUBLIC_PATH}/{REGION_NAME}/lulc/collection{COLLECTION_ID}'
PUBLIC_SOIL_COLLECTION    = f'{PROJECT_PUBLIC_PATH}/{REGION_NAME}/soil/collection{SOIL_COLLECTION_ID}'
PUBLIC_WATER_COLLECTION   = f'{PROJECT_PUBLIC_PATH}/{REGION_NAME}/water/collection{WATER_COLLECTION_ID}'

PUBLIC_INTEGRATION            = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_coverage_{VERSION_PUBLIC_INTEGRATION}'
PUBLIC_TRANSITIONS            = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_transitions_{VERSION_PUBLIC_TRANSITIONS}'
PUBLIC_DEF_SEC_VEG            = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_deforestation_secondary_vegetation_{VERSION_PUBLIC_DEF_SEC_VEG}'
PUBLIC_DEF_SEC_VEG_ANN        = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_deforestation_secondary_vegetation_{VERSION_PUBLIC_DEF_SEC_VEG_ANN}'
PUBLIC_DEF_SEC_VEG_ACC        = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_deforestation_secondary_vegetation_accumulated_{VERSION_PUBLIC_DEF_SEC_VEG_ACC}'
PUBLIC_DEF_FREQUENCY          = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_deforestation_frequency_{VERSION_PUBLIC_DEF_FREQ}'
PUBLIC_SEC_VEG_AGE            = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_secondary_vegetation_age_{VERSION_PUBLIC_SEC_VEG_AGE}'
PUBLIC_MINING_SUBSTANCES      = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_mining_substances_{VERSION_PUBLIC_MINING_SUBSTANCES}'
PUBLIC_PASTURE_VIGOR          = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_pasture_vigor_{VERSION_PUBLIC_PASTURE_VIGOR}'
PUBLIC_PASTURE_BIOMASS        = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_pasture_biomass_{VERSION_PUBLIC_PASTURE_BIOMASS}'
PUBLIC_URBAN_HAND             = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_urban_height_above_nearest_drainage_{VERSION_PUBLIC_URBAN_HAND}'
PUBLIC_URBAN_RISK             = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_urban_risk_{VERSION_PUBLIC_URBAN_RISK}'
PUBLIC_URBAN_SLOPE            = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_urban_slope_{VERSION_PUBLIC_URBAN_SLOPE}'
PUBLIC_URBAN_SLUM             = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_urban_slum_{VERSION_PUBLIC_URBAN_SLUM}'
PUBLIC_URBAN_TRACTS           = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_urban_tracts_{VERSION_PUBLIC_URBAN_TRACTS}'
PUBLIC_URBAN_EPOCHS           = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_urban_epochs_{VERSION_INPUT_URBAN_EPOCHS}'
PUBLIC_WATER_SURFACE          = f'{PUBLIC_WATER_COLLECTION}/mapbiomas_{REGION_NAME}_collection{WATER_COLLECTION_ID}_water_{VERSION_PUBLIC_WATER_SURFACE}'
PUBLIC_WATER_BODIES           = f'{PUBLIC_WATER_COLLECTION}/mapbiomas_{REGION_NAME}_collection{WATER_COLLECTION_ID}_water_bodies_{VERSION_PUBLIC_WATER_BODIES}'
PUBLIC_SOIL_SAND_FRACTION     = f'{PUBLIC_SOIL_COLLECTION}/mapbiomas_{REGION_NAME}_collection{SOIL_COLLECTION_ID}_soil_sand_fraction_{VERSION_PUBLIC_SOIL_SAND_FRACTION}'
PUBLIC_SOIL_CLAY_FRACTION     = f'{PUBLIC_SOIL_COLLECTION}/mapbiomas_{REGION_NAME}_collection{SOIL_COLLECTION_ID}_soil_clay_fraction_{VERSION_PUBLIC_SOIL_CLAY_FRACTION}'
PUBLIC_SOIL_SILT_FRACTION     = f'{PUBLIC_SOIL_COLLECTION}/mapbiomas_{REGION_NAME}_collection{SOIL_COLLECTION_ID}_soil_silt_fraction_{VERSION_PUBLIC_SOIL_SILT_FRACTION}'
PUBLIC_SOIL_TEXTURAL_GROUP    = f'{PUBLIC_SOIL_COLLECTION}/mapbiomas_{REGION_NAME}_collection{SOIL_COLLECTION_ID}_soil_textural_group_{VERSION_PUBLIC_SOIL_TEXTURAL_GROUP}'
PUBLIC_SOIL_TEXTURAL_SUBGROUP = f'{PUBLIC_SOIL_COLLECTION}/mapbiomas_{REGION_NAME}_collection{SOIL_COLLECTION_ID}_soil_textural_subgroup_{VERSION_PUBLIC_SOIL_TEXTURAL_SUBGROUP}'
PUBLIC_SOIL_TEXTURAL_CLASS    = f'{PUBLIC_SOIL_COLLECTION}/mapbiomas_{REGION_NAME}_collection{SOIL_COLLECTION_ID}_soil_textural_class_{VERSION_PUBLIC_SOIL_TEXTURAL_CLASS}'
PUBLIC_SOIL_CARBON           = f'{PUBLIC_SOIL_COLLECTION}/mapbiomas_{REGION_NAME}_collection{SOIL_COLLECTION_ID}_soil_carbon_{VERSION_PUBLIC_SOIL_CARBON}'
PUBLIC_AGRICULTURE_IRRIGATION        = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_agriculture_irrigation_systems_{VERSION_PUBLIC_AGRICULTURE_IRRIGATION}'
PUBLIC_AGRICULTURE_NUM_CYCLES        = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_agriculture_number_cycles_{VERSION_PUBLIC_AGRICULTURE_NUM_CYCLES}'
PUBLIC_AGRICULTURE_NUM_CYCLES_MEAN   = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_agriculture_number_cycles_mean_{VERSION_PUBLIC_AGRICULTURE_NUM_CYCLES_MEAN}'
PUBLIC_DEG_EDGE_SIZE          = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_degradation_edge_size_{VERSION_PUBLIC_DEG_EDGE_SIZE}'
PUBLIC_DEG_PATCH_SIZE         = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_degradation_patch_size_{VERSION_PUBLIC_DEG_PATCH_SIZE}'
PUBLIC_CORAL_REEFS            = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_reefs_{VERSION_PUBLIC_CORAL_REEFS}'
PUBLIC_QUALITY                = f'{PUBLIC_LULC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_quality_{VERSION_PUBLIC_QUALITY}'
PUBLIC_COVERAGE_DATA_CATALOG  = f'{PROJECT_PUBLIC_PATH}/{REGION_NAME}/lulc/v1'
ASSET_COVERAGE_CATALOG        = PUBLIC_INTEGRATION

# ================================
# YEARS QUALITY, TRANSITION PERIODS
# ================================
YEARS                 = list(range(1985, 2025))
YEARS_MINING_SUBSTANCES = list(range(1985, 2024))
YEARS_PASTURE_VIGOR   = list(range(2000, 2024))
YEARS_PASTURE_BIOMASS = list(range(2000, 2023))
YEARS_URBAN_HAND      = list(range(1985, 2024))
YEARS_URBAN_RISK      = list(range(1985, 2024))
YEARS_URBAN_SLOPE     = list(range(1985, 2024))
YEARS_URBAN_SLUM      = list(range(1985, 2024))
YEARS_URBAN_TRACTS    = list(range(1985, 2024))
YEARS_WATER_SURFACE   = list(range(1985, 2025))
YEARS_WATER_BODIES    = list(range(1985, 2025))
YEARS_AGRICULTURE_NUM_CYCLES = list(range(2017, 2025))
YEARS_AGRICULTURE_IRRIGATION = list(range(1985, 2025))
YEARS_DEG_EDGE_SIZE   = list(range(1985, 2025))
YEARS_DEG_PATCH_SIZE  = list(range(1985, 2025))
YEARS_SOIL_CARBON     = list(range(1985, 2024))
YEARS_CORAL_REEFS     = list(range(1985, 2025))
YEARS_QUALITY         = list(range(1985, 2025))

TRANSITION_PERIODS = [
    (1985, 1986), (1986, 1987), (1987, 1988), (1988, 1989), (1989, 1990),
    (1990, 1991), (1991, 1992), (1992, 1993), (1993, 1994), (1994, 1995),
    (1995, 1996), (1996, 1997), (1997, 1998), (1998, 1999), (1999, 2000),
    (2000, 2001), (2001, 2002), (2002, 2003), (2003, 2004), (2004, 2005),
    (2005, 2006), (2006, 2007), (2007, 2008), (2008, 2009), (2009, 2010),
    (2010, 2011), (2011, 2012), (2012, 2013), (2013, 2014), (2014, 2015),
    (2015, 2016), (2016, 2017), (2017, 2018), (2018, 2019), (2019, 2020),
    (2020, 2021), (2021, 2022), (2022, 2023), (2023, 2024), (1985, 2024)
]

SOIL_DEPTHS = [
    ['000', '010'], 
    ['010', '020'], 
    ['020', '030'], 
    ['000', '020'], 
    ['000', '030']
]

SOIL_DEPTHS_TEXTURAL = [
    ['000', '010'], 
    ['000', '020'], 
    ['000', '030'], 
]

SOIL_DEPTHS_CARBON = [
    ['000', '030'], 
]
# ================================
# EXECUTION CONTROL
# ================================
RUN_EXPORT_INTEGRATION            = False
RUN_EXPORT_TRANSITIONS            = False
RUN_EXPORT_DEF_SEC_VEG            = False
RUN_EXPORT_DEF_SEC_VEG_ANN        = False
RUN_EXPORT_DEF_SEC_VEG_ACC        = False
RUN_EXPORT_DEF_FREQ               = False
RUN_EXPORT_SEC_VEG_AGE            = False
RUN_EXPORT_QUALITY                = False
RUN_EXPORT_MINING_SUBSTANCES      = False
RUN_EXPORT_PASTURE_VIGOR          = False
RUN_EXPORT_PASTURE_BIOMASS        = False
RUN_EXPORT_URBAN_EPOCHS           = False
RUN_EXPORT_URBAN_HAND             = False
RUN_EXPORT_URBAN_RISK             = False
RUN_EXPORT_URBAN_SLOPE            = False
RUN_EXPORT_URBAN_SLUM             = False
RUN_EXPORT_URBAN_TRACTS           = False
RUN_EXPORT_URBAN_EPOCHS           = False
RUN_EXPORT_WATER_SURFACE          = False
RUN_EXPORT_WATER_BODIES           = False
RUN_EXPORT_SOIL_SAND_FRACTION     = False
RUN_EXPORT_SOIL_CLAY_FRACTION     = False
RUN_EXPORT_SOIL_SILT_FRACTION     = False
RUN_EXPORT_SOIL_TEXTURAL_GROUP    = False
RUN_EXPORT_SOIL_TEXTURAL_SUBGROUP = False
RUN_EXPORT_SOIL_TEXTURAL_CLASS    = False
RUN_EXPORT_SOIL_CARBON            = False
RUN_EXPORT_AGRI_IRRIGATION        = False
RUN_EXPORT_AGRI_NUM_CYCLES        = False
RUN_EXPORT_AGRI_NUM_CYCLES_MEAN   = False
RUN_EXPORT_DEG_EDGE_SIZE          = False
RUN_EXPORT_DEG_PATCH_SIZE         = False
RUN_EXPORT_CORAL_REEFS            = False
RUN_EXPORT_COVERAGE_DATA_CATALOG  = True


# ================================
# TASK MONITORING
# ================================

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



def export_public_mining_substances():
    """
    Exports a multi-band image with mining substances for each year.
    Each band corresponds to a year, named 'classification_YEAR'.
    The image is masked with the territory mask and exported to a public asset.
    """
    band_images = []

    for year in YEARS:
        asset_id = f'{ASSET_MINING_SUBSTANCES}/{year}-{VERSION_INPUT_MINING_SUBSTANCES}'
        try:
            image = ee.Image(asset_id).rename(f'classification_{year}')
            band_images.append(image)
            print(f'✅ Loaded: {asset_id}')
        except Exception as e:
            print(f'❌ Failed to load {asset_id}: {e}')

    if not band_images:
        print('⚠️ No images loaded. Aborting export.')
        return

    image = ee.Image.cat(band_images)

    return export_image_to_asset(image, PUBLIC_MINING_SUBSTANCES, version=VERSION_PUBLIC_MINING_SUBSTANCES)


def export_public_pasture_vigor():
    """
    Exports the pasture vigor image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_PASTURE_VIGOR)

        original_bands = [f'classification_{year}' for year in YEARS_PASTURE_VIGOR]
        renamed_bands = [f'classification_{year}' for year in YEARS_PASTURE_VIGOR]

        image = image.select(original_bands, renamed_bands)
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return export_image_to_asset(image, PUBLIC_PASTURE_VIGOR, version=VERSION_PUBLIC_PASTURE_VIGOR)


def export_public_pasture_biomass():
    """
    Exports the pasture GPP image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_PASTURE_BIOMASS)

        original_bands = [f'gpp_{year}' for year in YEARS_PASTURE_BIOMASS]
        renamed_bands = [f'classification_{year}' for year in YEARS_PASTURE_BIOMASS]

        image = image.select(original_bands, renamed_bands)
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return export_image_to_asset(image, PUBLIC_PASTURE_BIOMASS, version=VERSION_PUBLIC_PASTURE_BIOMASS)


def export_public_urban_epochs():
    """
    Exports the urban epochs image for the region.
    This function filters the collection by version and exports the minimum image.
    """


def export_public_urban_height_above_nearest_drainage():
    """
    Exports the urban height above nearest drainage image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_URBAN_HAND)

        original_bands = [f'classification_{year}' for year in YEARS_URBAN_HAND]
        renamed_bands = [f'classification_{year}' for year in YEARS_URBAN_HAND]

        image = image.select(original_bands, renamed_bands)
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return export_image_to_asset(image, PUBLIC_URBAN_HAND, version=VERSION_PUBLIC_URBAN_HAND)


def export_public_urban_risk():
    """
    Exports the urban risk image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_URBAN_RISK)

        original_bands = [f'classification_{year}' for year in YEARS_URBAN_RISK]
        renamed_bands = [f'classification_{year}' for year in YEARS_URBAN_RISK]

        image = image.select(original_bands, renamed_bands)
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return export_image_to_asset(image, PUBLIC_URBAN_RISK, version=VERSION_PUBLIC_URBAN_RISK)


def export_public_urban_slope():
    """
    Exports the urban slope image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_URBAN_SLOPE)

        original_bands = [f'classification_{year}' for year in YEARS_URBAN_SLOPE]
        renamed_bands = [f'classification_{year}' for year in YEARS_URBAN_SLOPE]

        image = image.select(original_bands, renamed_bands)
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return export_image_to_asset(image, PUBLIC_URBAN_SLOPE, version=VERSION_PUBLIC_URBAN_SLOPE)


def export_public_urban_slum():
    """
    Exports the urban slum image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_URBAN_SLUM)

        original_bands = [f'classification_{year}' for year in YEARS_URBAN_SLUM]
        renamed_bands = [f'classification_{year}' for year in YEARS_URBAN_SLUM]

        image = image.select(original_bands, renamed_bands)
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return export_image_to_asset(image, PUBLIC_URBAN_SLUM, version=VERSION_PUBLIC_URBAN_SLUM)


def export_public_urban_tracts():
    """
    Exports the urban tracts image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_URBAN_TRACTS)

        original_bands = [f'classification_{year}' for year in YEARS_URBAN_TRACTS]
        renamed_bands = [f'classification_{year}' for year in YEARS_URBAN_TRACTS]

        image = image.select(original_bands, renamed_bands)
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return export_image_to_asset(image, PUBLIC_URBAN_TRACTS, version=VERSION_PUBLIC_URBAN_TRACTS)


def export_public_urban_epochs():
    """
    Exports the urban epochs image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_URBAN_EPOCHS)

        image = image.select(['epochs'], ['classification'])
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    properties = {
        "module": "Urban",
        "submodule": "Periods",
        "variable": "Urbanization epochs",
        "version": VERSION_PUBLIC_URBAN_EPOCHS,
    }
    
    return export_image_to_asset(image, 
                                 PUBLIC_URBAN_EPOCHS, 
                                 version=VERSION_PUBLIC_URBAN_EPOCHS,
                                 additional_properties=properties)


def export_public_soil_sand_fraction():
    """
    Exports the soil sand fraction image for the region.
    This function filters the collection by version and exports the minimum image.

    returns a list of tasks for each depth band.
    """
    try:
        image = ee.Image(ASSET_SOIL_SAND_FRACTION)

        collection_properties = {
            "module": "Soil",
            "submodule": "Texture",
            "variable": "Sand Fraction",
            "band_format": "sand_fraction",
            "format": "stratified_continuous_singleband_image",
            "stratification": "depth",
            "depths": json.dumps(SOIL_DEPTHS),
            "unit": "cm",
        }

        set_asset_properties(PUBLIC_SOIL_SAND_FRACTION, collection_properties)

        tasks = []
        for depth_start, depth_end in SOIL_DEPTHS:
            original_band = f'sand_{depth_start}_{depth_end}cm'

            asset_name = f'sand_fraction_{depth_start}_{depth_end}_{VERSION_PUBLIC_SOIL_SAND_FRACTION}'

            image_band = image.select([original_band], ['sand_fraction'])

            properties = {
                "module": "Soil",
                "submodule": "Texture",
                "variable": "Sand Fraction",
                "depth": f"{depth_start}_{depth_end}",
                "depth_start": depth_start,
                "depth_end": depth_end,
                "version": VERSION_PUBLIC_SOIL_SAND_FRACTION,
            }

            tasks.append(export_image_to_asset(image_band, 
                                                f'{PUBLIC_SOIL_SAND_FRACTION}/{asset_name}', 
                                                additional_properties=properties))
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return tasks


def export_public_soil_clay_fraction():
    """
    Exports the soil clay fraction image for the region.
    This function filters the collection by version and exports the minimum image.

    returns a list of tasks for each depth band.
    """
    try:
        image = ee.Image(ASSET_SOIL_CLAY_FRACTION)

        collection_properties = {
            "module": "Soil",
            "submodule": "Texture",
            "variable": "Clay Fraction",
            "band_format": "clay_fraction",
            "format": "stratified_continuous_singleband_image",
            "stratification": "depth",
            "depths": json.dumps(SOIL_DEPTHS),
            "unit": "cm",
        }

        set_asset_properties(PUBLIC_SOIL_CLAY_FRACTION, collection_properties)

        tasks = []
        for depth_start, depth_end in SOIL_DEPTHS:
            original_band = f'clay_{depth_start}_{depth_end}cm'

            asset_name = f'clay_fraction_{depth_start}_{depth_end}_{VERSION_PUBLIC_SOIL_CLAY_FRACTION}'

            image_band = image.select([original_band], ['clay_fraction'])

            properties = {
                "module": "Soil",
                "submodule": "Texture",
                "variable": "Clay Fraction",
                "depth": f"{depth_start}_{depth_end}",
                "version": VERSION_PUBLIC_SOIL_CLAY_FRACTION,
            }

            tasks.append(export_image_to_asset(image_band, 
                                                f'{PUBLIC_SOIL_CLAY_FRACTION}/{asset_name}', 
                                                additional_properties=properties))
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return tasks


def export_public_soil_silt_fraction():
    """
    Exports the soil silt fraction image for the region.
    This function filters the collection by version and exports the minimum image.

    returns a list of tasks for each depth band.
    """
    try:
        image = ee.Image(ASSET_SOIL_SILT_FRACTION)

        collection_properties = {
            "module": "Soil",
            "submodule": "Texture",
            "variable": "Silt Fraction",
            "band_format": "silt_fraction",
            "format": "stratified_continuous_singleband_image",
            "stratification": "depth",
            "depths": json.dumps(SOIL_DEPTHS),
            "unit": "cm",
        }

        set_asset_properties(PUBLIC_SOIL_SILT_FRACTION, collection_properties)

        tasks = []
        for depth_start, depth_end in SOIL_DEPTHS:
            original_band = f'silt_{depth_start}_{depth_end}cm'

            asset_name = f'silt_fraction_{depth_start}_{depth_end}_{VERSION_PUBLIC_SOIL_SILT_FRACTION}'

            image_band = image.select([original_band], ['silt_fraction'])

            properties = {
                "module": "Soil",
                "submodule": "Texture",
                "variable": "Silt Fraction",
                "depth": f"{depth_start}_{depth_end}",
                "version": VERSION_PUBLIC_SOIL_SILT_FRACTION,
            }

            tasks.append(export_image_to_asset(image_band, 
                                                f'{PUBLIC_SOIL_SILT_FRACTION}/{asset_name}', 
                                                additional_properties=properties))
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return tasks


def export_public_soil_textural_group():
    """
    Exports the soil textural group image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_SOIL_TEXTURAL_GROUP)

        collection_properties = {
            "module": "Soil",
            "submodule": "Texture",
            "variable": "Textural Group",
            "band_format": "textural_group",
            "format": "classification_singleband_image",
            "stratification": "depth",
            "depths": json.dumps(SOIL_DEPTHS_TEXTURAL),
            "unit": "cm",
        }

        set_asset_properties(PUBLIC_SOIL_TEXTURAL_GROUP, collection_properties)

        tasks = []
        for depth_start, depth_end in SOIL_DEPTHS_TEXTURAL:
            original_band = f'textural_groups_{depth_start}_{depth_end}cm'

            asset_name = f'textural_group_{depth_start}_{depth_end}_{VERSION_PUBLIC_SOIL_TEXTURAL_GROUP}'

            image_band = image.select([original_band], ['textural_group'])

            properties = {
                "module": "Soil",
                "submodule": "Texture",
                "variable": "Textural Group",
                "depth": f"{depth_start}_{depth_end}",
                "version": VERSION_PUBLIC_SOIL_TEXTURAL_GROUP,
            }

            tasks.append(export_image_to_asset(image_band, 
                                                f'{PUBLIC_SOIL_TEXTURAL_GROUP}/{asset_name}', 
                                                additional_properties=properties))
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return tasks


def export_public_soil_textural_subgroup():
    """
    Exports the soil textural subgroup image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_SOIL_TEXTURAL_SUBGROUP)

        collection_properties = {
            "module": "Soil",
            "submodule": "Texture",
            "variable": "Textural Subgroup",
            "band_format": "textural_subgroup",
            "format": "classification_singleband_image",
            "stratification": "depth",
            "depths": json.dumps(SOIL_DEPTHS_TEXTURAL),
            "unit": "cm",
        }

        set_asset_properties(PUBLIC_SOIL_TEXTURAL_SUBGROUP, collection_properties)

        tasks = []
        for depth_start, depth_end in SOIL_DEPTHS_TEXTURAL:
            original_band = f'textural_subgroups_{depth_start}_{depth_end}cm'

            asset_name = f'textural_subgroup_{depth_start}_{depth_end}_{VERSION_PUBLIC_SOIL_TEXTURAL_SUBGROUP}'

            image_band = image.select([original_band], ['textural_subgroup'])

            properties = {
                "module": "Soil",
                "submodule": "Texture",
                "variable": "Textural Subgroup",
                "depth": f"{depth_start}_{depth_end}",
                "version": VERSION_PUBLIC_SOIL_TEXTURAL_SUBGROUP,
            }

            tasks.append(export_image_to_asset(image_band, 
                                                f'{PUBLIC_SOIL_TEXTURAL_SUBGROUP}/{asset_name}', 
                                                additional_properties=properties))
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return tasks


def export_public_soil_textural_class():
    """
    Exports the soil textural class image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_SOIL_TEXTURAL_CLASS)

        collection_properties = {
            "module": "Soil",
            "submodule": "Texture",
            "variable": "Textural Class",
            "band_format": "textural_class",
            "format": "classification_singleband_image",
            "stratification": "depth",
            "depths": json.dumps(SOIL_DEPTHS_TEXTURAL),
            "unit": "cm",
        }

        set_asset_properties(PUBLIC_SOIL_TEXTURAL_CLASS, collection_properties)

        tasks = []
        for depth_start, depth_end in SOIL_DEPTHS_TEXTURAL:
            original_band = f'textural_classes_{depth_start}_{depth_end}cm'

            asset_name = f'textural_class_{depth_start}_{depth_end}_{VERSION_PUBLIC_SOIL_TEXTURAL_CLASS}'

            image_band = image.select([original_band], ['textural_class'])

            properties = {
                "module": "Soil",
                "submodule": "Texture",
                "variable": "Textural Class",
                "depth": f"{depth_start}_{depth_end}",
                "version": VERSION_PUBLIC_SOIL_TEXTURAL_CLASS,
            }

            tasks.append(export_image_to_asset(image_band, 
                                                f'{PUBLIC_SOIL_TEXTURAL_CLASS}/{asset_name}', 
                                                additional_properties=properties))
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return tasks


def export_public_soil_carbon():
    """
    Exports the soil carbon image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = ee.Image(ASSET_SOIL_CARBON)

        original_bands = [f'prediction_{year}' for year in YEARS_SOIL_CARBON]
        renamed_bands = [f'carbon_{year}' for year in YEARS_SOIL_CARBON]

        image = image.select(original_bands, renamed_bands)
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    properties = {
        "module": "Soil",
        "submodule": "Carbon",
        "variable": "Soil Organic Carbon",
        "band_format": "carbon_{year}",
        "data_type": "annual",
        "version": VERSION_PUBLIC_SOIL_CARBON,
        "format": "continuous_multiband_image",
        "unit": "t/ha",
    }

    return export_image_to_asset(image, 
                                 PUBLIC_SOIL_CARBON, 
                                 additional_properties=properties)


def export_public_agriculture_irrigation_system():
    """
    Loads the irrigated agriculture image, renames its bands from
    'classification' to 'classification_{year}', and exports
    to a new public asset.
    """
    try:
        image = (
            ee.ImageCollection(ASSET_AGRI_IRRIGATION)
                 .filter(ee.Filter.eq('version', VERSION_INPUT_AGRICULTURE_IRRIGATION))
                 ).toBands()

        image = image.rename([f'classification_{year}' for year in YEARS_AGRICULTURE_IRRIGATION])


    except Exception as e:
        print(f'❌ Failed to export: {e}')

    properties = {
        "module": "Agriculture",
        "submodule": "Irrigation",
        "variable": "Irrigation System",
        "format": "classification_multiband_image",
        "band_format": "classification_{year}",
        "data_type": "annual",
        "version": VERSION_PUBLIC_AGRICULTURE_IRRIGATION,
    }

    return export_image_to_asset(image, PUBLIC_AGRICULTURE_IRRIGATION, additional_properties=properties)


def export_public_agriculture_number_cycles():
    """
    Exports the agriculture number of cycles image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = (
            ee.ImageCollection(ASSET_AGRI_NUM_CYCLES)
                  .filter(ee.Filter.eq('version', VERSION_INPUT_AGRICULTURE_NUM_CYCLES))
                  .first())

        original_bands = [f'b{year}' for year in YEARS_AGRICULTURE_NUM_CYCLES]
        renamed_bands = [f'classification_{year}' for year in YEARS_AGRICULTURE_NUM_CYCLES]

        image = image.select(original_bands, renamed_bands)
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    properties = {
        "module": "Agriculture",  
        "submodule": "Agriculture",  
        "variable": "Annual cropping frequency",  
        "format": "classification_multiband_image",
        "unit": "cycles",
        "band_format": "classification_{year}",
        "data_type": "annual",
        "version": VERSION_PUBLIC_AGRICULTURE_NUM_CYCLES,
    }

    return export_image_to_asset(image, 
                                 PUBLIC_AGRICULTURE_NUM_CYCLES, 
                                 scale=EXPORT_SCALE_10M,
                                 additional_properties=properties)


def export_public_agriculture_number_cycles_mean():
    """
    Exports the mean number of agriculture cycles image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = (
            ee.ImageCollection(ASSET_AGRI_NUM_CYCLES)
                  .filter(ee.Filter.eq('version', VERSION_INPUT_AGRICULTURE_NUM_CYCLES))
                  .first())

        original_bands = [f'b{year}' for year in YEARS_AGRICULTURE_NUM_CYCLES]
        renamed_bands = [f'classification_{year}' for year in YEARS_AGRICULTURE_NUM_CYCLES]

        image = image.select(original_bands, renamed_bands).reduce(ee.Reducer.mean()).rename('cycles_mean')

    except Exception as e:
        print(f'❌ Failed to export: {e}')

    properties = {
        "module": "Agriculture",
        "submodule": "Agriculture",
        "variable": "Mean cropping frequency",
        "format": "continuos_singleband_image",
        "unit": "cycles",
        "band_format": "cycles_mean",
        "data_type": "constant",
        "version": VERSION_PUBLIC_AGRICULTURE_NUM_CYCLES_MEAN,
    }

    return export_image_to_asset(image, 
                                 PUBLIC_AGRICULTURE_NUM_CYCLES_MEAN, 
                                 scale=EXPORT_SCALE_10M,
                                 additional_properties=properties)

def export_public_degradation_edge_size():
    """
    Exports the degradation edge size image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        collection = ee.ImageCollection(ASSET_DEG_EDGE_SIZE)
        
        properties = {
            "module": "Degradation",
            "submodule": "Fragmentation",
            "variable": "Edge size",
            "band_format": "edge_size_{year}",
            "format": "continuos_multiband_image",
            "type": "annual",
            "unit": "m",
            "version": VERSION_PUBLIC_DEG_EDGE_SIZE,
        }

        band_images = []

        for year in YEARS_DEG_EDGE_SIZE:
            image_year = collection.filter(ee.Filter.eq('year', year)).first().rename(f'edge_size_{year}')

            band_images.append(image_year)

        image = ee.Image.cat(band_images)
        
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return export_image_to_asset(image, 
                            f'{PUBLIC_DEG_EDGE_SIZE}', 
                            additional_properties=properties)


def export_public_degradation_patch_size():
    """
    Exports the degradation patch size image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        collection = ee.ImageCollection(ASSET_DEG_PATCH_SIZE)

        properties = {
            "module": "Degradation",
            "submodule": "Fragmentation",
            "variable": "Patch size",
            "band_format": "patch_size_{year}",
            "format": "continuos_multiband_image",
            "type": "annual",
            "unit": "m",
            "version": VERSION_PUBLIC_DEG_PATCH_SIZE,
        }

        band_images = []

        for year in YEARS_DEG_PATCH_SIZE:
            image_year = collection.filter(ee.Filter.eq('year', year)).first().rename(f'patch_size_{year}')

            band_images.append(image_year)

        image = ee.Image.cat(band_images)
        
    except Exception as e:
        print(f'❌ Failed to export: {e}')

    return export_image_to_asset(image, 
                            f'{PUBLIC_DEG_PATCH_SIZE}', 
                            additional_properties=properties)


def export_public_coral_reefs():
    """
    Exports the coral reefs image for the region.
    This function filters the collection by version and exports the minimum image.
    """
    try:
        image = (
            ee.ImageCollection(ASSET_CORAL_REEFS)
                 .filter(ee.Filter.eq('version', VERSION_INPUT_CORAL_REEFS))
                 .filter(ee.Filter.eq('year', YEARS_CORAL_REEFS[-1]))
                ).toBands()
        
        image = image.rename([f'classification'])

    except Exception as e:
        print(f'❌ Failed to export: {e}')

    properties = {
        "module": "Coral Reefs",
        "submodule": "Coral Reefs",
        "variable": "Coral Reefs",
        "format": "categorical_singleband_image",
        "band_format": "classification_{year}",
        "data_type": "constant",
        "version": VERSION_PUBLIC_CORAL_REEFS,
    }

    return export_image_to_asset(image, 
                                 PUBLIC_CORAL_REEFS, 
                                 additional_properties=properties)

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

if RUN_EXPORT_COVERAGE_DATA_CATALOG:
    export_tasks.extend(export_coverage_gee_data_catalog(
        export_image_to_asset,
        YEARS, PUBLIC_COVERAGE_DATA_CATALOG, ASSET_COVERAGE_CATALOG,
        COLLECTION_ID, VERSION_PUBLIC_INTEGRATION, REGION_NAME,
    ))

if RUN_EXPORT_TRANSITIONS:
    export_tasks.append(export_public_transitions())

if RUN_EXPORT_DEF_SEC_VEG:
    export_tasks.append(export_collection_product(
        export_image_to_asset,
        ASSET_DEF_SEC_VEG, VERSION_INPUT_DEF_SEC_VEG,
        PUBLIC_DEF_SEC_VEG, VERSION_PUBLIC_DEF_SEC_VEG,
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

if RUN_EXPORT_MINING_SUBSTANCES:
    export_tasks.append(export_public_mining_substances())

if RUN_EXPORT_AGRI_IRRIGATION:
    export_tasks.append(export_public_agriculture_irrigation_system())

if RUN_EXPORT_AGRI_NUM_CYCLES_MEAN:
    export_tasks.append(export_public_agriculture_number_cycles_mean())

if RUN_EXPORT_PASTURE_VIGOR:
    export_tasks.append(export_public_pasture_vigor())

if RUN_EXPORT_PASTURE_BIOMASS:
    export_tasks.append(export_public_pasture_biomass())

if RUN_EXPORT_URBAN_EPOCHS:
    export_tasks.append(export_public_urban_epochs())

if RUN_EXPORT_URBAN_HAND:
    export_tasks.append(export_public_urban_height_above_nearest_drainage())

if RUN_EXPORT_URBAN_RISK:
    export_tasks.append(export_public_urban_risk())

if RUN_EXPORT_URBAN_SLOPE:
    export_tasks.append(export_public_urban_slope())

if RUN_EXPORT_URBAN_SLUM:
    export_tasks.append(export_public_urban_slum())

if RUN_EXPORT_URBAN_TRACTS:
    export_tasks.append(export_public_urban_tracts())

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
        class_remap={'from': [1, 2, 3, 4, 5, 6], 'to': [1, 3, 4, 5, 6, 7]},
    ))

if RUN_EXPORT_SOIL_SAND_FRACTION:
    export_tasks.extend(export_public_soil_sand_fraction())

if RUN_EXPORT_SOIL_CLAY_FRACTION:
    export_tasks.extend(export_public_soil_clay_fraction())

if RUN_EXPORT_SOIL_SILT_FRACTION:
    export_tasks.extend(export_public_soil_silt_fraction())

if RUN_EXPORT_SOIL_TEXTURAL_GROUP:
    export_tasks.extend(export_public_soil_textural_group())

if RUN_EXPORT_SOIL_TEXTURAL_SUBGROUP:
    export_tasks.extend(export_public_soil_textural_subgroup())

if RUN_EXPORT_SOIL_TEXTURAL_CLASS:
    export_tasks.extend(export_public_soil_textural_class())

if RUN_EXPORT_AGRI_NUM_CYCLES:
    export_tasks.append(export_public_agriculture_number_cycles())

if RUN_EXPORT_DEG_EDGE_SIZE:
    export_tasks.append(export_public_degradation_edge_size())

if RUN_EXPORT_DEG_PATCH_SIZE:
    export_tasks.append(export_public_degradation_patch_size())

if RUN_EXPORT_SOIL_CARBON:
    export_tasks.append(export_public_soil_carbon())

if RUN_EXPORT_CORAL_REEFS:
    export_tasks.append(export_public_coral_reefs())


wait_until_tasks_finish(export_tasks=export_tasks, polling_interval=30)
