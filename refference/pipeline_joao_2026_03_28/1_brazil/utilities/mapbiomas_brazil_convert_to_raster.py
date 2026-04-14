# -*- coding: utf-8 -*-
"""
Rasterize territory vector assets to images for the GEE statistics pipeline.

Supports two rasterization modes:
- 'category': One image per category (all features with FEATURE_ID as pixel values)
- 'feature':  One image per feature (original behavior)

The 'category' mode is recommended for the statistics pipeline as it's more efficient:
- Statistics pipeline groups by FEATURE_ID via Reducer.sum().group(1, 'feature_id')
- One image per category = 1 reduceRegion call per category×period (vs N per feature)
"""

import ee
import sys

sys.path.insert(0, '/Users/joaosiqueira/Documents/Projects/mapbiomas-pipeline')
from utils.export_utils import resolve_territory_asset

ee.Initialize(project='mapbiomas-brazil')

# ============================================================================
# Configuration
# ============================================================================

# Base paths for territory assets (try primary, fallback to staging)
ASSET_FOLDER_PRIMARY = 'projects/mapbiomas-territories/assets/TERRITORIES/BRAZIL/WORKSPACE'
ASSET_FOLDER_STAGING = 'projects/mapbiomas-territories/assets/TERRITORIES-STAGING/BRAZIL/WORKSPACE'

# Destination for rasterized territory images
ASSET_OUTPUT = 'projects/mapbiomas-territories/assets/TERRITORIES/LULC/BRAZIL/COLLECTION10/territory-collection'

VERSION_OUTPUT = 1

# Rasterization mode:
# 'category' → one image per category (efficient for statistics pipeline, recommended)
# 'feature'  → one image per feature (original behavior)
RASTERIZE_MODE = 'category'

# Territory categories with metadata
# Note: VERSION can be overridden by resolve_territory_asset if higher version exists
CATEGORIES = [
    {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": 1, "VERSION": "v1"},
    {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": 2, "VERSION": "v1"},
    {"CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": 3, "VERSION": "v1"},
    {"CATEGORY": "BIOMES", "CATEG_ID": 4, "VERSION": "v1"},
    {"CATEGORY": "COASTAL_MARINE_ZONE", "CATEG_ID": 5, "VERSION": "v1"},
    {"CATEGORY": "BASIN_LEVEL_1_PNRH", "CATEG_ID": 10, "VERSION": "v1"},
    {"CATEGORY": "BASIN_LEVEL_2_PNRH", "CATEG_ID": 11, "VERSION": "v1"},
    {"CATEGORY": "BASIN_LEVEL_1_DNAEE", "CATEG_ID": 12, "VERSION": "v1"},
    {"CATEGORY": "BASIN_LEVEL_2_DNAEE", "CATEG_ID": 13, "VERSION": "v1"},
]

# ============================================================================
# Rasterization Functions
# ============================================================================

def rasterize_category(
    feature_collection,
    category_id,
    category_name,
    asset_output,
    already_in_collection,
    version_output=1
):
    """
    Rasterize all features of a category into a single image.

    Creates one image per category where all features are painted with their
    FEATURE_ID as pixel values. This is efficient for the statistics pipeline
    which groups by feature_id via Reducer.

    Args:
        feature_collection (ee.FeatureCollection): Vector features to rasterize
        category_id (int): Category ID
        category_name (str): Category name (e.g., 'BIOMES')
        asset_output (str): Output asset path
        already_in_collection (list): List of asset names already exported
        version_output (int): Version number for the asset

    Returns:
        None (submits GEE task)
    """
    name = f'{category_id}_{category_name}_{version_output}'

    if name in already_in_collection:
        print(f"📍 {category_name} (id={category_id}) - OK!")
        return

    print(f"📍 {category_name} (id={category_id})")

    # Paint all features into one image with FEATURE_ID as pixel value
    raster = ee.Image().uint32().paint(
        featureCollection=feature_collection,
        color='FEATURE_ID'
    )

    # Add metadata
    raster = (
        raster
            .set('CATEG_ID', category_id)
            .set('CATEG_NAME', category_name)
            .set('version', str(version_output))
            .set('rasterize_mode', 'category')
    )

    # Export to asset
    task = ee.batch.Export.image.toAsset(
        image=raster,
        description=name,
        assetId=f'{asset_output}/{name}',
        pyramidingPolicy={'.default': 'mode'},
        region=feature_collection.geometry().bounds(),
        scale=30,
        maxPixels=1e13,
    )

    task.start()
    print(f"   ✅ Task submitted: {name}")


def rasterize_feature(
    feature_collection,
    category_id,
    category_name,
    asset_output,
    already_in_collection,
    version_output=1
):
    """
    Rasterize each feature into a separate image.

    Creates one image per feature where only that feature is painted with its
    FEATURE_ID value. This is the original behavior of the script.

    Args:
        feature_collection (ee.FeatureCollection): Vector features to rasterize
        category_id (int): Category ID
        category_name (str): Category name (e.g., 'BIOMES')
        asset_output (str): Output asset path
        already_in_collection (list): List of asset names already exported
        version_output (int): Version number for the asset

    Returns:
        None (submits GEE tasks)
    """
    feature_ids = feature_collection.aggregate_array('FEATURE_ID').getInfo()

    for feature_id in feature_ids:
        feature = feature_collection.filter(
            ee.Filter.eq('FEATURE_ID', feature_id)
        )

        name = f'{category_id}_{feature_id}_{version_output}'
        territory_id = f'{category_id}-{feature_id}-{version_output}'
        territory_name = feature.first().get('NAME').getInfo()

        if name in already_in_collection:
            print(f"  🔹 {territory_name} - OK!")
            continue

        print(f"  🔹 {category_name}/{territory_name} (feature_id={feature_id})")

        # Paint only this feature with its FEATURE_ID
        raster = ee.Image().uint32().paint(
            featureCollection=feature,
            color='FEATURE_ID'
        )

        # Add metadata
        raster = (
            raster
                .set('CATEG_ID', category_id)
                .set('CATEG_NAME', category_name)
                .set('FEATURE_ID', feature_id)
                .set('TERRITORY_ID', territory_id)
                .set('NAME', territory_name)
                .set('version', str(version_output))
                .set('rasterize_mode', 'feature')
        )

        # Export to asset
        task = ee.batch.Export.image.toAsset(
            image=raster,
            description=name,
            assetId=f'{asset_output}/{name}',
            pyramidingPolicy={'.default': 'mode'},
            region=feature.geometry().bounds(),
            scale=30,
            maxPixels=1e13,
        )

        task.start()
        print(f"     ✅ Task submitted")


# ============================================================================
# Main Execution
# ============================================================================

def load_feature_collection(category_name, category_version):
    """
    Load feature collection from primary or staging assets.

    Args:
        category_name (str): Category name (e.g., 'BIOMES')
        category_version (str): Version (e.g., 'v1', 'v4')

    Returns:
        tuple: (ee.FeatureCollection, str) or (None, error_message)
    """
    # Try primary location first
    try:
        asset_path = resolve_territory_asset(
            f'{ASSET_FOLDER_PRIMARY}/{category_name}/{category_name}_{category_version}'
        )
        fc = ee.FeatureCollection(asset_path)
        _ = fc.first().getInfo()  # Verify it loads
        return fc, None
    except Exception as e_primary:
        pass

    # Fallback to staging
    try:
        asset_path = resolve_territory_asset(
            f'{ASSET_FOLDER_STAGING}/{category_name}/{category_name}_{category_version}'
        )
        fc = ee.FeatureCollection(asset_path)
        _ = fc.first().getInfo()  # Verify it loads
        return fc, None
    except Exception as e_staging:
        error_msg = f"Primary: {str(e_primary)[:50]}... | Staging: {str(e_staging)[:50]}..."
        return None, error_msg


if __name__ == "__main__":
    # Load already exported assets
    already_in_collection = ee.ImageCollection(
        ASSET_OUTPUT).aggregate_array('system:index').getInfo()

    print(f"\n{'='*80}")
    print(f"🔧 Converting territories to raster (mode: {RASTERIZE_MODE})")
    print(f"🔍 Primary folder: {ASSET_FOLDER_PRIMARY}")
    print(f"📦 Fallback folder: {ASSET_FOLDER_STAGING}")
    print(f"{'='*80}\n")

    # Process each category
    for category_dict in CATEGORIES:
        category_name = category_dict['CATEGORY']
        category_id = category_dict['CATEG_ID']
        category_version = category_dict['VERSION']

        # Load feature collection (try primary, fallback to staging)
        feature_collection, error = load_feature_collection(category_name, category_version)

        if feature_collection is None:
            print(f"⚠️  {category_name} (id={category_id}): Could not load asset")
            print(f"   Error: {error}\n")
            continue

        # Rasterize using appropriate mode
        try:
            if RASTERIZE_MODE == 'category':
                rasterize_category(
                    feature_collection,
                    category_id,
                    category_name,
                    ASSET_OUTPUT,
                    already_in_collection,
                    VERSION_OUTPUT
                )
            elif RASTERIZE_MODE == 'feature':
                rasterize_feature(
                    feature_collection,
                    category_id,
                    category_name,
                    ASSET_OUTPUT,
                    already_in_collection,
                    VERSION_OUTPUT
                )
            else:
                print(f"❌ Unknown RASTERIZE_MODE: {RASTERIZE_MODE}")
                sys.exit(1)
        except Exception as e:
            print(f"❌ {category_name}: Error during rasterization")
            print(f"   Error: {str(e)[:100]}\n")
            continue

    print(f"\n{'='*80}")
