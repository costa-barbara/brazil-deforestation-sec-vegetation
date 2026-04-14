# -*- coding: utf-8 -*-
"""
Shared export functions for MapBiomas public dataset export scripts.

These functions centralize logic that is repeated across regional scripts.
Each function accepts an `export_fn` callable — the region-specific wrapper
around `export_utils.export_image_to_asset` — plus region-specific constants.
"""

import ee

from utils.export_utils import ensure_asset_exists, set_asset_properties, asset_exists


def export_coverage_gee_data_catalog(
    export_fn, years, catalog_path, source_asset,
    collection_id, version, region_name,
    overwrite=False,
):
    """
    Exports coverage data to the GEE Data Catalog as one image per year.

    Parameters:
    - export_fn: Region-specific export wrapper callable.
    - years: Iterable of years to export.
    - catalog_path (str): Destination ImageCollection path.
    - source_asset (str): Multiband source image asset path.
    - collection_id: Collection identifier (int or float).
    - version (str): Version tag for the public assets.
    - region_name (str): Region name (lowercased in properties).
    - overwrite (bool): If True, delete existing per-year assets before re-exporting.
      Use only when updating a previous catalog version for a region. Defaults to False.

    Returns:
    - list: Started export tasks.
    """
    ensure_asset_exists(catalog_path)
    coverage = ee.Image(source_asset)
    tasks = []
    for year in years:
        coverage_year = coverage.select([f'classification_{year}'], ['classification'])
        collection_id_str = str(collection_id).replace('.', '_')
        asset_name = f'collection_{collection_id_str}_{year}_{version}'
        asset_path = f'{catalog_path}/{asset_name}'
        if overwrite and asset_exists(asset_path):
            ee.data.deleteAsset(asset_path)
            print(f"🗑️  Deleted for overwrite: {asset_path}")
        properties = {
            "theme": "coverage",
            "subtheme": "coverage",
            "version": version,
            "year": int(year),
            "collection_id": float(collection_id),
            "source": "mapbiomas",
            "region": region_name.lower(),
        }
        tasks.append(export_fn(
            coverage_year,
            asset_path,
            additional_properties=properties,
        ))
    return tasks


def export_collection_product(
    export_fn, asset, version_input, public_path, version_public,
    select_bands=None, additional_properties=None,
):
    """
    Loads an ImageCollection, filters by version, takes the pixel-wise minimum,
    and exports the result.

    Parameters:
    - export_fn: Region-specific export wrapper callable.
    - asset (str): Source ImageCollection asset path.
    - version_input: Version value to filter the collection.
    - public_path (str): Destination asset path.
    - version_public (str): Version tag for the exported image.
    - select_bands (list[str], optional): Band names to select before export.
    - additional_properties (dict, optional): Extra properties to set on the image.

    Returns:
    - ee.batch.Task | None: The started export task.
    """
    image = (
        ee.ImageCollection(asset)
        .filter(ee.Filter.eq('version', version_input))
        .min()
    )
    if select_bands:
        image = image.select(select_bands)
    return export_fn(
        image, public_path,
        version=version_public,
        additional_properties=additional_properties,
    )


def export_quality_mosaic(export_fn, asset, years, public_path, version_public):
    """
    Builds and exports a quality mosaic image for all specified years.

    Parameters:
    - export_fn: Region-specific export wrapper callable.
    - asset (str): Source ImageCollection asset path.
    - years: Iterable of years to process.
    - public_path (str): Destination asset path.
    - version_public (str): Version tag for the exported image.

    Returns:
    - ee.batch.Task | None: The started export task.
    """
    collection = ee.ImageCollection(asset)
    quality_bands = [
        collection.filter(ee.Filter.eq('year', year)).mosaic().rename(f'quality_{year}')
        for year in years
    ]
    return export_fn(ee.Image(quality_bands).byte(), public_path, version=version_public)


def export_water_surface(
    export_fn, asset, years, public_path, version_public,
    version_input=None, use_collection=True,
):
    """
    Exports annual water surface classification as a multiband image.

    Parameters:
    - export_fn: Region-specific export wrapper callable.
    - asset (str): Source asset path (ImageCollection or Image).
    - years: Iterable of years to process.
    - public_path (str): Destination asset path.
    - version_public (str): Version tag for the exported image.
    - version_input: Version value to filter the collection (required if use_collection=True).
    - use_collection (bool): If True, load from ImageCollection filtering by version, cadence
      and year; if False, load from a multiband Image using `water_{year}` band names.

    Returns:
    - ee.batch.Task | None: The started export task.
    """
    images = []
    for year in years:
        if use_collection:
            img = (
                ee.ImageCollection(asset)
                .filter(ee.Filter.eq('version', version_input))
                .filter(ee.Filter.eq('cadence', 'annual'))
                .filter(ee.Filter.eq('year', year))
                .mosaic()
                .rename(f'classification_{year}')
            )
        else:
            img = ee.Image(asset).select(f'water_{year}').rename(f'classification_{year}')
        images.append(img)

    properties = {
        "module": "Water",
        "submodule": "Water",
        "variable": "Water Surface",
        "format": "classification_multiband_image",
        "band_format": "classification_{year}",
        "data_type": "annual",
        "version": version_public,
    }
    return export_fn(ee.Image(images), public_path, additional_properties=properties)


def export_water_bodies(
    export_fn, asset, years, public_path, version_public,
    version_input=None, use_collection=True,
    class_remap=None,
):
    """
    Exports annual water bodies classification as a multiband image.

    Parameters:
    - export_fn: Region-specific export wrapper callable.
    - asset (str): Source asset path (ImageCollection or Image).
    - years: Iterable of years to process.
    - public_path (str): Destination asset path.
    - version_public (str): Version tag for the exported image.
    - version_input: Version value to filter the collection (required if use_collection=True).
    - use_collection (bool): If True, load from ImageCollection filtering by version and year;
      if False, load from a multiband Image using `water_bodies_{year}` band names.
    - class_remap (dict, optional): {'from': [...], 'to': [...]} remap applied per-year band.

    Returns:
    - ee.batch.Task | None: The started export task.
    """
    images = []
    for year in years:
        if use_collection:
            img = (
                ee.ImageCollection(asset)
                .filter(ee.Filter.eq('version', version_input))
                .filter(ee.Filter.eq('year', year))
                .mosaic()
            )
            if class_remap:
                img = img.remap(class_remap['from'], class_remap['to'])
            img = img.rename(f'classification_{year}')
        else:
            img = ee.Image(asset).select(f'water_bodies_{year}').rename(f'classification_{year}')
        images.append(img)

    properties = {
        "module": "Water",
        "submodule": "Water",
        "variable": "Water Bodies",
        "format": "classification_multiband_image",
        "band_format": "classification_{year}",
        "data_type": "annual",
        "version": version_public,
    }
    return export_fn(ee.Image(images), public_path, additional_properties=properties)


def export_water_monthly(
    export_fn, asset, years, public_path, version_public,
    version_input=None, months=range(1, 13),
    use_version_filter=True, month_as_string=True,
):
    """
    Exports monthly water classification as individual images in an ImageCollection.

    Parameters:
    - export_fn: Region-specific export wrapper callable.
    - asset (str): Source ImageCollection asset path.
    - years: Iterable of years to process.
    - public_path (str): Destination ImageCollection path.
    - version_public (str): Version tag for the exported images.
    - version_input: Version value to filter the collection (used if use_version_filter=True).
    - months: Iterable of month numbers (default: 1-12).
    - use_version_filter (bool): Whether to filter the collection by version.
    - month_as_string (bool): If True, filter month as zero-padded string ('01'-'12');
      if False, filter as integer (1-12).

    Returns:
    - list: Started export tasks.
    """
    collection_properties = {
        "module": "Water",
        "submodule": "Water",
        "variable": "Water Monthly",
        "band_format": "water_monthly",
        "format": "temporal_categorical_singleband_collection",
        "data_type": "monthly",
        "version": version_public,
    }
    ensure_asset_exists(public_path)
    set_asset_properties(public_path, collection_properties)

    collection = ee.ImageCollection(asset)
    tasks = []
    for year in years:
        for month in months:
            month_str = str(month).zfill(2)
            filt = collection.filter(ee.Filter.eq('year', year))
            if use_version_filter:
                filt = filt.filter(ee.Filter.eq('version', version_input))
            filt = filt.filter(ee.Filter.eq('month', month_str if month_as_string else month))
            image = filt.first().gt(0).multiply(month).rename(['water_monthly'])
            asset_name = f'water_monthly_{year}_{month_str}_{version_public}'
            properties = {
                "module": "Water",
                "submodule": "Water",
                "variable": "Water Monthly",
                "format": "temporal_categorical_singleband_collection",
                "band_format": "water_monthly",
                "data_type": "monthly",
                "month": month_str,
                "year": year,
                "version": version_public,
            }
            tasks.append(export_fn(
                image,
                f'{public_path}/{asset_name}',
                additional_properties=properties,
            ))
    return tasks
