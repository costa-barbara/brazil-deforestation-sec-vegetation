# -*- coding: utf-8 -*-
"""
Shared utilities for MapBiomas public dataset export scripts.

Functions here are region-agnostic. Each regional script imports them and
provides a thin _export() wrapper that supplies its own region constants.
"""

import ee
import re
import time
import subprocess
import json
from datetime import timedelta


def wait_until_tasks_finish(export_tasks=[], polling_interval=60):
    """
    Waits until all export tasks have finished.

    Continues monitoring even if some tasks fail. Displays status and duration
    at the end. Handles transient network errors by retrying on the next interval.
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
    try:
        max_len = max(len(task.status().get('description', '')) for task in export_tasks)
    except Exception:
        max_len = 0
    label_pad = max(40, max_len + 4)

    while True:
        all_done = True

        for task in export_tasks:
            try:
                status = task.status()
            except Exception as e:
                print(f"⚠️  Network error while polling status: {e}. Retrying in {polling_interval}s...")
                all_done = False
                continue

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
                print(f"❌ {description.ljust(label_pad)} | {time.strftime('%Y-%m-%dT%H:%M:%S')} | Status: {state} | Error: {error_message}")
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


def resolve_territory_asset(asset_path):
    """
    Resolves the best available version of a territory asset.

    Given a path ending in '<name>_vN':
    1. Lists siblings in the same GEE folder and returns the asset with the
       highest version number (>= or < N — always takes the best available).
    2. Falls back to the equivalent TERRITORIES-STAGING folder if nothing is
       found under TERRITORIES.
    3. Returns the original path as a last resort (GEE will raise at runtime).

    Must be called after ee.Initialize().
    """
    parts = asset_path.split('/')
    parent = '/'.join(parts[:-1])
    asset_name = parts[-1]

    base_match = re.match(r'^(.+?)_v(\d+)$', asset_name)
    if not base_match:
        return asset_path

    base_name = base_match.group(1)

    def find_best_in_folder(folder):
        try:
            result = ee.data.listAssets({'parent': folder})
            items = result.get('assets', [])
            candidates = []
            for item in items:
                name = item['name'].split('/')[-1]
                m = re.match(rf'^{re.escape(base_name)}_v(\d+)$', name)
                if m:
                    candidates.append((int(m.group(1)), item['name']))
            if candidates:
                candidates.sort(reverse=True)
                best = candidates[0][1]
                print(f"✅ Resolved territory: {best}")
                return best
        except Exception as e:
            print(f"⚠️  Could not list assets in {folder}: {e}")
        return None

    result = find_best_in_folder(parent)
    if result:
        return result

    staging_parent = parent.replace('/TERRITORIES/', '/TERRITORIES-STAGING/')
    result = find_best_in_folder(staging_parent)
    if result:
        print(f"⚠️  Using TERRITORIES-STAGING fallback")
        return result

    print(f"⚠️  No territory asset found, keeping original: {asset_path}")
    return asset_path


def asset_exists(asset_id):
    """
    Returns True if the Earth Engine asset exists, False otherwise.
    """
    try:
        ee.data.getAsset(asset_id)
        return True
    except ee.EEException:
        return False


def ensure_asset_exists(path, asset_type='ImageCollection'):
    """
    Ensures a GEE asset exists; creates it if not.

    Parameters:
    - path (str): Full asset path, e.g. 'projects/my-project/assets/my-collection'.
    - asset_type (str): 'Folder' or 'ImageCollection'. Defaults to 'ImageCollection'.

    Raises:
    - ee.EEException if asset creation fails.
    """
    try:
        ee.data.getAsset(path)
        print(f"✅ Asset already exists: {path}")
    except ee.EEException:
        print(f"📁 Asset does not exist: {path}")
        print(f"🛠️ Creating asset of type '{asset_type}'...")
        try:
            ee.data.createAsset({'type': asset_type}, path)
            print(f"✅ Created {asset_type}: {path}")
        except ee.EEException as create_err:
            print(f"❌ Failed to create asset: {path}")
            raise create_err


def set_asset_properties(asset_id: str, properties: dict):
    """
    Sets or updates properties on a GEE asset using the earthengine CLI.

    Parameters:
    - asset_id (str): Full asset ID (e.g. 'projects/xxx/assets/yyy').
    - properties (dict): Key-value pairs to set on the asset.
    """
    for key, value in properties.items():
        value_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        cmd = ['earthengine', 'asset', 'set', asset_id, '-p', f'{key}={value_str}']
        print(f"[GEE CLI] Setting property: {key} = {value_str}")
        subprocess.run(cmd, check=True)


def get_pyramiding_policy_mode(image, pyramiding_policy='MODE'):
    """
    Returns a pyramiding policy dict with the given policy for all bands.

    Parameters:
    - image (ee.Image): Image whose band names will be used.
    - pyramiding_policy (str): Policy to apply. Defaults to 'MODE'.

    Returns:
    - dict: {band_name: pyramiding_policy} for all bands.
    """
    band_names = image.bandNames().getInfo()
    return {band: pyramiding_policy for band in band_names}


def export_image_to_asset(
    image,
    asset_path,
    region_mask,
    region_bbox,
    collection_id,
    region_name,
    max_pixels,
    version=None,
    scale=30,
    additional_properties=None,
    pyramiding_policy='MODE',
    overwrite=False,
):
    """
    Masks and exports an image to a GEE asset. Skips if asset already exists,
    unless overwrite=True, in which case the existing asset is deleted first.

    Parameters:
    - image (ee.Image): Image to export.
    - asset_path (str): Full destination asset path.
    - region_mask (ee.Image): Binary mask to apply to the image.
    - region_bbox (ee.Geometry): Export region geometry.
    - collection_id: Collection identifier set as image property.
    - region_name (str): Region name set as 'territory' property (uppercased).
    - max_pixels (int|float): Maximum pixel count for the export.
    - version (str): Version tag set as image property.
    - scale (int): Spatial resolution in metres. Defaults to 30.
    - additional_properties (dict): Extra key-value pairs to set on the image.
    - pyramiding_policy (str): Pyramiding policy for all bands. Defaults to 'MODE'.
    - overwrite (bool): If True, delete existing asset before exporting. Defaults to False.

    Returns:
    - ee.batch.Task | None: The started task, or None if asset already exists.
    """
    if asset_exists(asset_path):
        if overwrite:
            ee.data.deleteAsset(asset_path)
            print(f"🗑️  Deleted existing asset for overwrite: {asset_path}")
        else:
            print(f"⚠️  Skipping {asset_path} (already exists).")
            return None

    image = (
        image.updateMask(region_mask)
        .set("collection_id", collection_id)
        .set("territory", region_name.upper())
        .set("version", version)
        .set("source", "MAPBIOMAS")
    )

    if additional_properties:
        for key, value in additional_properties.items():
            image = image.set(key, value)

    task = ee.batch.Export.image.toAsset(
        image=image,
        description=f'{region_name}:{asset_path.split("/")[-1]}',
        assetId=asset_path,
        pyramidingPolicy=get_pyramiding_policy_mode(image, pyramiding_policy),
        region=region_bbox,
        scale=scale,
        maxPixels=max_pixels,
    )
    task.start()
    print(f"[Started] Exporting: {asset_path}")
    return task
