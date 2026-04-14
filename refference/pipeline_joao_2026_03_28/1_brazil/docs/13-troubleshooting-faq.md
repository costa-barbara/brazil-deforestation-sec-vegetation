# Troubleshooting & FAQ

Debugging common issues in MapBiomas Brazil pipeline.

---

## GEE Tasks

### "Task timed out"

**Cause:** GEE computation exceeded time limit (usually 5 minutes per pixel operation).

**Solutions:**
1. Break the export into smaller regions using `REGION_BBOX` subsetting
2. Reduce `scale` parameter (30m → 60m or 90m) to aggregate pixels
3. Simplify the computation (remove unnecessary bands/operations)
4. Wait for GEE queue to clear and retry

**Check task logs:**
```bash
earthengine task list
# Find the failed task ID and check the log
```

---

### "Asset does not exist"

**Cause:** Referenced asset path is incorrect or asset was deleted.

**Solutions:**
1. Verify asset path:
```bash
earthengine asset info projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/mapbiomas_brazil_collection10_1_integration_v1
```

2. List assets in folder:
```bash
earthengine ls projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1
```

3. Check CLAUDE.md for correct asset naming conventions

---

### "Permission denied: Cannot read asset"

**Cause:** Your GEE project does not have access to the asset.

**Solutions:**
1. Verify authentication:
```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python -m ee.ee_command authenticate
```

2. Re-authenticate with correct account:
```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python -m ee.ee_command authenticate --clear
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python -m ee.ee_command authenticate
```

3. Verify project initialization:
```python
import ee
ee.Initialize(project='mapbiomas-brazil')
```

---

## BigQuery

### "Dataset not found"

**Cause:** Statistics dataset was not initialized.

**Solution:**
```bash
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/utilities/setup_statistics_tables.py
```

---

### "Table already exists"

**Cause:** Table already exists, cannot overwrite via schema creation.

**Solution:** The statistics script appends data. This is expected behavior. To reset:
1. Delete tables manually in BigQuery:
```sql
DROP TABLE `mapbiomas-brazil.mapbiomas_brazil_statistics.coverage`;
```

2. Re-run setup:
```bash
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/utilities/setup_statistics_tables.py
```

---

### "Quota exceeded"

**Cause:** BigQuery API limit hit (typically after exporting 1000+ rows in quick succession).

**Solution:**
1. Wait 5–10 minutes before rerunning
2. Enable exponential backoff in script:
```python
# Add to export function
import time
time.sleep(0.1)  # 100ms delay between exports
```

---

## Territory Rasterization

### "FeatureCollection is empty"

**Cause:** Territory vectors failed to load from `mapbiomas-territories`.

**Solutions:**
1. Verify territory asset exists:
```bash
earthengine asset info projects/mapbiomas-territories/assets/TERRITORIES/BRAZIL/WORKSPACE/POLITICAL_LEVEL_1
```

2. Check `resolve_territory_asset()` fallback:
```python
from utils.export_utils import resolve_territory_asset
asset = resolve_territory_asset(
    'projects/mapbiomas-territories/assets/TERRITORIES/BRAZIL/WORKSPACE/POLITICAL_LEVEL_1/POLITICAL_LEVEL_1_v1'
)
print(asset)
```

3. Verify territory project has access

---

### "Territory raster is empty (0 pixels)"

**Cause:** Territory vectors have empty geometries or fall outside the region.

**Solutions:**
1. Inspect territory FeatureCollection:
```python
import ee
ee.Initialize(project='mapbiomas-territories')
fc = ee.FeatureCollection('projects/mapbiomas-territories/assets/TERRITORIES/BRAZIL/WORKSPACE/POLITICAL_LEVEL_1/POLITICAL_LEVEL_1_v1')
print(fc.size().getInfo())  # Should be > 0
print(fc.first().getInfo())  # Inspect first feature
```

2. Check region bounds match territory bounds

---

## Water Statistics

### "Water annual/monthly tables empty"

**Cause:** Water asset does not load, territory raster missing, or no water coverage in region.

**Debug steps:**
```bash
# 1. Check water asset exists
earthengine asset info 'projects/mapbiomas-brazil/assets/WATER/COLLECTION-5/mapbiomas_brazil_collection5_water_v1'

# 2. Run debug script
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/utilities/debug_water_data.py

# 3. Inspect territory-collection
earthengine asset info projects/mapbiomas-territories/assets/TERRITORIES/LULC/BRAZIL/COLLECTION10/territory-collection
```

**Solutions:**
1. Verify water asset has bands for all years:
```bash
earthengine asset info 'projects/mapbiomas-brazil/assets/WATER/COLLECTION-5/mapbiomas_brazil_collection5_water_v1' | grep classification
```

2. Verify territory-collection was created:
```bash
earthengine ls projects/mapbiomas-territories/assets/TERRITORIES/LULC/BRAZIL/COLLECTION10/territory-collection/
```

3. Re-run rasterization if territory-collection is missing:
```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/utilities/mapbiomas_brazil_convert_to_raster.py
```

---

## Statistics Pipeline

### "No tasks submitted" / script exits immediately

**Cause:** All `RUN_EXPORT_*` flags are `False`.

**Solution:** Enable flags in script header:
```python
RUN_EXPORT_COVERAGE      = True
RUN_EXPORT_DEFORESTATION = True
RUN_EXPORT_TRANSITIONS   = True
RUN_EXPORT_WATER_ANNUAL  = True
RUN_EXPORT_WATER_MONTHLY = True
```

---

### "Coverage/deforestation results seem wrong (too large/small)"

**Cause:** Possible error in `reduceRegion()` logic or class mapping.

**Solutions:**
1. Verify class codes:
```bash
# Check deforestation classes (should be 0–7)
earthengine asset info projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/DEFORESTATION/...
```

2. Spot-check results:
```sql
SELECT class, area_km2, year
FROM `mapbiomas-brazil.mapbiomas_brazil_statistics.coverage`
WHERE feature_id = 1
  AND year = 2023
ORDER BY area_km2 DESC
LIMIT 10;
```

3. Manual validation in GEE (compute total area for Brazil):
```python
import ee
from utils.region_utils import RegionUtils

ee.Initialize(project='mapbiomas-brazil')
region_utils = RegionUtils()
bbox = region_utils.get_bbox(iso3='BRA')

coverage = ee.ImageCollection('projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/mapbiomas_brazil_collection10_1_integration_v1')
area = coverage.first().select('classification_2023').eq(0).multiply(900).divide(1e6)  # 30m pixel = 900 m²
total = area.reduceRegion(ee.Reducer.sum(), geometry=bbox, scale=30).getInfo()
print(f"Total area (2023): {total.get('classification_2023')} km²")
```

---

### "Transitions results are all zeros"

**Cause:** Transitions computation may have incorrect band selection or reducing logic.

**Solution:**
1. Verify transitions asset exists and has data:
```bash
earthengine asset info projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/transitions
```

2. Check transitions are computed as cross-tabulation (class_from × class_to):
```sql
SELECT DISTINCT class_from, class_to
FROM `mapbiomas-brazil.mapbiomas_brazil_statistics.transitions`
LIMIT 20;
```

3. If empty, verify integration asset exists:
```bash
earthengine asset info projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/mapbiomas_brazil_collection10_1_integration_v1
```

---

## Python Errors

### "ModuleNotFoundError: No module named 'google.cloud'"

**Cause:** Using `earthengine-api` venv instead of `.venv` with BigQuery.

**Solution:**
```bash
# For statistics scripts, use:
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/mapbiomas_brazil_export_statistics_pipeline.py

# For other scripts, use:
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/mapbiomas_brazil_export_pipeline.py
```

---

### "ValueError: Rule is missing min_start"

**Cause:** A deforestation rule is malformed (missing 3rd element).

**Solution:** Check `deforestation_secondary_vegetation.py` — all rules must have exactly 3 elements:
```python
[kernel_bef, kernel_aft, min_start]  # min_start is mandatory
```

---

### "ImportError: cannot import name 'RegionUtils'"

**Cause:** `utils/region_utils.py` not found.

**Solution:**
```bash
# Verify file exists
ls 1_brazil/../utils/region_utils.py

# Ensure PYTHONPATH includes project root
export PYTHONPATH=/Users/joaosiqueira/Documents/Projects/mapbiomas-pipeline:$PYTHONPATH
```

---

## Performance Issues

### Script is very slow

**Causes:**
1. GEE queue is congested → wait or retry later
2. Region is too large → subset `REGION_BBOX`
3. Too many categories enabled → disable unused `RUN_EXPORT_*` flags

**Optimization:**
- Reduce `EXPORT_MAX_PIXELS` to export smaller regions (increases task count but reduces per-task time)
- Use `scale=60` instead of `30` to aggregate pixels
- Process one year at a time instead of all years

---

### BigQuery quota exceeded

**Cause:** Too many inserts in short timeframe.

**Solution:**
```python
# Add delay in export function
import time
time.sleep(1)  # 1-second delay between table inserts
```

---

## Logs and Debugging

### View export logs

```bash
# Check if logs directory exists
ls 1_brazil/logs/

# Tail main export log
tail -f 1_brazil/logs/main_export.log

# Check for errors
grep -i error 1_brazil/logs/*.log
```

### Enable debug output

Most scripts have a DEBUG flag at the top:
```python
DEBUG = True  # Enable verbose output
```

---

## Contact & Support

For issues not covered here:
1. Check CLAUDE.md for architecture/conventions
2. Review script comments and docstrings
3. Inspect GEE Code Editor logs: https://code.earthengine.google.com (Tasks tab)
4. Check BigQuery error logs for failed table exports
