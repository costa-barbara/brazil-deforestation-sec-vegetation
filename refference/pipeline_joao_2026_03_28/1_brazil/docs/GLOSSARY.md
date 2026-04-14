# Glossary

Terminology, class codes, and abbreviations used in MapBiomas Brazil pipeline.

---

## Land Cover Classes (0–62)

MapBiomas Collection 10.1 uses 62 classes to represent different land cover types. For deforestation analysis, these are aggregated to 7 simplified classes.

### Full Classes (0–62)

See the official MapBiomas classification legend:
- **Class 0:** No Data
- **Class 1–3:** Water (surface water, river, lake, ocean)
- **Class 4–11:** Natural vegetation (forest types, savanna, grassland, etc.)
- **Class 12–28:** Anthropic uses (pasture, agriculture, urban, mining, etc.)

(Full mapping available in MapBiomas documentation)

### Simplified Deforestation Classes (0–7)

Used internally by `DeforestationSecondaryVegetation` after aggregation:

| Code | Name | Description |
|------|------|-------------|
| **0** | No Data | Unobserved or masked pixels |
| **1** | Anthropic | Any human land use (pasture, agriculture, urban, mining) |
| **2** | Primary Vegetation | Native vegetation with no prior anthropic history |
| **3** | Secondary Vegetation | Vegetation regrowth on previously anthropic land |
| **4** | Primary Veg. Suppression | **Confirmed deforestation** on primary vegetation |
| **5** | Recovery to Secondary | **First year** of vegetation regrowth after anthropic use |
| **6** | Secondary Veg. Suppression | **Confirmed deforestation** on secondary vegetation |
| **7** | Other Transitions | Water, rocky outcrops, beaches, filtered noise |

See: **[10-deforestation-rules.md](10-deforestation-rules.md)** for detailed explanation.

---

## Water Classes (0–255)

Water presence and quality classes in the water asset:

| Code | Interpretation |
|------|-----------------|
| **0** | No water |
| **1** | Water (low confidence) |
| **2–254** | Water (varying confidence/quality levels) |
| **255** | Invalid / no data |

(Exact mapping varies; see water asset documentation)

---

## Geographic Terms

| Term | Meaning |
|------|---------|
| **POLITICAL_LEVEL_1** | States (27 territories in Brazil) |
| **POLITICAL_LEVEL_2** | Municipalities (5,570 territories in Brazil) |
| **POLITICAL_LEVEL_3** | Districts (sub-municipal level) |
| **BIOMES** | Major ecological zones (Amazon, Cerrado, Atlantic Forest, etc.) |
| **COASTAL_MARINE_ZONE** | Coastal and marine territorial zones |
| **BASIN_LEVEL_1** | Major river basins (PNRH: National Water Resources Framework) |
| **BASIN_LEVEL_2** | Sub-basins |

These correspond to territory categories in `mapbiomas-territories` GEE project.

---

## Project & Repository Terms

| Term | Full Name | Context |
|------|-----------|---------|
| **GEE** | Google Earth Engine | Cloud platform for geospatial analysis |
| **BigQuery** | Google BigQuery | Cloud data warehouse for querying statistics |
| **Collection** | MapBiomas Collection | Versioned release of land cover dataset (e.g., Collection 10.1) |
| **Collection ID** | — | Numeric identifier for collection (e.g., `10.1` for Collection 10.1) |
| **Asset** | GEE Asset | Raster or vector object stored in GEE (Image, ImageCollection, FeatureCollection) |
| **Image** | EE Image | Single raster layer (all bands = all years) |
| **ImageCollection** | EE ImageCollection | Stack of Images (can have multiple tiles) |
| **Band** | — | Single time slice within an Image (e.g., `classification_2023`) |
| **Tile** | — | Spatial partition of an ImageCollection |
| **Task** | GEE Task | Submitted computation job (export, reduce, etc.) |

---

## Pipeline Terms

| Term | Meaning |
|------|---------|
| **Integration** | Harmonized coverage map (input to deforestation pipeline) |
| **Deforestation** | Output of temporal rules applied to integration |
| **Transitions** | Class-to-class changes year-over-year (cross-tabulation) |
| **Territory raster** | Rasterized vector layer where pixel values = feature IDs |
| **Sentinel filters** | Applied corrections for Sentinel-2 10m data |
| **Integration filters** | Applied corrections for 30m Landsat data |
| **Public datasets** | Assets exported to `projects/mapbiomas-public/` for public use |
| **Statistics** | Area (km²) aggregated by category, class, and period |

---

## Temporal Terms

| Term | Meaning |
|------|---------|
| **Kernel** | Sliding window of consecutive years (e.g., 3-year or 4-year kernel) |
| **kernel_bef** | Expected input pattern (before state) in a deforestation rule |
| **kernel_aft** | Output pattern (after state) in a deforestation rule |
| **min_start** | Minimum year index at which a deforestation rule can apply |
| **t1, t2, t3, t4** | Time indices within a kernel (t1=oldest, t4=newest) |
| **YEARS** | Full time series (1985–2024, 40 years) |
| **YEARS_END** | Last subset of years where special rules apply (end-of-series transitions) |

Example: For a 4-year kernel `[1985, 1986, 1987, 1988]`:
- `t1 = 1985`, `t2 = 1986`, `t3 = 1987`, `t4 = 1988`

See: **[10-deforestation-rules.md](10-deforestation-rules.md)** for examples.

---

## Version Conventions

| Pattern | Meaning | Example |
|---------|---------|---------|
| `COLLECTION_ID` | Collection version (int or float) | `10.1` |
| `VERSION_INPUT_*` | Input asset version | `VERSION_INPUT_INTEGRATION = 'v1'` |
| `VERSION_PUBLIC_*` | Output asset version | `VERSION_PUBLIC_INTEGRATION = 'v2'` |
| `v1`, `v2`, ... | Asset version suffix | Path contains `..._integration_v2` |

**Key rule:** When bumping `VERSION_PUBLIC_*`, always update the corresponding `VERSION_INPUT_*` in dependent scripts.

---

## Function Terms

| Term | Meaning | Module |
|------|---------|--------|
| **aggregate_classes()** | Map full 62 classes → simplified 7 classes | `deforestation_secondary_vegetation.py` |
| **apply_rule_kernel_4()** | Apply 4-year temporal rule | `deforestation_secondary_vegetation.py` |
| **apply_rule_kernel_3()** | Apply 3-year temporal rule | `deforestation_secondary_vegetation.py` |
| **resolve_territory_asset()** | Dynamically resolve territory asset to latest version | `utils/export_utils.py` |
| **export_image_to_asset()** | Export masked image to GEE asset | `utils/export_utils.py` |
| **wait_until_tasks_finish()** | Monitor and wait for GEE tasks to complete | `utils/export_utils.py` |
| **reduceRegion()** | Aggregate pixels within a region | GEE Python API |

---

## Configuration Terms

| Flag/Variable | Meaning | Example |
|---------------|---------|---------|
| `RUN_EXPORT_*` | Enable/disable specific export | `RUN_EXPORT_COVERAGE = True` |
| `REGION_BBOX` | Bounding box of region (from `RegionUtils`) | Used in filtering and export |
| `COLLECTION_ID` | Collection version identifier | `10.1` |
| `REGION_NAME` | Region identifier (lowercase) | `'brazil'` |
| `EXPORT_MAX_PIXELS` | Max pixels per GEE export task | `1e8` (100 million) |
| `EXPORT_PYRAMIDING_POLICY` | Aggregation method for zoom levels | `'MODE'` (most common value) |

---

## File Organization

| Folder | Contents |
|--------|----------|
| `1_brazil/` | Brazil pipeline root |
| `1_brazil/docs/` | Documentation (this glossary, guides) |
| `1_brazil/utilities/` | Setup & maintenance scripts |
| `1_brazil/logs/` | Pipeline execution logs |
| `utils/` | Shared functions (all regions) |
| `deforestation/` | Deforestation rule classes |

---

## Abbreviations

| Abbr. | Expansion |
|------|-----------|
| **GEE** | Google Earth Engine |
| **BQ** | BigQuery |
| **km²** | Square kilometers |
| **m²** | Square meters |
| **px** | Pixel |
| **30m** | 30-meter resolution (Landsat) |
| **10m** | 10-meter resolution (Sentinel-2) |
| **YYYY** | Year (e.g., 2023) |
| **HH** | Hour (e.g., 01–12) |
| **MM** | Month (e.g., 01–12) |
| **ISO3** | 3-letter country code (e.g., BRA, BOL, PER) |
| **CATEG_ID** | Category ID (numeric identifier for territory type) |
| **FEATURE_ID** | Feature ID (within a territory category) |

---

## Boolean States

| Term | Meaning |
|------|---------|
| **True / False** | Pipeline flags (enable/disable features) |
| **SUCCESS** | GEE task completed without error |
| **FAILED** | GEE task encountered error |
| **RUNNING** | GEE task currently executing |
| **READY** | GEE task queued, waiting for execution |

---

## Data Type Conventions

| Type | Meaning | Example |
|------|---------|---------|
| **INT** | Integer (whole number) | Class codes 0–62 |
| **FLOAT64** | 64-bit floating point | Area in km² (can have decimals) |
| **STRING** | Text | Territory names, version tags |
| **UINT32** | Unsigned 32-bit integer | Feature IDs (cannot be negative) |

---

## Related Documentation

- **[10-deforestation-rules.md](10-deforestation-rules.md)** — Detailed deforestation class definitions & rules
- **[15-collection-structure.md](15-collection-structure.md)** — GEE asset structure & band naming
- **[CLAUDE.md](../../CLAUDE.md)** — Project conventions & version management
