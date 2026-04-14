# MapBiomas Collection Structure — GEE Assets Architecture

**MapBiomas Brazil · Collection 10.1**

This document describes how MapBiomas Collection 10.1 is organized in Google Earth Engine, including the asset hierarchy, band naming conventions, data types, and access patterns.

---

## Collection Hierarchy

```
Collection (ImageCollection)
├── Tile 1
│   ├── Band 1985
│   ├── Band 1986
│   └── Band Year N
├── Tile 2
│   ├── Band 1985
│   ├── Band 1986
│   └── Band Year N
└── Tile N
    ├── Band 1985
    ├── Band 1986
    └── Band Year N
```

---

## Asset Paths

### Coverage (Integration)

```
projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/
  mapbiomas_brazil_collection10_1_integration_...  (ImageCollection, 40 bands)
```

**Bands:** `classification_1985` to `classification_2024`

**Data type:** INT (0-62)

**Pyramiding policy:** MODE

---

### Deforestation

```
projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/DEFORESTATION/
  mapbiomas_brazil_collection10_1_deforestation_...  (ImageCollection, 40 bands)
```

**Bands:** `classification_1985` to `classification_2024`

**Data type:** INT (0-7, simplified classes)

**Pyramiding policy:** MODE

---

### Water (Collection 5)

**Annual:**
```
projects/mapbiomas-brazil/assets/WATER/COLLECTION-5/
  mapbiomas_brazil_collection5_water_v1  (Image, 40 bands)
```

**Bands:** `classification_1985` to `classification_2024`

**Data type:** INT (0-255)

**Monthly:**
```
projects/mapbiomas-brazil/assets/WATER/COLLECTION-5/
  mapbiomas_brazil_collection5_water_monthly_v1  (ImageCollection, 480 images)
```

**Images:** `water_monthly_{year}_{month}` (e.g., `water_monthly_1985_01` to `water_monthly_2024_12`)

**Band per image:** `water_monthly` (INT 0-255)

**Properties per image:** `year` (int), `month` (string "01"-"12"), `version` ("1")

---

### Territories (Reference)

```
projects/mapbiomas-territories/assets/TERRITORIES/LULC/BRAZIL/COLLECTION10/
  territory-collection/  (ImageCollection)
    1_POLITICAL_LEVEL_1_1
    2_POLITICAL_LEVEL_2_1
    3_POLITICAL_LEVEL_3_1
    4_BIOMES_1
    5_COASTAL_MARINE_ZONE_1
    ...
```

**Pixel values:** FEATURE_ID (uint32)

**Pyramiding policy:** MODE

---

## Band Naming Convention

MapBiomas uses a consistent band naming scheme:

- **Format:** `{variable}_{year}` or `classification_{year}`
- **Years:** 1985 to 2024 (40 bands)
- **Examples:**
  - `classification_1985`, `classification_1986`, ..., `classification_2024` (coverage)
  - `water_monthly` (monthly water, single band per image)

---

## Accessing Assets via Python API

### Load an entire ImageCollection (40 bands)

```python
import ee

ee.Initialize(project='mapbiomas-brazil')

# Load coverage
coverage = ee.ImageCollection('projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/mapbiomas_brazil_collection10_1_integration_v1')

# Get first image (composite of all bands)
first_image = coverage.first()
print(first_image.bandNames().getInfo())  # ['classification_1985', 'classification_1986', ...]
```

### Access a specific band

```python
# Select only year 2023
band_2023 = first_image.select('classification_2023')
```

### Reduce over a region

```python
# Sum pixels in a region (by category)
def get_area_by_class(image, region):
    return image.select('classification_2023').reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=region,
        scale=30
    )
```

---

## Inspecting Assets via CLI

### Get asset metadata

```bash
earthengine asset info projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/mapbiomas_brazil_collection10_1_integration_v1
```

Output includes:
- Asset type (Image, ImageCollection)
- Band names
- Pyramiding policies
- Bands' data types
- Geospatial bounds

### List assets in a folder

```bash
earthengine ls projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1
```

---

## Data Types & Ranges

| Asset | Band | Data Type | Range | Meaning |
|-------|------|-----------|-------|---------|
| Coverage | classification_YYYY | INT | 0–62 | MapBiomas land cover class |
| Deforestation | classification_YYYY | INT | 0–7 | Simplified class (see 10-deforestation-rules.md) |
| Water Annual | classification_YYYY | INT | 0–255 | Water presence/quality |
| Water Monthly | water_monthly | INT | 0–255 | Water presence/quality |

---

## Pyramiding Policies

MapBiomas uses **MODE** pyramiding for all assets:

- **MODE:** Selects the most common value at each coarser resolution level
- **Why:** Preserves discrete land cover classes at zoom levels where individual pixels merge

Other policies (MEAN, MEDIAN, SUM) would blur class boundaries.

---

## Example: Querying an Asset

```bash
# Check if the coverage asset exists
earthengine asset info \
  projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/mapbiomas_brazil_collection10_1_integration_v1

# Output:
# Type: ImageCollection
# Bands (40):
#   Band 0: classification_1985 (INT) pyramiding_policy=MODE
#   Band 1: classification_1986 (INT) pyramiding_policy=MODE
#   ...
#   Band 39: classification_2024 (INT) pyramiding_policy=MODE
# Properties:
#   version: "1"
#   collection: "10.1"
```
