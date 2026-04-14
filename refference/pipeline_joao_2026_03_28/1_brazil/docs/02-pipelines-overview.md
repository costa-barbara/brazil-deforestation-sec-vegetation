# MapBiomas Brazil — Pipelines Overview

Architecture and execution order for the 5 core pipelines.

---

## The 5 Core Pipelines

| # | Pipeline | Script | Purpose | Frequency | Depends On |
|---|----------|--------|---------|-----------|-----------|
| 1 | **Main Export** | `mapbiomas_brazil_export_pipeline.py` | Export coverage & deforestation GEE assets | Yearly | — |
| 2 | **Integration Filters (Sentinel)** | `export_integration_filters_pipeline_sentinel.py` | Apply 6 sequential filter stages to Sentinel data | As needed | Main Export (integration asset) |
| 3 | **Public Datasets** | `mapbiomas_brazil_export_datasets_public.py` | Export public GEE Data Catalog assets | Yearly | Main Export, Sentinel Filters |
| 4 | **Statistics** | `mapbiomas_brazil_export_statistics_pipeline.py` | Export area statistics to BigQuery | Yearly | Main Export, Territory raster |
| 5 | **Integration Filters (30m)** | `export_integration_filters_pipeline.py` | Apply integration filters to 30m Landsat data | As needed | Main Export (integration asset) |

---

## Typical Execution Workflow

```
1. Main Export
   └─→ generates: LAND-COVER/COLLECTION-10_1/integration
                  LAND-COVER/COLLECTION-10_1/deforestation

2. Sentinel Filters (optional)
   └─→ generates: filtered sentinel versions

3. Public Datasets
   └─→ generates: projects/mapbiomas-public/assets/brazil/...

4. Territory Rasterization (first run only)
   mapbiomas_brazil_convert_to_raster.py
   └─→ generates: territory-collection raster images

5. Statistics
   └─→ exports to BigQuery: coverage, deforestation, transitions, water
```

---

## Pipeline Descriptions

### 1. Main Export (`mapbiomas_brazil_export_pipeline.py`)

**Input:** Integration (coverage) image from GEE

**Output:**
- Coverage: `projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/mapbiomas_brazil_collection10_1_integration_v1`
- Deforestation: `projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10_1/DEFORESTATION/...`

**Runtime:** 30–60 minutes

**Execution:**
```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/mapbiomas_brazil_export_pipeline.py
```

**What it does:**
1. Loads integration image from GEE
2. Applies class aggregation
3. Applies deforestation temporal rules
4. Exports coverage and deforestation assets

---

### 2. Sentinel Filters (`export_integration_filters_pipeline_sentinel.py`)

**Input:** Integration asset from Main Export

**Output:** 6 filter assets (silvicultura, general, agriculture, pasture, alertas, transitions)

**Runtime:** ~5 hours (6 sequential stages, each ~50 min)

**Execution:**
```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/mapbiomas_brazil_export_integration_filters_pipeline_sentinel.py
```

**What it does:**
1. Applies silvicultura filter
2. Applies general filter
3. Applies agriculture filter
4. Applies pasture filter
5. Applies alertas filter
6. Applies transitions filter

Each stage waits for completion before starting the next.

---

### 3. Public Datasets (`mapbiomas_brazil_export_datasets_public.py`)

**Input:** Coverage, deforestation, transitions, water assets

**Output:** Public GEE Data Catalog assets
- `projects/mapbiomas-public/assets/brazil/lulc/v1/` (coverage by year)
- `projects/mapbiomas-public/assets/brazil/deforestation/v1/` (deforestation)
- Water, transitions, quality mosaics, etc.

**Runtime:** 30–90 minutes

**Execution:**
```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/mapbiomas_brazil_export_datasets_public.py
```

**What it does:**
1. Loads coverage, deforestation, water assets
2. Exports individual years to GEE Data Catalog
3. Exports quality mosaics
4. Exports water surface/bodies/monthly collections

---

### 4. Territory Rasterization (`mapbiomas_brazil_convert_to_raster.py`)

**Input:** Territory vectors (FeatureCollections) from `mapbiomas-territories`

**Output:** Territory raster images
- `projects/mapbiomas-territories/assets/TERRITORIES/LULC/BRAZIL/COLLECTION10/territory-collection/`

**Runtime:** 1–2 hours (9 tasks submitted, waits for completion)

**Execution:**
```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/utilities/mapbiomas_brazil_convert_to_raster.py
```

**When to run:** Once per new collection release, or when territory definitions change.

**What it does:**
1. Loads territory FeatureCollections (POLITICAL_LEVEL_1, BIOMES, etc.)
2. Rasterizes each category into an image (pixel value = FEATURE_ID)
3. Exports to GEE as `territory-collection`

---

### 5. Statistics Export (`mapbiomas_brazil_export_statistics_pipeline.py`)

**Input:** Coverage, deforestation, transitions, water assets + territory rasters

**Output:** BigQuery tables in `mapbiomas_brazil_statistics` dataset

**Runtime:** 2–4 hours

**Execution:**
```bash
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/mapbiomas_brazil_export_statistics_pipeline.py
```

**What it does:**
1. For each year and category:
   - Reduces coverage image by territory and sums by class
   - Reduces deforestation image by territory and sums by class
   - Calculates transitions (cross-tabulation)
   - Reduces water annual and monthly by territory
2. Exports results to BigQuery

**Configuration:** Enable/disable specific exports with `RUN_EXPORT_*` flags.

---

## Data Flow Diagram

```
GEE Raw Data (Integration)
    ↓
[1] Main Export Pipeline
    ├─→ Coverage asset (40 bands, 1985–2024)
    └─→ Deforestation asset (40 bands, 1 simplified class per year)
        ↓
    [2a] Sentinel Filters (6 stages)
    [2b] Integration Filters (30m)
        ↓
    [3] Public Datasets Export
        └─→ GEE Data Catalog
            ├─→ Coverage (1 image/year)
            ├─→ Deforestation (1 image/year)
            ├─→ Water surface/bodies
            └─→ Quality mosaics
        ↓
    [4] Territory Rasterization (one-time)
        └─→ Territory collection (raster by category)
            ↓
    [5] Statistics Export
        └─→ BigQuery tables
            ├─→ coverage
            ├─→ deforestation_annual
            ├─→ transitions
            ├─→ water_annual
            └─→ water_monthly
```

---

## Recommended Execution Schedule

### Annual Release Cycle

```
Month 1:
  • Run [1] Main Export
  • Run [3] Public Datasets (once assets are ready)
  • Run [5] Statistics (once territory raster exists)
  • Validate results in BigQuery and GEE

Month 2–3:
  • Run [2a] Sentinel Filters (if needed for Sentinel product)
  • Run [2b] Integration Filters (if needed for 30m product)

Ongoing:
  • Monitor GEE task queue
  • Check BigQuery for data quality
```

### One-Time Setup

```
[1] Territory Rasterization (mapbiomas_brazil_convert_to_raster.py)
    • Run once for new collection
    • Re-run only if territory definitions change
```

---

## Expected Execution Times

| Pipeline | Duration | Notes |
|----------|----------|-------|
| Main Export | 30–60 min | Depends on GEE task queue |
| Sentinel Filters | ~5 hours | 6 sequential stages |
| Public Datasets | 30–90 min | 40 years × 10+ products |
| Territory Raster | 1–2 hours | 9 tasks parallel |
| Statistics | 2–4 hours | 40 years × 9 categories |

**Total for annual release:** ~9–12 hours (sequential execution)

---

## Monitoring

### Monitor GEE tasks

Visit: https://code.earthengine.google.com (Tasks tab)

Or via CLI:
```bash
earthengine task list
```

### Monitor BigQuery exports

In BigQuery console:
```sql
SELECT dataset_id, table_id, TIMESTAMP_MILLIS(creation_time) as created
FROM `mapbiomas-brazil.mapbiomas_brazil_statistics.__TABLES__`
ORDER BY created DESC
LIMIT 10;
```

### Check script logs

Each pipeline logs to `logs/` directory:
- `logs/main_export.log`
- `logs/public_datasets.log`
- `logs/statistics.log`

---

## Dependencies Between Pipelines

```
Main Export (required for all others)
    ↓
    ├─→ Sentinel Filters (optional, downstream product)
    ├─→ Integration Filters (optional, downstream product)
    ├─→ Public Datasets (requires coverage + deforestation)
    └─→ Statistics (requires main + territory raster)
            ↑
            └─ Territory Rasterization (one-time setup)
```

**Critical path:** Main Export → Public Datasets / Statistics

**Optional paths:** Sentinel Filters, Integration Filters
