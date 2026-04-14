# MapBiomas Brazil Pipeline Documentation

Complete guide for the MapBiomas Brazil land cover and deforestation export pipeline.

---

## Quick Start (3 steps)

### 1. Setup environment
```bash
# First time only: authenticate with GEE
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python -m ee.ee_command authenticate

# Verify setup
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python -c "import ee; ee.Initialize(); print('✓ GEE ready')"
```

See: **[01-setup-environment.md](01-setup-environment.md)**

### 2. Run main export
```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/mapbiomas_brazil_export_pipeline.py
```

Generates coverage and deforestation assets. **Runtime:** ~1 hour

See: **[02-pipelines-overview.md](02-pipelines-overview.md#1-main-export)**

### 3. Export statistics to BigQuery (optional)
```bash
# First time only: initialize BigQuery dataset
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/utilities/setup_statistics_tables.py

# Rasterize territories (first time only)
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/utilities/mapbiomas_brazil_convert_to_raster.py

# Export statistics
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/mapbiomas_brazil_export_statistics_pipeline.py
```

See: **[07-statistics-pipeline.md](07-statistics-pipeline.md)**

---

## Documentation Index

### Core Guides

| Doc | Purpose | Read if... |
|-----|---------|-----------|
| **[01-setup-environment.md](01-setup-environment.md)** | Python, GEE auth, dependencies | You're setting up the pipeline for the first time |
| **[02-pipelines-overview.md](02-pipelines-overview.md)** | Architecture & execution order | You want to understand the 5 pipelines and their relationships |

### Pipeline Details

| Doc | Purpose | Scripts Covered |
|-----|---------|-----------------|
| **[03-main-export-pipeline.md](03-main-export-pipeline.md)** | Coverage & deforestation export | `mapbiomas_brazil_export_pipeline.py` |
| **[04-export-public-datasets.md](04-export-public-datasets.md)** | Public GEE Data Catalog | `mapbiomas_brazil_export_datasets_public.py` |
| **[05-sentinel-filters-pipeline.md](05-sentinel-filters-pipeline.md)** | Sentinel filter stages (6 stages) | `export_integration_filters_pipeline_sentinel.py` |
| **[06-integration-filters-pipeline.md](06-integration-filters-pipeline.md)** | 30m integration filters | `export_integration_filters_pipeline.py` |
| **[07-statistics-pipeline.md](07-statistics-pipeline.md)** | BigQuery export | `mapbiomas_brazil_export_statistics_pipeline.py` |
| **[08-territory-rasterization.md](08-territory-rasterization.md)** | Rasterize territory vectors | `mapbiomas_brazil_convert_to_raster.py` |
| **[09-bigquery-setup.md](09-bigquery-setup.md)** | Initialize BigQuery tables | `setup_statistics_tables.py` |

### Reference

| Doc | Purpose |
|-----|---------|
| **[10-deforestation-rules.md](10-deforestation-rules.md)** | Temporal rules & `min_start` parameter explanation |
| **[11-water-data-debugging.md](11-water-data-debugging.md)** | Water asset structure & troubleshooting |
| **[12-common-workflows.md](12-common-workflows.md)** | Typical scenarios & recipes |
| **[13-troubleshooting-faq.md](13-troubleshooting-faq.md)** | Debugging GEE, BigQuery, Python errors |
| **[14-version-management.md](14-version-management.md)** | Collection ID, versioning conventions |
| **[15-collection-structure.md](15-collection-structure.md)** | GEE asset hierarchy & band naming |
| **[GLOSSARY.md](GLOSSARY.md)** | Terminology, class codes, abbreviations |

---

## When to Run Each Pipeline

### Annual Release Cycle

```
January:
  → Main Export (2–3 hours)
  → Public Datasets (1–2 hours, once integration ready)
  → Statistics (2–4 hours)
  → Validate in BigQuery & GEE

February–March:
  → Sentinel Filters (optional, ~5 hours)
  → Integration Filters (optional, ~3 hours)

Throughout:
  → Monitor GEE task queue
  → Check BigQuery for data quality
```

### One-Time Setup

```
First execution:
  1. Run Main Export
  2. Run Territory Rasterization
  3. Run BigQuery Setup
  4. Run Statistics
```

---

## Directory Structure

```
1_brazil/
├── mapbiomas_brazil_export_pipeline.py         # Main export
├── mapbiomas_brazil_export_datasets_public.py  # Public datasets
├── mapbiomas_brazil_export_integration_filters_pipeline.py
├── mapbiomas_brazil_export_integration_filters_pipeline_sentinel.py
├── README.md                                    # (redirect to docs/)
├── docs/                                        # ← You are here
│   ├── README.md                               # This file
│   ├── 01-setup-environment.md
│   ├── 02-pipelines-overview.md
│   ├── ... (03–15)
│   ├── GLOSSARY.md
│   └── EXAMPLES/                               # Example scripts
├── utilities/
│   ├── mapbiomas_brazil_convert_to_raster.py
│   ├── setup_statistics_tables.py
│   ├── cleanup_duplicate_statistics.py
│   ├── debug_water_data.py
│   └── ...
└── logs/                                        # Pipeline logs
```

---

## Troubleshooting

**Can't import GEE?**
→ See [01-setup-environment.md](01-setup-environment.md#troubleshooting)

**GEE task failed?**
→ See [13-troubleshooting-faq.md](13-troubleshooting-faq.md#gee-tasks)

**Water statistics empty?**
→ See [13-troubleshooting-faq.md](13-troubleshooting-faq.md#water-statistics)

**Don't know what a term means?**
→ See [GLOSSARY.md](GLOSSARY.md)

---

## Key Resources

| Resource | Link |
|----------|------|
| GEE Code Editor | https://code.earthengine.google.com |
| BigQuery Console | https://console.cloud.google.com/bigquery |
| MapBiomas Main Site | https://mapbiomas.org |
| GEE Datasets | https://developers.google.com/earth-engine/datasets |

---

## Scripts by Purpose

| Purpose | Script | Location |
|---------|--------|----------|
| Export coverage + deforestation | `mapbiomas_brazil_export_pipeline.py` | Root |
| Export public assets | `mapbiomas_brazil_export_datasets_public.py` | Root |
| Apply Sentinel filters (6 stages) | `export_integration_filters_pipeline_sentinel.py` | Root |
| Apply 30m filters | `export_integration_filters_pipeline.py` | Root |
| Export to BigQuery | `mapbiomas_brazil_export_statistics_pipeline.py` | Root |
| **Setup & utilities** | | |
| Initialize BigQuery tables | `setup_statistics_tables.py` | `utilities/` |
| Rasterize territories | `mapbiomas_brazil_convert_to_raster.py` | `utilities/` |
| Debug water data | `debug_water_data.py` | `utilities/` |
| Clean duplicate stats | `cleanup_duplicate_statistics.py` | `utilities/` |

---

## Document Map

```
Quick Start
    ↓
01-setup-environment     ← Authentication & dependencies
    ↓
02-pipelines-overview   ← Understand architecture
    ↓
[Choose your task]
    ├─→ Main export?              → 03-main-export-pipeline
    ├─→ Public datasets?           → 04-export-public-datasets
    ├─→ Statistics?               → 07-statistics-pipeline
    ├─→ Need deforestation rules? → 10-deforestation-rules
    └─→ Debugging?                → 13-troubleshooting-faq
```

---

## Notes

- **Two Python interpreters required:** GEE-only (`earthengine-api`) and GEE+BigQuery (`.venv`)
- **Assets are region-locked:** Each project can only write to its own folder in GEE
- **BigQuery requires setup:** Run `setup_statistics_tables.py` first time only
- **Territory raster is one-time:** Create once per collection release
- **Deforestation rules have temporal constraints:** See `min_start` in [10-deforestation-rules.md](10-deforestation-rules.md)
