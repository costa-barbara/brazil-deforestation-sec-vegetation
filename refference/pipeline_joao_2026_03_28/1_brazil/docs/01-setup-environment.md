# Setup Environment for MapBiomas Brazil Pipeline

Complete guide for setting up Python, GEE authentication, and dependencies.

---

## Python Interpreters

The MapBiomas Brazil pipeline requires **two separate Python environments**:

### 1. Earth Engine API only (`earthengine-api`)

**Location:** `/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python`

**Used for:**
- `mapbiomas_brazil_export_pipeline.py` — main coverage/deforestation export
- `mapbiomas_brazil_export_datasets_public.py` — public asset exports
- `mapbiomas_brazil_export_integration_filters_pipeline_sentinel.py` — sentinel filters
- `mapbiomas_brazil_export_integration_filters_pipeline.py` — integration filters
- `mapbiomas_brazil_convert_to_raster.py` — territory rasterization

**Installation:**
```bash
pipx install earthengine-api
```

### 2. Earth Engine + BigQuery (`.venv`)

**Location:** `/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python`

**Used for:**
- `mapbiomas_brazil_export_statistics_pipeline.py` — statistics export
- `setup_statistics_tables.py` — BigQuery table initialization
- `cleanup_duplicate_statistics.py` — deduplication

**Installation:**
```bash
cd 1_brazil
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install earthengine-api google-cloud-bigquery
```

> **Important:** BigQuery scripts require `google-cloud-bigquery`. The `earthengine-api` venv does not have this dependency.

---

## Google Earth Engine Authentication

### Initialize GEE

```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python -m ee.ee_command authenticate --quiet
```

This opens a browser to authorize access. A credentials file is saved locally.

### Verify authentication

```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python -c "import ee; ee.Initialize(); print('GEE initialized successfully')"
```

### Project selection

Scripts initialize GEE with a specific project:

```python
import ee
ee.Initialize(project='mapbiomas-brazil')
```

Ensure your Google account has access to the appropriate GEE projects:
- `mapbiomas-brazil` (Brazil pipeline)
- `mapbiomas-public` (public assets)
- `mapbiomas-territories` (territory layers)

---

## Validate Setup

### Check Python interpreters

```bash
# EE API only
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python --version

# EE API + BigQuery
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python --version
```

### Check dependencies

```bash
# EE API venv
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python -m pip list | grep earthengine

# .venv (BigQuery)
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python -m pip list | grep -E "earthengine|google-cloud-bigquery"
```

### Test GEE connection

```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python << 'EOF'
import ee
ee.Initialize(project='mapbiomas-brazil')
print('✓ GEE initialized')
print(f"Project: {ee.batch.task_state.UNKNOWN}")
EOF
```

### Test BigQuery connection

```bash
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python << 'EOF'
from google.cloud import bigquery
client = bigquery.Client(project='mapbiomas-brazil')
print('✓ BigQuery initialized')
EOF
```

---

## Running Scripts

### Template: GEE-only script

```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/mapbiomas_brazil_export_pipeline.py
```

### Template: Statistics script (GEE + BigQuery)

```bash
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/mapbiomas_brazil_export_statistics_pipeline.py
```

---

## Troubleshooting

### "Module earthengine not found"

**Solution:**
- Check the interpreter: `which python`
- Reinstall: `pipx reinstall earthengine-api`
- Ensure `.venv` is activated: `source .venv/bin/activate`

### "ModuleNotFoundError: No module named 'google.cloud'"

**Solution:** You're using the wrong interpreter. For BigQuery scripts, use:
```bash
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python
```

### "EE error: 'mapbiomas-brazil' project not found"

**Solution:**
- Verify authentication: `ee.auth.refresh_credentials()`
- Check project access in GEE: https://code.earthengine.google.com

### "credentials.json not found"

**Solution:** Run authentication again:
```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python -m ee.ee_command authenticate
```
