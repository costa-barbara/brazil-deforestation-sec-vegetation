# MapBiomas Export Dataset Public

This Python script automates the export of MapBiomas products to the public folder on Earth Engine.

Includes support for:
- Land use and cover transitions (multi-bands)
- Annual integrated map
- Deforestation and secondary vegetation (annual, accumulated, frequency, and age)
- Annual quality mosaic

---

## ⚙️ Requirements

- Account on [Google Earth Engine](https://earthengine.google.com/)
- Python libraries:
  - `earthengine-api`
  - `time`, `datetime` (native)

Install the Earth Engine API with:

```bash
pip install earthengine-api
```

Authenticate with:

```bash
earthengine authenticate
```

---

## 🧭 Configuration

Edit the following parameters at the beginning of the script to adapt to your region:

| Parameter            | Description                                      |
|:---------------------|:-------------------------------------------------|
| `REGION_NAME`        | Region name (e.g., `'peru'`)                     |
| `COLLECTION_ID`      | MapBiomas collection ID (e.g., `'3'`)            |
| `SCALE`              | Spatial resolution for export (e.g., `30`)       |
| `VERSION_INPUT_*`    | Input versions for each product type             |
| `VERSION_OUTPUT_*`   | Output versions for publication                  |
| `RUN_EXPORT_*`       | Boolean flags to enable/disable exports          |

To optionally execute the export of some datasets, adjust the value of the `RUN_EXPORT_*` variables.
Example:
```python
RUN_EXPORT_DEF_SEC_VEG = True
RUN_EXPORT_QUALITY = False
```

---

## 🗂 Exportable Products

| Product                                  | Execution Flag                   | Function Name                                         |
|:-----------------------------------------|:---------------------------------|:-----------------------------------------------------|
| Transitions (multi-bands)                | `RUN_EXPORT_TRANSITIONS`         | `export_public_transitions()`                       |
| Integrated map                           | `RUN_EXPORT_INTEGRATION`         | `export_public_integration()`                       |
| Deforestation + Secondary Vegetation     | `RUN_EXPORT_DEF_SEC_VEG`         | `export_public_deforestation_and_secondary_vegetation()` |
| Deforestation + Secondary Vegetation (acc)| `RUN_EXPORT_DEF_SEC_VEG_ACC`    | `export_public_deforestation_and_secondary_vegetation_accumulated()` |
| Deforestation frequency                  | `RUN_EXPORT_DEF_FREQUENCY`       | `export_public_deforestation_frequency()`           |
| Secondary vegetation age                 | `RUN_EXPORT_SEC_VEG_AGE`         | `export_public_secondary_vegetation_age()`          |
| Quality (multi-bands per year)           | `RUN_EXPORT_QUALITY`             | `export_public_quality_mosaic()`                    |

---

## ▶️ Running the Script

After defining the parameters and enabling the desired products via flags, run the script with:

```bash
python3 mapbiomas_{region}_export_datasets_public.py
```

---

## ⏱ Task Monitoring

The script uses `wait_until_tasks_finish()` to:

- Wait for the completion of all exported tasks
- Display real-time status
- Measure total execution time
- Alert about failures with detailed messages

You will see messages like:

```bash
🚀 mapbiomas_peru_collection3_transitions_v3     | Status: RUNNING
✅ Task completed: mapbiomas_peru_collection3_integration_v1
❌ Task 'mapbiomas_peru_collection3_quality_v1' failed with error:
→ MaxPixelsExceededException: Too many pixels
```

---

## 📁 Export Structure

All files are exported to:

```
projects/mapbiomas-public/assets/{region}/collection{collection_id}/
```

Example:

```
projects/mapbiomas-public/assets/peru/collection3/mapbiomas_peru_collection3_transitions_v3
```

---

## 📌 Notes

- Ensure you do not exceed the `maxPixels` limits.
- Use the Earth Engine Code Editor to check tasks (`Tasks`).
- For future collections, simply update `COLLECTION_ID` and versions.

---

## 🧑‍💻 License

Distributed to support the official publication of MapBiomas data on Earth Engine.
