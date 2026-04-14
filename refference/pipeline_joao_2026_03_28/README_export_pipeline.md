# MapBiomas Peru – Land Cover Export Script

This script exports different land use and cover products based on MapBiomas Peru Collection 3. The export is performed by 1:250,000 grids (CIM), with automatic task monitoring and descriptive metadata in Markdown format.

## 📦 Exported Products

- **Transitions between land use/cover classes**
- **Annual deforestation and secondary vegetation**
- **Cumulative deforestation and secondary vegetation**
- **Deforestation frequency**
- **Secondary vegetation age**

---

## 🛠️ Requirements

- Account on [Google Earth Engine](https://earthengine.google.com/)
- Installed `earthengine-api` library:
   ```bash
   pip install earthengine-api
   ```

---

## 🚀 Execution

1. Clone this repository:
    ```bash
    git clone https://github.com/joaosiqueira/mapbiomas-peru.git
    cd mapbiomas-peru
    ```

2. Authenticate with Earth Engine:
    ```bash
    earthengine authenticate
    ```

3. Run the script:
    ```bash
    python mapbiomas_peru_export_pipeline.py
    ```

---

## ⚙️ Parameterization

### Region and Collection

These parameters define which collection and country will be processed:
```python
REGION_NAME = 'peru'
COLLECTION_ID = '3'
TERRITORY_ID = '1_1_1'
```

### Year Range

Defines the period to be processed:
```python
YEARS = list(range(2000, 2025))  # from 2000 to 2024
```

### Execution Control

Enable or disable individual products:
```python
RUN_TRANSITIONS = True
RUN_DEF_ANNUAL = True
RUN_DEF_ACCUMULATED = True
RUN_DEF_FREQUENCY = True
RUN_SV_AGE = True
```

---

## 🧠 Dependency Logic

- The export of **cumulative deforestation and secondary vegetation data** depends on the completion of **annual data**.
- Other products can be exported in parallel.
- The script automatically manages task monitoring using `ThreadPoolExecutor`.

---

## 📝 Metadata

Each exported image contains a `description` field in **Markdown** format, which includes:

- Product description
- Methodology
- Input and output versions
- Analyzed year range
- Visualization scripts for Earth Engine

Example:
```markdown
### Annual Deforestation and Secondary Vegetation – MapBiomas PERU – Collection 3

**Description:** ...

**Visualization example script:**
```javascript
var image = ee.ImageCollection('...').filter(...);
Map.addLayer(...);
```
```

---

## 🗺️ Expected Output

The data is exported to the Earth Engine `Assets` in paths like:

```
projects/mapbiomas-peru/assets/LAND-COVER/COLLECTION3/PRODUCTS/...
```

Each task is registered with identifiers like:

```
deforestation-annual-SB-18-21-2
```

---

## 📌 Notes

- The export uses `ee.batch.Export.image.toAsset` with a 300 m buffer per grid.
- Tasks are continuously monitored until completion.
- Task errors are reported in the console with explanatory messages.
