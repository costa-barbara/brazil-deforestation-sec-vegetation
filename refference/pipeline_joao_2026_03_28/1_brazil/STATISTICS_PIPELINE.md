# MapBiomas Brazil — Statistics Pipeline

Exportação de estatísticas de cobertura, deforestação, transições e água para BigQuery.

---

## Visão geral

O pipeline de estatísticas calcula área (km²) agrupada por:
- **Categoria** (POLITICAL_LEVEL_1, POLITICAL_LEVEL_2, BIOMES, etc.)
- **Classe** de cobertura / deforestação / água
- **Período** (ano para cobertura/deforestação/água anual; ano+mês para água mensal)

Todos os dados são exportados para BigQuery no dataset `mapbiomas_brazil_statistics`.

---

## Arquivos principais

| Arquivo | Função |
|---------|--------|
| `mapbiomas_brazil_export_statistics_pipeline.py` | Pipeline principal de exportação |
| `mapbiomas_brazil_convert_to_raster.py` | Rasteriza territórios em imagens GEE |
| `setup_statistics_tables.py` | Inicializa dataset e tabelas BigQuery |
| `debug_water_data.py` | Debug para estatísticas de água |

---

## Fluxo de execução

### 1. Inicializar infraestrutura BigQuery

```bash
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/setup_statistics_tables.py
```

Cria:
- Dataset: `mapbiomas_brazil_statistics`
- Tabelas: `coverage`, `deforestation_annual`, `transitions`, `water_annual`, `water_monthly`

### 2. Rasterizar territórios (primeira execução)

```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/mapbiomas_brazil_convert_to_raster.py
```

Submete 9 tasks GEE para rasterizar territórios em imagens:
- POLITICAL_LEVEL_1, POLITICAL_LEVEL_2, POLITICAL_LEVEL_3
- BIOMES, COASTAL_MARINE_ZONE
- BASIN_LEVEL_1–2 PNRH, BASIN_LEVEL_1–2 DNAEE

Aguarde conclusão (monitorar em https://code.earthengine.google.com).

### 3. Executar pipeline de estatísticas

```bash
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/mapbiomas_brazil_export_statistics_pipeline.py
```

**Antes de rodar**, configure quais exportações ativar no topo do script:

```python
RUN_EXPORT_COVERAGE      = True   # Ativar exportação de cobertura
RUN_EXPORT_DEFORESTATION = True   # Ativar exportação de deforestação
RUN_EXPORT_TRANSITIONS   = True   # Ativar exportação de transições
RUN_EXPORT_WATER_ANNUAL  = True   # Ativar exportação de água anual
RUN_EXPORT_WATER_MONTHLY = True   # Ativar exportação de água mensal
```

---

## Configuração

### Constantes de período

```python
YEARS_COVERAGE      = list(range(1985, 2025))      # 1985-2024
YEARS_DEFORESTATION = list(range(1985, 2025))
YEARS_WATER         = list(range(1985, 2025))
MONTHS              = list(range(1, 13))            # 1-12
```

### Categorias de territórios

```python
CATEGORIES = [
    {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
    {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
    # ...
]

WATER_CATEGORIES = [
    {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
    # Descomentar conforme necessário
]
```

### Assets GEE

```python
ASSET_COVERAGE      = f"projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION10/..."
ASSET_DEFORESTATION = f"projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION10/DEFORESTATION/..."
ASSET_TRANSITIONS   = f"projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION10/transitions"
ASSET_THEMES        = f"projects/mapbiomas-territories/assets/TERRITORIES/LULC/BRAZIL/COLLECTION10/territory-collection"

# Water
ASSET_WATER_ANNUAL  = 'projects/mapbiomas-brazil/assets/WATER/COLLECTION-5/mapbiomas_brazil_collection5_water_v1'
ASSET_WATER_MONTHLY = 'projects/mapbiomas-brazil/assets/WATER/COLLECTION-5/mapbiomas_brazil_collection5_water_monthly_v1'
```

---

## Tabelas BigQuery

Dataset: `mapbiomas_brazil_statistics`

### coverage

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `category` | STRING | Tipo de território |
| `feature_id` | INTEGER | ID do feature dentro da categoria |
| `feature_name` | STRING | Nome do território |
| `class` | INTEGER | Classe de cobertura (0-62) |
| `area_km2` | FLOAT64 | Área em km² |
| `year` | INTEGER | Ano |

### deforestation_annual

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `category` | STRING | Tipo de território |
| `feature_id` | INTEGER | ID do feature |
| `feature_name` | STRING | Nome do território |
| `class` | INTEGER | Classe de deforestação |
| `area_km2` | FLOAT64 | Área em km² |
| `year` | INTEGER | Ano |

### transitions

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `category` | STRING | Tipo de território |
| `feature_id` | INTEGER | ID do feature |
| `feature_name` | STRING | Nome do território |
| `class_from` | INTEGER | Classe de origem |
| `class_to` | INTEGER | Classe de destino |
| `area_km2` | FLOAT64 | Área transicionada (km²) |
| `year_from` | INTEGER | Ano inicial |
| `year_to` | INTEGER | Ano final |

### water_annual

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `category` | STRING | Tipo de território |
| `feature_id` | INTEGER | ID do feature |
| `feature_name` | STRING | Nome do território |
| `water_class` | INTEGER | Classe de água |
| `area_km2` | FLOAT64 | Área (km²) |
| `year` | INTEGER | Ano |

### water_monthly

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `category` | STRING | Tipo de território |
| `feature_id` | INTEGER | ID do feature |
| `feature_name` | STRING | Nome do território |
| `water_class` | INTEGER | Classe de água |
| `area_km2` | FLOAT64 | Área (km²) |
| `year` | INTEGER | Ano |
| `month` | STRING | Mês ("01"-"12") |

---

## Debugging

### Verificar status dos rasters de territórios

```bash
# Verificar se territory-collection existe e tem imagens
# (será útil criar um script auxiliar para isso)
```

### Debug de água

```bash
# Script de debug para investigar estatísticas vazias
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/debug_water_data.py
```

Verifica:
1. Se a imagem de água carrega e tem dados
2. Se a collection de territórios existe e tem imagens
3. Se a interseção produz resultados

### Verificar resultados no BigQuery

```sql
SELECT COUNT(*) as total_rows
FROM `mapbiomas-brazil.mapbiomas_brazil_statistics.water_annual`
WHERE year = 2023;

SELECT DISTINCT category, COUNT(DISTINCT feature_id) as features
FROM `mapbiomas-brazil.mapbiomas_brazil_statistics.water_annual`
WHERE year = 2023
GROUP BY category;
```

---

## Troubleshooting

### "FeatureCollection is empty"

**Causa:** territorio-collection não foi rasterizado.

**Solução:**
1. Executar `mapbiomas_brazil_convert_to_raster.py`
2. Aguardar conclusão das tasks GEE
3. Verificar em GEE se `territory-collection` tem imagens

### "No tasks submitted" / pipeline rápido demais

**Causa:** Flags `RUN_EXPORT_*` estão todas como `False`.

**Solução:** Ativar as flags desejadas no topo do script.

### Resultados muito pequenos ou muito grandes

**Causa:** Possível erro na lógica de redução ou grouping.

**Ação:** Executar `debug_water_data.py` para verificar reduções intermediárias.

---

## Notas de performance

- **Category mode (recomendado):** 1 `reduceRegion` por categoria × ano = 9 × 40 = 360 calls
- **Feature mode (lento):** 1 `reduceRegion` por feature × categoria × ano = muito maior

Por isso o script usa `RASTERIZE_MODE = 'category'` por padrão.

---

## Links úteis

- [GEE Code Editor](https://code.earthengine.google.com)
- [BigQuery Console](https://console.cloud.google.com/bigquery)
- [MapBiomas Assets - Territories](https://developers.google.com/earth-engine/datasets/catalog/projects_mapbiomas-territories)
