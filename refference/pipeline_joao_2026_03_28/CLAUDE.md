# MapBiomas Pipeline — CLAUDE.md

> 📚 **Documentação completa em [1_brazil/docs/](1_brazil/docs/)** — comece em [1_brazil/docs/README.md](1_brazil/docs/README.md)

## Visão geral

Pipeline de exportação de dados de cobertura e uso da terra do MapBiomas usando a API Python do Google Earth Engine (GEE). Cobre múltiplas regiões (Brasil, Bolívia, Peru, Indonésia, Colômbia, Venezuela, Amazônia, Equador, Chile, Argentina, Uruguai, Chaco, Mata Atlântica, Pampa) com scripts independentes por região.

---

## Ambiente

**Dois intérpretes Python necessários:**

1. **GEE apenas** (`earthengine-api`): `/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python`
   - Pipelines principais (export, public, filters)
   - Territory rasterization

2. **GEE + BigQuery** (`.venv`): `/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python`
   - Estatísticas (BigQuery export)
   - Setup de tabelas

Para rodar um script:

```bash
# GEE-only (export pipeline)
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python 1_brazil/mapbiomas_brazil_export_pipeline.py

# GEE + BigQuery (statistics)
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python \
    1_brazil/mapbiomas_brazil_export_statistics_pipeline.py
```

**Ver [1_brazil/docs/01-setup-environment.md](1_brazil/docs/01-setup-environment.md) para detalhes completos.**

### Projeto GEE por região

Cada script autentica no GEE usando o projeto `mapbiomas-{região}`:

| Região | Projeto GEE |
|---|---|
| Brasil | `mapbiomas-brazil` |
| Bolívia | `mapbiomas-bolivia` |
| Peru | `mapbiomas-peru` |
| Indonésia | `mapbiomas-indonesia` |
| Colômbia | `mapbiomas-colombia` |
| Venezuela | `mapbiomas-venezuela` |
| Amazônia | `mapbiomas-amazon` |
| Equador | `mapbiomas-ecuador` |
| Chile | `mapbiomas-chile` |
| Argentina | `mapbiomas-argentina` |
| Uruguai | `mapbiomas-uruguay` |
| Chaco | `mapbiomas-chaco` |
| Mata Atlântica | `mapbiomas-atlantic_forest` |
| Pampa | `mapbiomas-pampa` |

```python
ee.Initialize(project=f'mapbiomas-{REGION_NAME}')
```

Assets públicos são escritos em `projects/mapbiomas-public/assets/{região}/...`

---

## Estrutura de diretórios

```
mapbiomas-pipeline/
├── 1_brazil/                  # Pipeline do Brasil (Collection 10.1)
│   ├── mapbiomas_brazil_export_pipeline.py           # Pipeline principal de exportação
│   ├── mapbiomas_brazil_export_datasets_public.py    # Exportação para pasta pública GEE
│   ├── mapbiomas_brazil_export_statistics_pipeline.py # Exportação de estatísticas para BigQuery
│   ├── mapbiomas_brazil_export_integration_filters_pipeline.py
│   ├── mapbiomas_brazil_export_integration_filters_pipeline_sentinel.py
│   ├── README.md              # Índice + redirect para docs/
│   ├── docs/                  # Documentação completa (leia primeiro!)
│   │   ├── README.md                                 # Índice de documentação
│   │   ├── 01-setup-environment.md                   # Python, GEE auth, dependências
│   │   ├── 02-pipelines-overview.md                  # Arquitetura e fluxo dos 5 pipelines
│   │   ├── 03–09-pipeline-guides.md                  # Guias para cada pipeline
│   │   ├── 10-deforestation-rules.md                 # Regras temporais e min_start
│   │   ├── 11–15-reference-docs.md                   # Referência (agua, workflows, troubleshooting)
│   │   ├── GLOSSARY.md                               # Terminologia e códigos de classe
│   │   └── EXAMPLES/                                 # Exemplos de uso
│   └── utilities/             # Scripts auxiliares e de setup/manutenção
│       ├── mapbiomas_brazil_convert_to_raster.py    # Rasteriza territórios em imagens GEE
│       ├── setup_statistics_tables.py                # Inicializa dataset BigQuery
│       ├── cleanup_duplicate_statistics.py           # Deduplicação de tabelas BigQuery
│       ├── debug_water_data.py                       # Debug de estatísticas de água
│       ├── asset_info.sh                             # Inspeciona assets GEE
│       ├── set_time_properties.sh                    # Define propriedades de tempo em assets
│       └── Untitled-1.ipynb                          # Notebook exploratório
├── 2_bolivia/
├── 3_peru/
├── 4_indonesia/
├── 5_colombia/
├── 6_venezuela/
├── 7_amazon/
├── 8_ecuador/
├── 9_chile/                   # Exportação pública + estatísticas (BigQuery)
├── 10_argentina/
├── 11_uruguay/
├── 12_chaco/
├── 13_atlantic_forest/
├── 14_pampa/
├── deforestation/
│   └── deforestation_secondary_vegetation.py  # Classe DeforestationSecondaryVegetation
├── scripts/
│   └── discover_territory_assets.py           # Descobre assets de territórios disponíveis
├── utils/
│   ├── region_utils.py          # Classe RegionUtils (bbox, grids por ISO3)
│   ├── export_utils.py          # Funções auxiliares compartilhadas (asset, export, territory)
│   ├── export_public_utils.py   # Funções de exportação pública compartilhadas entre regiões
│   └── bigquery_utils.py        # Utilidades para gestão de tabelas BigQuery
└── shell/                       # Utilitários GEE: mover assets, permissões públicas
```

---

## Convenções de código

### COLLECTION_ID

`COLLECTION_ID` é definido como `int` ou `float` — **nunca string**. Não existe `SUB_COLLECTION`: a sub-coleção faz parte do valor float (ex: `10.1`).

| Uso | Código |
|---|---|
| Path de asset | `str(COLLECTION_ID).replace(".", "_")` → `"2"` ou `"10_1"` |
| Campo BQ `collection` | `str(COLLECTION_ID)` → `"2"` ou `"10.1"` |

```python
COLLECTION_ID = 10.1
ASSET_COLLECTION = f'projects/mapbiomas-{REGION_NAME}/assets/LAND-COVER/COLLECTION-{str(COLLECTION_ID).replace(".", "_")}'
```

### RegionUtils — bbox por região

`REGION_BBOX` **nunca deve ser hardcoded**. Sempre usar `RegionUtils`:

```python
from utils.region_utils import RegionUtils

region_utils = RegionUtils()
REGION_ISO3 = 'PER'
REGION_BBOX = region_utils.get_bbox(iso3=REGION_ISO3)
```

Chaves disponíveis (ISO3 padrão ou identificador de bioma):

| Chave | Região |
|---|---|
| `BRA`, `BOL`, `PER`, `IDN`, `COL`, `VEN`, `ECU`, `CHL`, `ARG`, `URY` | Países |
| `AMZ` | Amazônia (Pan-Amazônia) |
| `CHACO` | Chaco |
| `AF` | Mata Atlântica |
| `PAMPA` | Pampa |

O parâmetro `iso3` do `get_bbox()` é apenas uma chave de dicionário — regiões não-ISO3 usam identificadores descritivos.

---

### Regras de kernel (deforestation)

Toda regra deve ter exatamente 3 elementos: `[kernel_bef, kernel_aft, min_start]`.

| Campo | Tipo | Descrição |
|---|---|---|
| `kernel_bef` | `list[int]` | Classes esperadas na entrada (estado antes) |
| `kernel_aft` | `list[int]` | Classes a escrever no match (estado depois) |
| `min_start` | `int` | Índice mínimo em `YEARS` para aplicar a regra |

Critério para `min_start`:
- `0` — regra não escreve classe `3` ou `5` em `t2`; segura desde o primeiro ano
- `1` — regra escreve classe `3` ou `5` em `t2`; requer histórico anterior a 1985

`min_start` é **obrigatório** em todas as regras. Omitir levanta `ValueError`.

```python
# correto
[[1, 2, 2, 2], [1, 5, 3, 3], 1]   # min_start=1: t2 recebe classe 5
[[2, 2, 1, 1], [2, 2, 4, 1], 0]   # min_start=0: nenhuma classe 3 ou 5 escrita

# errado — levanta ValueError
[[2, 2, 1, 1], [2, 2, 4, 1]]
```

### Flags de execução

Cada produto tem uma flag `RUN_EXPORT_*`. Apenas as flags `True` são processadas:

```python
RUN_EXPORT_DEF_SEC_VEG  = True
RUN_EXPORT_TRANSITIONS  = False
```

### Versionamento

Versões de entrada e saída são declaradas como constantes no topo de cada script:

```python
VERSION_INPUT_INTEGRATION    = '0-4-tra-1'
VERSION_PUBLIC_DEF_SEC_VEG   = f'{VERSION_INPUT_INTEGRATION}-2'
```

Convenção de nomenclatura nos scripts `export_datasets_public.py`:
- `ASSET_*` — assets de trabalho internos (leitura)
- `PUBLIC_*` — assets/coleções públicos finais (escrita para `projects/mapbiomas-public/`)
- `VERSION_INPUT_*` — versão do asset de entrada
- `VERSION_PUBLIC_*` — versão do asset público de saída
- `PROJECT_PUBLIC_PATH = 'projects/mapbiomas-public/assets'` — base de todos os paths públicos

Ao bumpar `VERSION_PUBLIC_*`, atualizar também os `VERSION_INPUT_*` correspondentes no script `mapbiomas_{region}_export_datasets_public.py` da região afetada.

---

## utils/export_utils.py

Módulo compartilhado com funções auxiliares usadas por todos os scripts `export_datasets_public.py`. **Nunca duplicar essas funções nos scripts regionais.**

### Funções disponíveis

| Função | Descrição |
|---|---|
| `wait_until_tasks_finish(tasks, interval)` | Monitora tasks GEE com tolerância a erros de rede |
| `asset_exists(asset_id)` | Retorna `True` se o asset existe no GEE |
| `ensure_asset_exists(path, asset_type)` | Cria o asset se não existir |
| `set_asset_properties(asset_id, properties)` | Define propriedades via CLI `earthengine` |
| `get_pyramiding_policy_mode(image, policy)` | Gera dict de pyramiding policy para todas as bandas |
| `resolve_territory_asset(asset_path)` | Resolve melhor versão disponível de territory asset; fallback para TERRITORIES-STAGING |
| `export_image_to_asset(image, path, ..., overwrite=False)` | Mascara e exporta imagem; pula se já existe; `overwrite=True` deleta antes de re-exportar |

### Padrão de uso nos scripts regionais

Cada script regional importa as funções e define um wrapper local `export_image_to_asset` que injeta as constantes da região:

```python
from utils.export_utils import (
    wait_until_tasks_finish,
    ensure_asset_exists,
    set_asset_properties,
    export_image_to_asset as _export_image_to_asset,
)

def export_image_to_asset(image, asset_path, version=None, scale=30,
                          additional_properties=None, region_mask=REGION_MASK):
    """Wrapper around export_utils.export_image_to_asset with region defaults."""
    return _export_image_to_asset(
        image, asset_path,
        region_mask=region_mask,
        region_bbox=REGION_BBOX,
        collection_id=COLLECTION_ID,
        region_name=REGION_NAME,
        max_pixels=EXPORT_MAX_PIXELS,
        version=version,
        scale=scale,
        additional_properties=additional_properties,
        pyramiding_policy=EXPORT_PYRAMIDING_POLICY,
    )
```

Se a região tiver uma segunda máscara (ex: Brasil tem `REGION_MASK_CORALS`), passar `region_mask=REGION_MASK_CORALS` na chamada — o wrapper aceita override.

### resolve_territory_asset

Resolve dinamicamente a melhor versão disponível de um territory asset. Dado um path terminado em `_vN`, lista irmãos na mesma pasta GEE e retorna o de maior versão. Faz fallback para `TERRITORIES-STAGING/` se nada for encontrado em `TERRITORIES/`.

```python
from utils.export_utils import resolve_territory_asset

ASSET_TERRITORY = resolve_territory_asset(
    f'{PROJECT_TERRITORY_PATH}/TERRITORIES/ARGENTINA/WORKSPACE/POLITICAL_LEVEL_1/POLITICAL_LEVEL_1_v1'
)
```

### Importar apenas o necessário

Não importar funções que o script não usa diretamente. `get_pyramiding_policy_mode` e `asset_exists` são usados internamente pelo `export_image_to_asset` do utils — não precisam ser importados nos scripts regionais.

---

## utils/bigquery_utils.py

Módulo compartilhado para gestão de tabelas BigQuery nos scripts de estatísticas.

### Funções disponíveis

| Função | Descrição |
|--------|-----------|
| `table_exists(table_id)` | Verifica se tabela existe no BigQuery |
| `ensure_dataset_exists(dataset_id, project)` | Cria dataset se não existir |
| `ensure_table_exists(table_id, schema)` | Cria tabela com schema se não existir |
| `list_tables(dataset_id)` | Lista todas as tabelas de um dataset |

### Schemas predefinidos

Cada função `get_schema_*()` retorna um dict com estrutura de tabela:

```python
from utils.bigquery_utils import get_schema_water_annual

schema = get_schema_water_annual()
# retorna [
#     {"name": "category", "type": "STRING", "mode": "NULLABLE"},
#     {"name": "water_class", "type": "INTEGER", "mode": "NULLABLE"},
#     {"name": "area_km2", "type": "FLOAT64", "mode": "NULLABLE"},
#     {"name": "year", "type": "INTEGER", "mode": "NULLABLE"},
# ]
```

Disponíveis: `get_schema_coverage()`, `get_schema_deforestation()`, `get_schema_transitions()`,
`get_schema_water_annual()`, `get_schema_water_monthly()`.

### Padrão de uso no `setup_statistics_tables.py`

```python
from utils.bigquery_utils import ensure_dataset_exists, ensure_table_exists, get_schema_water_annual

dataset_id = ensure_dataset_exists("mapbiomas_brazil_statistics", project="mapbiomas-brazil")
table_id = f"mapbiomas-brazil.mapbiomas_brazil_statistics.water_annual"
ensure_table_exists(table_id, get_schema_water_annual())
```

---

## utils/export_public_utils.py

Módulo com funções de exportação pública reutilizadas por todos os 14 scripts `export_datasets_public.py`. **Nunca duplicar esses corpos de função nos scripts regionais.**

### Funções disponíveis

| Função | Descrição |
|---|---|
| `export_coverage_gee_data_catalog(export_fn, years, catalog_path, source_asset, collection_id, version, region_name, overwrite=False)` | Exporta uma imagem por ano para o GEE Data Catalog |
| `export_collection_product(export_fn, asset, version_input, public_path, version_public, ...)` | Carrega ImageCollection, filtra versão, `.min()`, exporta |
| `export_quality_mosaic(export_fn, asset, years, public_path, version_public)` | Mosaico de qualidade multi-ano |
| `export_water_surface(export_fn, asset, years, public_path, version_public, ..., use_collection=True)` | Superfície de água anual; `use_collection=False` para Image direta |
| `export_water_bodies(export_fn, asset, years, public_path, version_public, ..., class_remap=None)` | Corpos d'água anuais; remap opcional |
| `export_water_monthly(export_fn, asset, years, public_path, version_public, ..., use_version_filter=True, month_as_string=True)` | Água mensal como ImageCollection |

### Padrão de importação nos scripts regionais

```python
from utils.export_public_utils import (
    export_coverage_gee_data_catalog,
    export_collection_product,
    export_quality_mosaic,
    export_water_surface,
    export_water_bodies,
    export_water_monthly,
)
```

### Nomenclatura de assets no catálogo

O nome do asset usa `collection_id` normalizado: `str(collection_id).replace('.', '_')`.
Exemplo: `COLLECTION_ID = 10.1` → `collection_10_1_1985_v1` (não `collection_10.1_...`).

### overwrite no catalog export

Brasil tem versão anterior no catálogo — usar `overwrite=True` ao re-exportar. **Nunca** apagar a ImageCollection destino; apenas os assets individuais por ano.

```python
export_tasks.extend(export_coverage_gee_data_catalog(
    export_image_to_asset,
    YEARS, PUBLIC_COVERAGE_DATA_CATALOG, ASSET_COVERAGE_CATALOG,
    COLLECTION_ID, VERSION_PUBLIC_INTEGRATION, REGION_NAME,
    overwrite=True,  # somente para Brasil (já tem versão anterior)
))
```

---

## Pipeline de filtros sentinel (`export_integration_filters_pipeline_sentinel.py`)

Script sequencial de 6 etapas para o Brasil (Sentinel/10m). Cada etapa submete tasks GEE e aguarda antes de iniciar a próxima:

1. Silvicultura (`VERSION_SILVICULTURA_FILTER`)
2. General (`VERSION_GENERAL_FILTER`)
3. Agriculture (`VERSION_AGRICULTURE_FILTER`)
4. Pasture (`VERSION_PASTURE_FILTER`)
5. Alertas (`VERSION_ALERTAS_FILTER`)
6. Transitions (`VERSION_TRANSITIONS_FILTER`)

O script tem sua **própria** `wait_until_tasks_finish` (não importa de `utils/export_utils`). O formato de relatório adotado é **resumo por ciclo de polling**:

```
[2026-03-10T14:32:05] elapsed=0:12:30 | Total=98  COMPLETED=51  RUNNING=5  READY=42  FAILED=0
  RUNNING (5):
    integration-filter-silviculture-SA-23-0-4-sil-1
    ...
  FAILED (2):
    integration-filter-general-NB-22-0-4-gen-1 → Computation timed out.
```

- Uma linha de resumo por poll (não uma linha por task)
- Tasks RUNNING listadas por nome
- Tasks FAILED listadas com mensagem de erro
- Ao final: tempo total + lista completa de falhas

Imports obrigatórios no script: `from collections import Counter`.

---

## Scripts de estatísticas (BigQuery)

Scripts do tipo `export_statistics_*` exportam área por território para o BigQuery. **Não usam `export_image_to_asset`** — importam apenas `wait_until_tasks_finish`.

### Padrão obrigatório para exportação de tabelas

```python
from utils.export_utils import wait_until_tasks_finish

def export(areas, name, table):
    task = ee.batch.Export.table.toBigQuery(...)
    task.start()
    return task  # sempre retornar a task

export_tasks = []
# no loop de exportação:
export_tasks.append(export(areas, name, BIG_QUERY_TABLE))

# ao final do script:
wait_until_tasks_finish(export_tasks)
```

### Flags de execução e constantes

O script `mapbiomas_brazil_export_statistics_pipeline.py` usa flags `RUN_EXPORT_*` para controlar execução seletiva:

```python
RUN_EXPORT_COVERAGE      = False
RUN_EXPORT_DEFORESTATION = False
RUN_EXPORT_TRANSITIONS   = False
RUN_EXPORT_WATER_ANNUAL  = False
RUN_EXPORT_WATER_MONTHLY = False
```

E constantes de período:

```python
YEARS_COVERAGE      = list(range(1985, 2025))
YEARS_DEFORESTATION = list(range(1985, 2025))
YEARS_WATER         = list(range(1985, 2025))
MONTHS              = list(range(1, 13))
```

---

## Rasterização de territórios (`1_brazil/utilities/mapbiomas_brazil_convert_to_raster.py`)

Script que converte vetores de territórios em imagens raster para o pipeline de estatísticas do BigQuery.

### Propósito

O pipeline de estatísticas agrupa pixels por `FEATURE_ID` via `Reducer.sum().group(1, 'feature_id')`.
Ter uma **única imagem raster por categoria** (onde cada pixel = `FEATURE_ID` de todas as features)
é muito mais eficiente que uma imagem por feature: `1 reduceRegion` por categoria×ano
vs `N reduceRegion` por feature×ano.

### Modos de rasterização

```python
RASTERIZE_MODE = 'category'  # ou 'feature'
```

| Modo | Imagens | Uso | Eficiência |
|------|---------|-----|-----------|
| `'category'` | 1 por categoria | **Recomendado** para statistics pipeline | ✅ Ótima |
| `'feature'` | 1 por feature | Comportamento original | ⚠️ Lenta |

### Configuração de categorias

```python
CATEGORIES = [
    {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": 1, "VERSION": "v1"},
    {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": 2, "VERSION": "v1"},
    {"CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": 3, "VERSION": "v1"},
    {"CATEGORY": "BIOMES", "CATEG_ID": 4, "VERSION": "v1"},
    {"CATEGORY": "COASTAL_MARINE_ZONE", "CATEG_ID": 5, "VERSION": "v1"},
    # ... mais categorias
]
```

- `CATEGORY`: nome do tipo de território (chave de pasta em GEE)
- `CATEG_ID`: identificador numérico da categoria
- `VERSION`: versão solicitada (ex: `"v1"`). A versão real é resolvida dinamicamente via `resolve_territory_asset()`
  se uma versão superior existir

### Caminhos de asset

```python
ASSET_FOLDER_PRIMARY  = 'projects/mapbiomas-territories/assets/TERRITORIES/BRAZIL/WORKSPACE'
ASSET_FOLDER_STAGING  = 'projects/mapbiomas-territories/assets/TERRITORIES-STAGING/BRAZIL/WORKSPACE'
ASSET_OUTPUT          = 'projects/mapbiomas-territories/assets/TERRITORIES/LULC/BRAZIL/COLLECTION10/territory-collection'
```

O script tenta carregar vectores da pasta PRIMARY; se falhar, faz fallback para STAGING via `resolve_territory_asset()`.

### Saída (no modo `'category'`)

Para cada categoria, gera uma imagem raster:

```
Nome do asset:  {categ_id}_{category_name}_{VERSION_OUTPUT}
Exemplo:        1_POLITICAL_LEVEL_1_1, 4_BIOMES_1, 5_COASTAL_MARINE_ZONE_1

Pixel value:    FEATURE_ID (uint32)
Metadados:      CATEG_ID, CATEG_NAME, version, rasterize_mode='category'
Escala:         30m (EPSG:4326)
Pyramiding:     'mode'
```

### Execução

```bash
# Rasterizar territórios
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/utilities/mapbiomas_brazil_convert_to_raster.py
```

Submete 9 tasks GEE (uma por categoria). Consulte a fila do GEE para monitorar progresso.

---

## utils/bigquery_utils.py

Módulo compartilhado para gestão de tabelas BigQuery usadas pelos scripts de estatísticas.

### Funções disponíveis

| Função | Descrição |
|--------|-----------|
| `table_exists(table_id)` | Verifica se tabela existe |
| `ensure_dataset_exists(dataset_id, project)` | Cria dataset se não existir |
| `ensure_table_exists(table_id, schema)` | Cria tabela com schema se não existir |
| `list_tables(dataset_id)` | Lista tabelas em um dataset |

### Schemas disponíveis

| Schema | Descrição |
|--------|-----------|
| `get_schema_coverage()` | Cobertura: categoria, área, ano |
| `get_schema_deforestation()` | Deforestação: categoria, área, ano |
| `get_schema_transitions()` | Transições: categoria_bef, categoria_aft, área, período |
| `get_schema_water_annual()` | Água anual: categoria, classe_água, área, ano |
| `get_schema_water_monthly()` | Água mensal: categoria, classe_água, área, ano, mês |

### Uso no `setup_statistics_tables.py`

```bash
/Users/joaosiqueira/.local/pipx/venvs/earthengine-api/bin/python \
    1_brazil/utilities/setup_statistics_tables.py
```

Cria dataset `mapbiomas_brazil_statistics` e tabelas iniciais no BigQuery.

---

## Gestão de assets via GEE CLI

```bash
earthengine create folder <path>      # cria pasta
earthengine create collection <path>  # cria ImageCollection
earthengine cp <src> <dst>            # copia asset (pode renomear ao copiar)
```

### Bug: propriedades string via CLI

O CLI interpreta `--property version=3` como FLOAT64. A sintaxe `string:3` armazena literalmente `"string:3"` (errado). **Sempre usar a Python API para setar propriedades string:**

```python
info = ee.data.getAsset(asset_id)
props = info.get('properties', {})
props['version'] = '3'
ee.data.updateAsset(asset_id, {'properties': props}, ['properties'])
```

---

## Convenções de commit

Formato: `tipo(escopo): descrição`

| Tipo | Quando usar |
|---|---|
| `fix` | Correção de bug ou comportamento incorreto |
| `feat` | Nova funcionalidade ou novo script |
| `docs` | Documentação (README, comentários) |
| `refactor` | Reorganização sem mudança de comportamento |

Escopos comuns: `(brazil)`, `(deforestation)`, `(bolivia)`, `(utils)`

---

## export_public_coverage_gee_data_catalog

Presente em todos os 14 scripts `export_datasets_public.py` via `utils/export_public_utils.py`. Exporta a imagem de cobertura para o GEE Data Catalog: uma imagem por ano com banda única `classification`.

### Constantes obrigatórias por região

| Constante | Descrição |
|---|---|
| `PUBLIC_COVERAGE_DATA_CATALOG` | `projects/mapbiomas-public/assets/{region}/lulc/v1` — ImageCollection destino |
| `ASSET_COVERAGE_CATALOG` | Imagem multibanda fonte — ver tabela abaixo |

| Região | `ASSET_COVERAGE_CATALOG` | Observação |
|---|---|---|
| Brazil | `PUBLIC_INTEGRATION` | depende de `export_public_integration()` ter rodado |
| Bolivia | `PUBLIC_INTEGRATION` | idem — verificar se o asset existe antes de rodar |
| Ecuador | `PUBLIC_INTEGRATION` | `VERSION_PUBLIC_INTEGRATION = 'v3'`; path usa `coverage` |
| Indonesia | `PUBLIC_INTEGRATION` | script tem bugs de configuração pré-existentes |
| Peru | `PUBLIC_INTEGRATION` | `PUBLIC_COLLECTION` não usa `lulc/`; pasta `peru/lulc` deve existir |
| Colombia | `PUBLIC_INTEGRATION` | `VERSION_PUBLIC_INTEGRATION = 'v2'`; path usa `coverage` |
| Venezuela | `PUBLIC_INTEGRATION` | path usa `coverage` |
| Amazon | `PUBLIC_INTEGRATION` | `COLLECTION_ID=6`; asset usa `mapbiomas_collection60_integration_...` |
| Chile | `PUBLIC_INTEGRATION` | `YEARS = range(1999, 2025)` |
| Argentina | `PUBLIC_INTEGRATION` | `VERSION_PUBLIC_INTEGRATION = 'v3'`; path usa `integration` |
| Uruguay | `PUBLIC_INTEGRATION` | `YEARS = range(1985, 2025)` |
| Chaco | `PUBLIC_INTEGRATION` | `VERSION_PUBLIC_INTEGRATION = 'v2'`; path usa `integration` |
| Atlantic Forest | `PUBLIC_INTEGRATION` | `COLLECTION_ID=4`; path usa `coverage` |
| Pampa | `PUBLIC_INTEGRATION` | `PUBLIC_COLLECTION` não usa `lulc/`; path usa `integration` |

**Antes de rodar o catalog export**, confirmar que `ASSET_COVERAGE_CATALOG` existe:
```bash
earthengine asset info <ASSET_COVERAGE_CATALOG>
```

### Uso nos scripts regionais

```python
if RUN_EXPORT_COVERAGE_DATA_CATALOG:
    export_tasks.extend(export_coverage_gee_data_catalog(
        export_image_to_asset,
        YEARS, PUBLIC_COVERAGE_DATA_CATALOG, ASSET_COVERAGE_CATALOG,
        COLLECTION_ID, VERSION_PUBLIC_INTEGRATION, REGION_NAME,
        # overwrite=True  ← apenas para Brasil (já tem versão anterior no catálogo)
    ))
```

- `ASSET_COVERAGE_CATALOG = PUBLIC_INTEGRATION` em todas as regiões
- Asset name: `collection_{collection_id_str}_{year}_{version}` onde `collection_id_str = str(collection_id).replace('.', '_')`
- Description das tasks GEE: `{region_name}:{asset_name}` — colchetes `[ ]` são inválidos no GEE
- Regiões sem pasta `lulc/` (Peru, Pampa): criar antes do 1º run:
  ```bash
  earthengine create folder projects/mapbiomas-public/assets/{region}/lulc
  ```

### Status do catalog export (por região)

| Região | Path catálogo | Período | Status |
|---|---|---|---|
| Bolivia | `.../bolivia/lulc/v1` | 1985–2024 | ✅ completo |
| Peru | `.../peru/lulc/v1` | 1985–2024 | ✅ completo |
| Venezuela | `.../venezuela/lulc/v1` | 1985–2024 | ✅ completo |
| Ecuador | `.../ecuador/lulc/v1` | 1985–2024 | ✅ completo |
| Uruguay | `.../uruguay/lulc/v1` | 1985–2024 | ✅ completo |
| Chile | `.../chile/lulc/v1` | 1999–2024 | ✅ completo |
| Colombia | `.../colombia/lulc/v1` | 1985–2024 | ✅ completo |
| Brazil (antigo) | `.../brazil/lulc/v1` | 1985–2024 | ✅ `collection_10_*` (versão anterior) |
| Brazil (novo) | `.../brazil/lulc/v1` | 1985–2024 | 🔄 40 tasks PENDING — `collection_10_1_*` |
| Argentina | `.../argentina/lulc/v1` | 1985–2024 | 🔄 40 tasks PENDING |
| Indonesia | `.../indonesia/lulc/v1` | — | ❌ collection não existe; script tem bugs preexistentes |
| Amazon | — | — | ⏳ pendente — projeto GEE e territory não existem |
| Atlantic Forest | — | — | ⏳ pendente — projeto `mapbiomas-atlantic_forest` não encontrado |
| Chaco | — | — | ⏳ pendente — sem permissão para `mapbiomas-chaco` |
| Pampa | — | — | ⏳ pendente — projeto `mapbiomas-pampa` não encontrado |

### Ecuador — particularidades

`PUBLIC_INTEGRATION` usa `coverage` no nome (não `integration`) e `VERSION_PUBLIC_INTEGRATION = 'v3'`:
```python
PUBLIC_INTEGRATION = f'{PUBLIC_COLLECTION}/mapbiomas_{REGION_NAME}_collection{COLLECTION_ID}_coverage_{VERSION_PUBLIC_INTEGRATION}'
# → projects/mapbiomas-public/assets/ecuador/lulc/collection3/mapbiomas_ecuador_collection3_coverage_v3
```

---

## O que não fazer

- Não commitar `.DS_Store` (já no `.gitignore`)
- Não omitir `min_start` nas regras de kernel
- Não definir `COLLECTION_ID` como string; não criar `SUB_COLLECTION`
- Não alterar versões de saída sem atualizar os `VERSION_INPUT_*` correspondentes em `export_datasets_public.py`
- Não duplicar em scripts regionais funções que já existem em `utils/export_utils.py` ou `utils/export_public_utils.py`
- Não adicionar imports (`time`, `subprocess`, `json`, `timedelta`) que só seriam usados pelas funções do `export_utils`
- Não hardcodar `REGION_BBOX` como lista — sempre usar `RegionUtils.get_bbox(iso3=REGION_ISO3)`
- Não usar colchetes `[ ]` na description de tasks GEE — usar `{region_name}:{asset_name}` (colchetes são caracteres inválidos)
