# Scripts

Scripts auxiliares reutilizáveis para o pipeline MapBiomas.

## `get_categories.py`

Script genérico para consultar o BigQuery e listar todas as categorias de território disponíveis em `mapbiomas.mapbiomas_brazil_statistics.territories`.

Usado para gerar a lista `WATER_CATEGORIES` (e potencialmente outras listas de categorias) para exportações de estatísticas.

### Uso

```bash
python scripts/get_categories.py
```

### Saída

Printa a lista de categorias em formato Python dict, pronto para copiar/colar no pipeline:

```python
WATER_CATEGORIES = [
    {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
    {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
    ...
]
```

### Quando usar

- Atualizar a lista de categorias no pipeline após mudanças no BigQuery
- Verificar quais categorias estão disponíveis para exportação
- Gerar a lista completa de todas as categorias de território

### Pré-requisitos

- Python 3.7+
- `google-cloud-bigquery` instalado
- Credenciais de acesso ao BigQuery configuradas
- Usar o interpreter: `/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python`

### Exemplo de execução

```bash
/Users/joaosiqueira/Documents/Projects/mapbiomas-brazil/.venv/bin/python scripts/get_categories.py

# Output:
# 🔄 Querying BigQuery for territory categories...
# ✅ Found 31 categories:
#
# WATER_CATEGORIES = [
#     {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
#     ...
# ]
```
