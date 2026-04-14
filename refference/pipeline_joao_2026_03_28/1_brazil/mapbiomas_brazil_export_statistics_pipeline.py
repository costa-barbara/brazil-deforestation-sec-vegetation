# -*- coding: utf-8 -*-
import ee
import time
from google.cloud import bigquery
from datetime import timedelta


client = bigquery.Client(project="mapbiomas-brazil")

# ================================
# AUTHENTICATION AND INITIALIZATION
# ================================
try:
    ee.Initialize(project='mapbiomas-brazil')
except Exception:
    print("Authenticating Earth Engine...")
    ee.Authenticate()
    ee.Initialize(project='mapbiomas-brazil')

# ---------------------------
# Inicialização e constantes
# ---------------------------

REGION = 'brazil'
COLLECTION = '10'
SUB_COLLECTION = '0'

INPUT_VERSION = '9-1'
STATISTICS_VERSION = f"{INPUT_VERSION}.1"
THEMES_VERSION = '1'  # Versão dos territórios rasterizados

ASSET_COVERAGE      = f"projects/mapbiomas-{REGION}/assets/LAND-COVER/COLLECTION{COLLECTION}/integracion-pais/PERU-9"
ASSET_DEFORESTATION = f"projects/mapbiomas-{REGION}/assets/LAND-COVER/COLLECTION{COLLECTION}/DEFORESTATION/deforestation-secondary-vegetation-annual"
ASSET_TRANSITIONS   = f"projects/mapbiomas-{REGION}/assets/LAND-COVER/COLLECTION{COLLECTION}/transitions"
ASSET_THEMES        = f"projects/mapbiomas-territories/assets/TERRITORIES/LULC/{REGION.upper()}/COLLECTION{COLLECTION}/territory-collection"

TABLE_COVERAGE      = f"mapbiomas.mapbiomas_{REGION}_statistics.coverage"
TABLE_DEFORESTATION = f"mapbiomas.mapbiomas_{REGION}_statistics.deforestation_annual"
TABLE_TRANSITIONS   = f"mapbiomas.mapbiomas_{REGION}_statistics.transitions"
TABLE_WATER_ANNUAL  = f"mapbiomas.mapbiomas_{REGION}_statistics.water_annual"
TABLE_WATER_MONTHLY = f"mapbiomas.mapbiomas_{REGION}_statistics.water_monthly"

ASSET_WATER_ANNUAL  = 'projects/mapbiomas-brazil/assets/WATER/COLLECTION-5/mapbiomas_brazil_collection5_water_v1'
ASSET_WATER_MONTHLY = 'projects/mapbiomas-brazil/assets/WATER/COLLECTION-5/mapbiomas_brazil_collection5_water_monthly_v1'

WATER_COLLECTION        = '5'
INPUT_VERSION_WATER     = 'v1'
STATISTICS_VERSION_WATER = INPUT_VERSION_WATER

YEARS_COVERAGE = list(range(1985, 2025))
MONTHS = list(range(1, 13))

# ---------------------------
# Controle de execução
# ---------------------------

RUN_EXPORT_COVERAGE      = False
RUN_EXPORT_DEFORESTATION = False
RUN_EXPORT_TRANSITIONS   = False
RUN_EXPORT_WATER_ANNUAL  = True
RUN_EXPORT_WATER_MONTHLY = True

YEARS_DEFORESTATION = list(range(1985, 2025))
YEARS_WATER = list(range(1985, 2026))  # Inclui 2025

PERIODS_TRANSITIONS = [
    (1985, 1986), (1986, 1987), (1987, 1988), (1988, 1989), (1989, 1990),
    (1990, 1991), (1991, 1992), (1992, 1993), (1993, 1994), (1994, 1995),
    (1995, 1996), (1996, 1997), (1997, 1998), (1998, 1999), (1999, 2000),
    (2000, 2001), (2001, 2002), (2002, 2003), (2003, 2004), (2004, 2005),
    (2005, 2006), (2006, 2007), (2007, 2008), (2008, 2009), (2009, 2010),
    (2010, 2011), (2011, 2012), (2012, 2013), (2013, 2014), (2014, 2015),
    (2015, 2016), (2016, 2017), (2017, 2018), (2018, 2019), (2019, 2020),
    (2020, 2021), (2021, 2022), (2022, 2023), (2023, 2024),
    (1985, 1990), (1990, 1995), (1995, 2000), (2000, 2005), (2005, 2010),
    (2010, 2015), (2015, 2020), (1990, 2000), (2000, 2010), (2010, 2020),
    (1985, 2024), (2008, 2017), (1994, 2002), (2002, 2010), (2010, 2016),
    (1986, 2015), (1990, 2024), (2000, 2024), (2008, 2024), (2010, 2024),
    (2012, 2024),
]

PIXEL_AREA = ee.Image.pixelArea().divide(1e6)

# Categorias para coverage/deforestation/transitions (histórico)
CATEGORIES = [
    {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
    {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
    {"CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": "3"},
    {"CATEGORY": "BIOMES", "CATEG_ID": "4"},
]

# Categorias para water statistics (geradas via utilities/get_categories.py)
# Atualizar com: python utilities/get_categories.py
WATER_CATEGORIES = [
    {"CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1"},
    {"CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2"},
    {"CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": "3"},
    {"CATEGORY": "BIOMES", "CATEG_ID": "4"},
    {"CATEGORY": "BASIN_LEVEL_1_PNRH", "CATEG_ID": "10"},
    {"CATEGORY": "BASIN_LEVEL_2_PNRH", "CATEG_ID": "11"},

    # {"CATEGORY": "COASTAL_MARINE_ZONE", "CATEG_ID": "5"},
    # {"CATEGORY": "BASIN_LEVEL_1_DNAEE", "CATEG_ID": "12"},
    # {"CATEGORY": "BASIN_LEVEL_2_DNAEE", "CATEG_ID": "13"},
    # {"CATEGORY": "UGRHS", "CATEG_ID": "14"},
    # {"CATEGORY": "DHN250_LEVEL_1", "CATEG_ID": "15"},
    # {"CATEGORY": "DHN250_LEVEL_2", "CATEG_ID": "16"},
    # {"CATEGORY": "DHN250_LEVEL_3", "CATEG_ID": "17"},
    # {"CATEGORY": "AMACRO", "CATEG_ID": "20"},
    # {"CATEGORY": "ATLANTIC_FOREST_LAW", "CATEG_ID": "21"},
    # {"CATEGORY": "LEGAL_AMAZON", "CATEG_ID": "22"},
    # {"CATEGORY": "SEMIARID", "CATEG_ID": "23"},
    # {"CATEGORY": "MATOPIBA", "CATEG_ID": "24"},
    # {"CATEGORY": "FLORESTAS_PUBLICAS_NAO_DESTINADAS", "CATEG_ID": "25"},
    # {"CATEGORY": "AREAS_PRIORITARIAS_DO_MMA_2018", "CATEG_ID": "40"},
    # {"CATEGORY": "GEOPARQUES", "CATEG_ID": "41"},
    # {"CATEGORY": "RESERVA_DA_BIOSFERA", "CATEG_ID": "42"},
    # {"CATEGORY": "PROTECTED_AREA", "CATEG_ID": "60"},
    # {"CATEGORY": "INDIGENOUS_TERRITORIES", "CATEG_ID": "61"},
    # {"CATEGORY": "QUILOMBOS", "CATEG_ID": "62"},
    # {"CATEGORY": "SETTLEMENTS", "CATEG_ID": "63"},
    # {"CATEGORY": "CONCESSOES_FLORESTAIS", "CATEG_ID": "71"},
    # {"CATEGORY": "METROPOLITAN_REGIONS", "CATEG_ID": "91"},
    # {"CATEGORY": "POPULATION_ARRANGEMENT", "CATEG_ID": "92"},
    # {"CATEGORY": "URBAN_CONCENTRATION", "CATEG_ID": "93"},
    # {"CATEGORY": "URBAN_PERIMETER", "CATEG_ID": "94"},
]

# ---------------------------
# Funções utilitárias
# ---------------------------

def wait_until_tasks_finish(export_tasks=None, polling_interval=60):
    """
    Waits until all export tasks created by this script have finished.

    Continues monitoring even if some tasks fail. Displays status and duration at the end.
    Output is formatted with consistent alignment.
    Skips processing if no tasks were submitted.
    """
    if not export_tasks:
        print("⚠️  No export tasks to monitor.")
        return

    start_time = time.time()
    print(f"\n⏳ Waiting for {len(export_tasks)} task(s) to finish...")

    completed = set()
    failed_tasks = []

    # Calculate max width for formatting
    max_len = max(len(task.status().get('description', '')) for task in export_tasks)
    label_pad = max(40, max_len + 4)

    while True:
        all_done = True

        for task in export_tasks:
            status = task.status()
            description = status.get('description', '[No Description]')
            state = status['state']

            if description in completed:
                continue

            if state in ['READY', 'RUNNING']:
                all_done = False
                print(f"🚀 {description.ljust(label_pad)} | {time.strftime('%Y-%m-%dT%H:%M:%S')} | Status: {state}")
            elif state == 'FAILED':
                error_message = status.get('error_message', 'No error message provided.')
                failed_tasks.append((description, error_message))
                completed.add(description)
                print(f"❌ {description.ljust(label_pad)} | {time.strftime('%Y-%m-%dT%H:%M:%S')} | Status: {state} | Error: {error_message} ")
            elif state == 'COMPLETED':
                completed.add(description)
                print(f"✅ {description.ljust(label_pad)} | {time.strftime('%Y-%m-%dT%H:%M:%S')} | Status: {state}")
            elif state == 'CANCELLED':
                completed.add(description)
                print(f"⚠️ {description.ljust(label_pad)} | {time.strftime('%Y-%m-%dT%H:%M:%S')} | Status: {state}")

        if all_done and len(completed) == len(export_tasks):
            total_time = time.time() - start_time
            formatted_time = str(timedelta(seconds=int(total_time)))
            print(f"\n✅ All tasks finished. Total time: {formatted_time}")
            if failed_tasks:
                print(f"\n❌ {len(failed_tasks)} task(s) failed:\n")
                for description, error in failed_tasks:
                    print(f"❌ {description}\n→ {error}\n")
            break

        time.sleep(polling_interval)


def check_existing_entries(category, table, collection, version, stat_type):
    """
    Consulta o BigQuery para identificar quais períodos já foram exportados.

    Para 'transitions', retorna uma lista de dicts com as chaves year_from e year_to.
    Para 'water_monthly', retorna uma lista de dicts com year e month.
    Para os demais tipos, retorna uma lista de anos (int) já presentes na tabela.

    Args:
        category (str): Nome da categoria territorial (ex: 'BIOMES').
        table (str): Tabela BigQuery no formato 'project.dataset.table'.
        collection (str): Versão da coleção (ex: '10.0').
        version (str): Versão das estatísticas (ex: '9-1.1').
        stat_type (str): Tipo de estatística ('coverage', 'deforestation_annual', 'transitions', 'water_annual', 'water_monthly').

    Returns:
        list: Anos (int), pares (dict), ou tuplas (dict) já existentes para a combinação categoria/versão.
    """
    params = [
        bigquery.ScalarQueryParameter("category",   "STRING", category),
        bigquery.ScalarQueryParameter("version",    "STRING", version),
        bigquery.ScalarQueryParameter("collection", "STRING", collection),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    if stat_type == 'transitions':
        query = f"""
        WITH unique_pairs AS (
            SELECT DISTINCT version, year_from, year_to
            FROM `{table}`
            WHERE
                category   = @category AND
                version    = @version AND
                collection = @collection)
        SELECT
            version,
            ARRAY_AGG(STRUCT(year_from, year_to)) AS unique_years
        FROM unique_pairs
        GROUP BY version
        """
        result = client.query(query, job_config=job_config).result()

        # BigQuery retorna structs como objetos Row, não como dicts.
        # Convertemos explicitamente para garantir que a comparação
        # {'year_from': y0, 'year_to': y1} in existing funcione corretamente.
        rows = {row.version: row.unique_years for row in result}.get(version, [])
        return [{'year_from': r['year_from'], 'year_to': r['year_to']} for r in rows]

    elif stat_type == 'water_monthly':
        query = f"""
        WITH unique_pairs AS (
            SELECT DISTINCT version, year, month
            FROM `{table}`
            WHERE
                category   = @category AND
                version    = @version AND
                collection = @collection)
        SELECT
            version,
            ARRAY_AGG(STRUCT(year, month)) AS unique_pairs
        FROM unique_pairs
        GROUP BY version
        """
        result = client.query(query, job_config=job_config).result()

        rows = {row.version: row.unique_pairs for row in result}.get(version, [])
        return [{'year': r['year'], 'month': r['month']} for r in rows]

    else:
        query = f"""
        SELECT version, ARRAY_AGG(DISTINCT year) AS years
        FROM `{table}`
        WHERE
            category   = @category AND
            version    = @version AND
            collection = @collection
        GROUP BY version
        """
        result = client.query(query, job_config=job_config).result()

        return {row.version: row.years for row in result}.get(version, [])


def build_area_features(stat_type):
    """
    Retorna uma função que converte o resultado do redutor de área em um FeatureCollection.

    O redutor agrupa pixels por feature_id e por classe. Para cada grupo, a função
    interna interpreta o class_id de acordo com o stat_type:
        - coverage           : class_id → class
        - deforestation_annual: class_id → transition (÷100) e class (mod 100)
        - transitions        : class_id → class_from (÷100) e class_to (mod 100)
        - water_annual       : class_id → class
        - water_monthly      : class_id → class

    Args:
        stat_type (str): Tipo de estatística que define como decodificar o class_id.

    Returns:
        callable: Função que recebe um objeto do redutor e retorna um ee.FeatureCollection.
    """
    def inner(obj):
        obj = ee.Dictionary(obj)
        territory = ee.Number(obj.get('feature_id')).int64()  # Precisa ser integer
        groups = ee.List(obj.get('groups'))

        def format_feature(class_area):
            d = ee.Dictionary(class_area)
            class_id = ee.Number(d.get('class'))
            feature = ee.Feature(None).set('feature_id', territory)
            area = d.get('sum')
            if stat_type == 'coverage':
                return feature.set({'class': class_id, 'area': area})
            elif stat_type == 'deforestation_annual':
                return feature.set({
                    'transition': class_id.divide(100).int(),
                    'class': class_id.mod(100).int(),
                    'area': area
                })
            elif stat_type in ('water_annual', 'water_monthly'):
                return feature.set({'class': class_id, 'area': area})
            else:
                return feature.set({
                    'class_from': class_id.divide(100).int16(),
                    'class_to': class_id.mod(100).int16(),
                    'area': area
                })
        return ee.FeatureCollection(groups.map(format_feature))
    return inner


def calculate_area(image, territory, geometry, stat_type):
    """
    Calcula a área (km²) por classe e por território usando redutor agrupado do GEE.

    Empilha a imagem de área de pixel, a imagem de território e a imagem classificada,
    depois aplica um redutor sum agrupado por feature_id e por classe. O resultado é
    convertido em FeatureCollection via build_area_features.

    Args:
        image (ee.Image): Imagem classificada com os valores de classe.
        territory (ee.Image): Imagem com os IDs dos territórios (feature_id).
        geometry (ee.Geometry): Geometria de recorte para o redutor.
        stat_type (str): Tipo de estatística, repassado para build_area_features.

    Returns:
        ee.FeatureCollection: Features com feature_id, classe(s) e área em km².
    """
    # Ordem das bandas é acoplada aos índices do redutor:
    #   banda 0 → PIXEL_AREA  (valor reduzido por sum)
    #   banda 1 → territory   (agrupado como 'feature_id' pelo segundo .group)
    #   banda 2 → image       (agrupado como 'class' pelo primeiro .group)
    # O GEE renumera as bandas de agrupamento internamente a cada .group(),
    # por isso ambos os .group() usam índice 1.
    reducer = ee.Reducer.sum().group(1, 'class').group(1, 'feature_id')

    areas = ee.Dictionary(
        PIXEL_AREA.addBands(territory).addBands(image)
            .reduceRegion(
                reducer=reducer, 
                geometry=geometry, 
                scale=30, 
                maxPixels=1e12
        )
    )
    return ee.FeatureCollection(ee.List(areas.get('groups')).map(build_area_features(stat_type))).flatten()


def export_statistics(areas, name, table, stat_type):
    """
    Submete uma task de exportação do FeatureCollection para o BigQuery via GEE.

    Define os seletores de colunas de acordo com o stat_type e inicia a task em modo
    append (sem sobrescrever dados existentes). O nome da task é truncado em 99 caracteres
    para respeitar o limite da API do GEE.

    Args:
        areas (ee.FeatureCollection): Features com as estatísticas calculadas.
        name (str): Descrição/nome da task de exportação.
        table (str): Tabela de destino no BigQuery.
        stat_type (str): Tipo de estatística, define quais colunas são exportadas.

    Returns:
        ee.batch.Task: Task submetida ao GEE (já iniciada).
    """
    selectors = [
        'feature_id', 'category_id', 'year',
        'class', 'area', 'collection', 'version', 'category']

    if stat_type == 'deforestation_annual':

        selectors = [
            'feature_id', 'category_id', 'year',
            'transition', 'class', 'area', 'collection',
            'version', 'category']

    elif stat_type == 'water_annual':

        selectors = [
            'feature_id', 'category_id', 'year',
            'class', 'area', 'collection', 'version', 'category']

    elif stat_type == 'water_monthly':

        selectors = [
            'feature_id', 'category_id', 'year', 'month',
            'class', 'area', 'collection', 'version', 'category']

    elif stat_type == 'transitions':

        selectors = [
            'feature_id', 'category_id', 'year_from',
            'year_to', 'class_from', 'class_to',
            'area', 'collection', 'version', 'category']

    task = ee.batch.Export.table.toBigQuery(
        collection=areas,
        description=name[:99],
        table=table,
        overwrite=False,
        append=True,
        selectors=selectors
    )

    task.start()

    return task

# ---------------------------
# Pipeline genérico de exportação
# ---------------------------

def run_export(stat_type, table, periods, get_image_fn, get_name_fn, get_metadata_fn, is_existing_fn, categories=None, version=None):
    """
    Executa o pipeline genérico de exportação de estatísticas para o BigQuery via GEE.

    Itera sobre todas as categorias e períodos, pulando combinações já existentes
    no BigQuery. Os comportamentos específicos de cada tipo de estatística
    (imagem, nome, metadata, checagem de existência) são injetados via callables.

    Args:
        stat_type (str): Tipo de estatística ('coverage', 'deforestation_annual', 'transitions', 'water_annual', 'water_monthly').
        table (str): Tabela de destino no BigQuery.
        periods (list): Lista de períodos (anos inteiros ou tuplas (ano_from, ano_to)).
        get_image_fn (callable): Recebe um período e retorna um ee.Image.
        get_name_fn (callable): Recebe (category, period) e retorna o nome da task.
        get_metadata_fn (callable): Recebe (category, period) e retorna dict de metadata.
        is_existing_fn (callable): Recebe (period, existing) e retorna bool.
        categories (list): Lista de categorias a iterar; se None, usa CATEGORIES global.
        version (str): Versão para checagem de existência. Se None, usa STATISTICS_VERSION.

    Returns:
        list: Lista de tasks de exportação submetidas ao GEE.
    """
    tasks = []
    cats = categories if categories is not None else CATEGORIES

    # Determina a versão a usar para check_existing_entries
    check_version = version if version is not None else STATISTICS_VERSION
    # Para water stats, usa a collection (não a versão completa)
    check_collection = WATER_COLLECTION if stat_type in ('water_annual', 'water_monthly') else f"{COLLECTION}.{SUB_COLLECTION}"

    for category in cats:

        # Verifica quais períodos já existem no BigQuery para essa categoria
        existing = check_existing_entries(
            category["CATEGORY"], table,
            check_collection, check_version, stat_type)

        print(f"\n📍 Processando categoria: {category['CATEGORY']} (ID={category['CATEG_ID']})")

        # Carrega os territórios da categoria
        # Nota: Filtro de version removido pois propriedades customizadas estão dentro de properties{}
        # e não são diretamente filtráveis. Filtramos apenas por CATEG_ID.
        territories = (
            ee.ImageCollection(ASSET_THEMES)
                .filter(ee.Filter.eq("CATEG_ID", int(category["CATEG_ID"])))
                .filter(ee.Filter.eq("version", THEMES_VERSION))
        )

        territories_count = territories.size().getInfo()
        print(f"   ✅ Territórios encontrados: {territories_count}")

        if territories_count == 0:
            print(f"   ⚠️  AVISO: Nenhum território encontrado para {category['CATEGORY']}")
            print(f"      Verificar: ASSET_THEMES={ASSET_THEMES}")
            print(f"      Filtros: version={THEMES_VERSION} (str), CATEG_ID={category['CATEG_ID']} (int)")
            continue

        for period in periods:

            if is_existing_fn(period, existing):
                continue

            image    = get_image_fn(period)
            name     = get_name_fn(category, period)
            metadata = get_metadata_fn(category, period)

            # DEBUG: Verificar se a imagem foi carregada
            try:
                img_info = image.getInfo()
                if img_info is None:
                    print(f"   ⚠️  Período {period}: Imagem retornou None")
                    continue
            except Exception as e:
                print(f"   ⚠️  Período {period}: Erro ao verificar imagem - {str(e)[:50]}")
                continue

            # Calcula áreas e anexa metadata
            areas = (
                territories
                    .map(lambda t: calculate_area(image, t, t.geometry().bounds(), stat_type))
                    .flatten()
                    .map(lambda f: f.set(metadata))
            )

            print(f"   📊 Exportando: {name}")

            tasks.append(export_statistics(areas, name, table, stat_type))

    return tasks


# ---------------------------
# Funções principais de exportação
# ---------------------------

def export_coverage_statistics():
    """
    Exporta estatísticas de cobertura do solo para o BigQuery.

    Calcula a área por classe de cobertura para cada categoria e ano em
    CATEGORIES × YEARS_COVERAGE, usando o asset de cobertura como fonte.

    Callables:
        get_image_fn  : seleciona a banda 'classification_{year}' do asset de cobertura.
        get_name_fn   : gera nome no formato 'collection-{C}.{SC}-{region}-coverage-{CATEGORY}-{year}-{version}'.
        get_metadata_fn: anexa collection, category, category_id, version e year a cada feature.
        is_existing_fn: pula o ano se já houver registros para essa categoria/versão no BigQuery.
    """
    img = ee.Image(ASSET_COVERAGE)

    return run_export(
        stat_type="coverage",
        table=TABLE_COVERAGE,
        periods=YEARS_COVERAGE,
        get_image_fn=lambda year: img.select(f"classification_{year}"),
        get_name_fn=lambda cat, year: (
            f"collection-{COLLECTION}.{SUB_COLLECTION}-{REGION}-coverage-{cat['CATEGORY']}-{year}-{STATISTICS_VERSION}"
        ),
        get_metadata_fn=lambda cat, year: {
            'collection': f"{COLLECTION}.{SUB_COLLECTION}",
            'category': cat["CATEGORY"],
            'category_id': int(cat["CATEG_ID"]),
            'version': STATISTICS_VERSION,
            'year': year,
        },
        is_existing_fn=lambda year, existing: year in existing,
    )


def export_deforestation_statistics():
    """
    Exporta estatísticas anuais de desmatamento e vegetação secundária para o BigQuery.

    Calcula a área por classe de transição para cada categoria e ano em
    CATEGORIES × YEARS_DEFORESTATION, usando o mosaico do asset de desmatamento filtrado
    pela INPUT_VERSION.

    Callables:
        get_image_fn  : seleciona a banda 'classification_{year}' do mosaico de desmatamento.
        get_name_fn   : gera nome no formato 'collection-{C}.{SC}-{region}-deforestation.annual-{CATEGORY}-{year}-{version}'.
        get_metadata_fn: anexa collection, category, category_id, version e year a cada feature.
        is_existing_fn: pula o ano se já houver registros para essa categoria/versão no BigQuery.
    """
    img = (
        ee.ImageCollection(ASSET_DEFORESTATION)
            .filter(ee.Filter.eq("version", INPUT_VERSION))
            .mosaic()
    )

    return run_export(
        stat_type="deforestation_annual",
        table=TABLE_DEFORESTATION,
        periods=YEARS_DEFORESTATION,
        get_image_fn=lambda year: img.select(f"classification_{year}"),
        get_name_fn=lambda cat, year: (
            f"collection-{COLLECTION}.{SUB_COLLECTION}-{REGION}-deforestation.annual-{cat['CATEGORY']}-{year}-{STATISTICS_VERSION}"
        ),
        get_metadata_fn=lambda cat, year: {
            'collection': f"{COLLECTION}.{SUB_COLLECTION}",
            'category': cat["CATEGORY"],
            'category_id': int(cat["CATEG_ID"]),
            'version': STATISTICS_VERSION,
            'year': year,
        },
        is_existing_fn=lambda year, existing: year in existing,
    )


def export_transition_statistics():
    """
    Exporta estatísticas de transição entre classes de cobertura para o BigQuery.

    Calcula a área por par de classes (class_from → class_to) para cada categoria e
    período em CATEGORIES × PERIODS_TRANSITIONS. Diferente de coverage e deforestation,
    cada período possui seu próprio asset de transição no formato
    'mapbiomas_{region}_{year_from}_{year_to}_{INPUT_VERSION}'.

    Callables:
        get_image_fn  : carrega o asset de transição correspondente ao período (year_from, year_to).
        get_name_fn   : gera nome no formato 'collection-{C}-{region}-transitions-{CATEGORY}-{y0}.{y1}-{version}'.
        get_metadata_fn: anexa collection, category, category_id, version, year_from e year_to a cada feature.
        is_existing_fn: pula o período se já houver registros para essa categoria/versão no BigQuery.
    """
    return run_export(
        stat_type="transitions",
        table=TABLE_TRANSITIONS,
        periods=PERIODS_TRANSITIONS,
        get_image_fn=lambda period: ee.Image(
            f"{ASSET_TRANSITIONS}/mapbiomas_{REGION}_{period[0]}_{period[1]}_{INPUT_VERSION}"
        ),
        get_name_fn=lambda cat, period: (
            f"collection-{COLLECTION}-{REGION}-transitions-{cat['CATEGORY']}-{period[0]}.{period[1]}-{STATISTICS_VERSION}"
        ),
        get_metadata_fn=lambda cat, period: {
            'collection': f"{COLLECTION}.{SUB_COLLECTION}",
            'category': cat["CATEGORY"],
            'category_id': int(cat["CATEG_ID"]),
            'version': STATISTICS_VERSION,
            'year_from': period[0],
            'year_to': period[1],
        },
        is_existing_fn=lambda period, existing: {'year_from': period[0], 'year_to': period[1]} in existing,
    )


def export_water_annual_statistics():
    """
    Exporta estatísticas anuais de água superficial para o BigQuery.

    Calcula a área por classe de água para cada categoria e ano em
    WATER_CATEGORIES × YEARS_WATER, usando o asset de água anual.

    Callables:
        get_image_fn  : seleciona a banda 'classification_{year}' do asset de água.
        get_name_fn   : gera nome no formato 'collection-{WATER_COLLECTION}-{region}-water.annual-{CATEGORY}-{year}-{version}'.
        get_metadata_fn: anexa collection, category, category_id, version e year a cada feature.
        is_existing_fn: pula o ano se já houver registros para essa categoria/versão no BigQuery.
    """
    water_image = ee.Image(ASSET_WATER_ANNUAL)

    return run_export(
        stat_type="water_annual",
        table=TABLE_WATER_ANNUAL,
        periods=YEARS_WATER,
        get_image_fn=lambda year: water_image.select(f"classification_{year}"),
        get_name_fn=lambda cat, year: (
            f"collection-{WATER_COLLECTION}-{REGION}-water.annual-{cat['CATEGORY']}-{year}-{STATISTICS_VERSION_WATER}"
        ),
        get_metadata_fn=lambda cat, year: {
            'collection': WATER_COLLECTION,
            'category': cat["CATEGORY"],
            'category_id': int(cat["CATEG_ID"]),
            'version': STATISTICS_VERSION_WATER,
            'year': year,
        },
        is_existing_fn=lambda year, existing: year in existing,
        categories=WATER_CATEGORIES,
        version=STATISTICS_VERSION_WATER,
    )


def export_water_monthly_statistics():
    """
    Exporta estatísticas mensais de água superficial para o BigQuery.

    Calcula a área por classe de água para cada categoria, ano e mês em
    WATER_CATEGORIES × YEARS_WATER × MONTHS, usando a coleção de água mensal.

    Callables:
        get_image_fn  : filtra a coleção de água mensal por ano e mês (zero-padded).
        get_name_fn   : gera nome no formato 'collection-{WATER_COLLECTION}-{region}-water.monthly-{CATEGORY}-{year}-{month}-{version}'.
        get_metadata_fn: anexa collection, category, category_id, version, year e month a cada feature.
        is_existing_fn: pula o par (year, month) se já houver registros para essa categoria/versão no BigQuery.
    """
    water_monthly = ee.ImageCollection(ASSET_WATER_MONTHLY)

    def get_periods():
        return [{'year': y, 'month': m}
                for y in YEARS_WATER
                for m in MONTHS]

    def get_image(period):
        return (water_monthly
                .filter(ee.Filter.eq('year', period['year']))
                .filter(ee.Filter.eq('month', str(period['month']).zfill(2)))
                .first())

    def get_metadata(cat, period):
        return {
            'collection': WATER_COLLECTION,
            'category': cat["CATEGORY"],
            'category_id': int(cat["CATEG_ID"]),
            'version': STATISTICS_VERSION_WATER,
            'year': period['year'],
            'month': str(period['month']).zfill(2),
        }

    def is_existing(period, existing):
        return {'year': period['year'], 'month': str(period['month']).zfill(2)} in existing

    return run_export(
        stat_type="water_monthly",
        table=TABLE_WATER_MONTHLY,
        periods=get_periods(),
        get_image_fn=get_image,
        get_name_fn=lambda cat, period: (
            f"collection-{WATER_COLLECTION}-{REGION}-water.monthly-{cat['CATEGORY']}-{period['year']}-{str(period['month']).zfill(2)}-{STATISTICS_VERSION_WATER}"
        ),
        get_metadata_fn=get_metadata,
        is_existing_fn=is_existing,
        categories=WATER_CATEGORIES,
        version=STATISTICS_VERSION_WATER,
    )

# ---------------------------
# Execução do pipeline
# ---------------------------
if __name__ == "__main__":

    if RUN_EXPORT_COVERAGE:
        coverage_tasks = export_coverage_statistics()
        wait_until_tasks_finish(export_tasks=coverage_tasks, polling_interval=60)

    if RUN_EXPORT_DEFORESTATION:
        deforestation_tasks = export_deforestation_statistics()
        wait_until_tasks_finish(export_tasks=deforestation_tasks, polling_interval=60)

    if RUN_EXPORT_TRANSITIONS:
        transitions_tasks = export_transition_statistics()
        wait_until_tasks_finish(export_tasks=transitions_tasks, polling_interval=60)

    if RUN_EXPORT_WATER_ANNUAL:
        water_annual_tasks = export_water_annual_statistics()
        wait_until_tasks_finish(export_tasks=water_annual_tasks, polling_interval=60)

    if RUN_EXPORT_WATER_MONTHLY:
        water_monthly_tasks = export_water_monthly_statistics()
        wait_until_tasks_finish(export_tasks=water_monthly_tasks, polling_interval=60)
