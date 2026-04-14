import ee

ee.Initialize(project='mapbiomas-colombia')

cadence = 'annual'

# Série temporal da água: 2000–2023
water_years = list(range(2000, 2024))

# Série temporal dos glaciais: 1985–2023
glacier_years = list(range(1985, 2024))

# -------------------------
# Série temporal: ÁGUA (RAISG + Brasil)
# -------------------------

amazonCountriesCollection = (
    ee.ImageCollection('projects/mapbiomas-raisg/MAPBIOMAS-WATER/COLECCION2/water-integracion-01')
    .filter(ee.Filter.eq('version', 11))
    .filter(ee.Filter.eq('cadence', cadence))
)

brazilAssetCollection = (
    ee.ImageCollection('projects/nexgenmap/TRANSVERSAIS/AGUA5-FT')
    .filter(ee.Filter.eq('version', '11'))
    .filter(ee.Filter.eq('cadence', cadence))
    .filter(ee.Filter.stringEndsWith('system:index', 'validation').Not())
)

waterRaisg = amazonCountriesCollection.merge(brazilAssetCollection)

print('Numero de images', waterRaisg.size().getInfo())

image_list = ee.List(water_years).iterate(
    lambda year, img: ee.Image(img).addBands(
        waterRaisg
            .filter(ee.Filter.eq('year', ee.Number(year)))
            .mosaic()
            .multiply(33)
            .rename(
                ee.String('water_').cat(
                    ee.Number(year).format('%d')
                )
            )
    ),
    ee.Image().select()  # imagem vazia inicial
)

water = ee.Image(image_list)

# -------------------------
# Série temporal: CORPOS D’ÁGUA (CLASSIFICAÇÃO)
# -------------------------

amazonCountriesCollection_class = (
    ee.ImageCollection('projects/mapbiomas-raisg/MAPBIOMAS-WATER/COLECCION2/water-classification-integracion')
    .filter(ee.Filter.eq('version', '3'))
)

brazilCollection_class = (
    ee.ImageCollection('projects/mapbiomas-workspace/AMOSTRAS/GTAGUA/OBJETOS/CLASSIFICADOS/TESTE_1_raster')
    .filter(ee.Filter.eq('version', '5'))
)

waterBodiesRaisg = amazonCountriesCollection_class.merge(brazilCollection_class)

print(waterBodiesRaisg.size().getInfo())

water_bodies_list = ee.List(water_years).iterate(
    lambda year, img: ee.Image(img).addBands(
        waterBodiesRaisg
            .filter(ee.Filter.eq('year', ee.Number(year)))
            .mosaic()
            .rename(
                ee.String('water_bodies_').cat(
                    ee.Number(year).format('%d')
                )
            )
    ),
    ee.Image().select()  # imagem vazia inicial
)

waterBodies = ee.Image(water_bodies_list)

# -------------------------
# Série temporal: GLACIAIS
# -------------------------

glaciersCollection = (
    ee.ImageCollection('projects/mapbiomas-raisg/MAPBIOMAS-WATER/COLECCION2/glacier-integracion-01')
    .filter(ee.Filter.eq('version', '1'))
)
print(glaciersCollection.size().getInfo())
glaciers_list = ee.List(glacier_years).iterate(
    lambda year, img: ee.Image(img).addBands(
        glaciersCollection
            .filter(ee.Filter.eq('year', ee.Number(year)))
            .mosaic()
            .multiply(34)
            .rename(
                ee.String('glacier_').cat(
                    ee.Number(year).format('%d')
                )
            )
    ),
    ee.Image().select()  # imagem vazia inicial
)

glaciers = ee.Image(glaciers_list)

# -------------------------------------------
# ÁGUA MENSAL (RAISG + Brasil) - 2000–2023
# -------------------------------------------

cadence = 'monthly'

amazonCountriesCollection_monthly = (
    ee.ImageCollection('projects/mapbiomas-raisg/MAPBIOMAS-WATER/COLECCION2/water-integracion-01')
    .filter(ee.Filter.eq('version', 11))
    .filter(ee.Filter.eq('cadence', cadence))
)

brazilAssetCollection_monthly = (
    ee.ImageCollection('projects/nexgenmap/TRANSVERSAIS/AGUA5-FT')
    .filter(ee.Filter.eq('version', '11'))
    .filter(ee.Filter.eq('cadence', cadence))
    .filter(ee.Filter.stringEndsWith('system:index', 'validation').Not())
)

waterMonthly = amazonCountriesCollection_monthly.merge(brazilAssetCollection_monthly)

print('Número de imagens (monthly):', waterMonthly.size().getInfo())

years_monthly = ee.List(water_years)
months = ee.List.sequence(1, 12)

def map_year(year):
    year = ee.Number(year)
    # mosaic de todas as imagens daquele ano (com bandas classification_1 ... classification_12)
    water_year = waterMonthly.filter(ee.Filter.eq('year', year)).mosaic()

    def map_month(month):
        month = ee.Number(month)
        water_month = water_year.select(
            [ee.String('classification_').cat(month.format('%d'))],
            ['water_monthly']
        )
        return (water_month
                .gt(0)
                .multiply(month)
                .set('year', year)
                .set('month', month.format('%02d'))
                .set('band_format', 'water_monthly')
                .set('version', 'v1')
                .set('data_type', 'monthly'))

    return months.map(map_month)

image_list = years_monthly.map(map_year)
image_list_flat = ee.List(image_list).flatten()

water_monthly_adjusted = ee.ImageCollection.fromImages(image_list_flat)

print('Número de imagens geradas (year x month):', water_monthly_adjusted.size().getInfo())

# -------------------------
# Geometria de exportação
# -------------------------

geometry = ee.Geometry.Polygon(
    [[
        [-82.46501009901786, 13.700752870792344],
        [-82.46501009901786, -35.00183056075943],
        [-33.59782259901786, -35.00183056075943],
        [-33.59782259901786, 13.700752870792344]
    ]],
    proj=None,
    geodesic=False
)

# -------------------------
# EXPORTAÇÕES PARA ASSET
# -------------------------

water = (
    water
        .set('band_format', 'water_{year}')
        .set('version', 'v1')
        .set('data_type', 'annual')
)

task_water = ee.batch.Export.image.toAsset(
    image=water.byte(),
    description='mapbiomas_amazon_collection2_water_v1',
    assetId='projects/mapbiomas-public/assets/amazon/water/collection2/mapbiomas_amazon_collection2_water_v1',
    region=geometry,
    scale=30,
    maxPixels=1e13,
    pyramidingPolicy={'.default': 'mode'}
)
# task_water.start()

waterBodies = (
    waterBodies
        .set('band_format', 'water_bodies_{year}')
        .set('version', 'v1')
        .set('data_type', 'annual')
)

task_water_bodies = ee.batch.Export.image.toAsset(
    image=waterBodies.byte(),
    description='mapbiomas_amazon_collection2_water_bodies_v1',
    assetId='projects/mapbiomas-public/assets/amazon/water/collection2/mapbiomas_amazon_collection2_water_bodies_v1',
    region=geometry,
    scale=30,
    maxPixels=1e13,
    pyramidingPolicy={'.default': 'mode'}
)
# task_water_bodies.start()

glaciers = (
    glaciers
        .set('band_format', 'glacier_{year}')
        .set('version', 'v2')
        .set('data_type', 'annual')
)

task_glaciers = ee.batch.Export.image.toAsset(
    image=glaciers.byte(),
    description='mapbiomas_amazon_collection2_glacier_v1',
    assetId='projects/mapbiomas-public/assets/amazon/water/collection2/mapbiomas_amazon_collection2_glacier_v2',
    region=geometry,
    scale=30,
    maxPixels=1e13,
    pyramidingPolicy={'.default': 'mode'}
)
# task_glaciers.start()

print("Export tasks started! Verifique no Task Manager do Earth Engine.")

# -------------------------------------------
# EXPORT POR ANO/MÊS (WATER MONTHLY)
# -------------------------------------------

years_monthly = ee.List(water_years)  # usa a mesma lista do anual
months = range(1, 13)  # 1 a 12

for year in water_years:
    for month in months:
        month_str = f"{month:02d}"  # sempre '01'...'12'

        try:
            img = (
                water_monthly_adjusted
                    .filter(ee.Filter.eq('year', year))
                    .filter(ee.Filter.eq('month', month_str))
                    .first()
            )

            description = f'mapbiomas_amazon_collection2_water_monthly_{year}_{month_str}'

            asset_id = f'projects/mapbiomas-public/assets/amazon/water/collection2/mapbiomas_amazon_collection2_water_monthly_v1/water_monthly_{year}_{month_str}'
    

            task = ee.batch.Export.image.toAsset(
                image=img.byte(),
                description=description,
                assetId=asset_id,
                region=geometry,
                scale=30,
                maxPixels=1e13,
                pyramidingPolicy={'.default': 'mode'}
            )

            task.start()
            print("Started task:", description)

        except Exception as e:
            # Não interrompe o loop, apenas registra o problema
            print(f"Erro ao iniciar export {year}-{month_str} →", e)
            continue
