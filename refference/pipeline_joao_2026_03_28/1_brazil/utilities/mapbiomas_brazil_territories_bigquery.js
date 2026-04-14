/**
 * Exporta coleções para BigQuery usando apenas o nome da pasta como categoria
 */
var asset_folder = 'projects/mapbiomas-territories/assets/TERRITORIES/BRAZIL/WORKSPACE';

var assets = [
    { "CATEGORY": "POLITICAL_LEVEL_1", "CATEG_ID": "1" },
    { "CATEGORY": "POLITICAL_LEVEL_2", "CATEG_ID": "2" },
    { "CATEGORY": "POLITICAL_LEVEL_3", "CATEG_ID": "3" },
    { "CATEGORY": "BIOMES", "CATEG_ID": "4" },
    { "CATEGORY": "BASIN_LEVEL_1_PNRH", "CATEG_ID": "10" },
    { "CATEGORY": "BASIN_LEVEL_2_PNRH", "CATEG_ID": "11" },
    { "CATEGORY": "COASTAL_MARINE_ZONE", "CATEG_ID": "5" },
    { "CATEGORY": "BASIN_LEVEL_1_DNAEE", "CATEG_ID": "12" },
    { "CATEGORY": "BASIN_LEVEL_2_DNAEE", "CATEG_ID": "13" },
    { "CATEGORY": "UGRHS", "CATEG_ID": "14" },
    { "CATEGORY": "DHN250_LEVEL_1", "CATEG_ID": "15" },
    { "CATEGORY": "DHN250_LEVEL_2", "CATEG_ID": "16" },
    { "CATEGORY": "DHN250_LEVEL_3", "CATEG_ID": "17" },
    { "CATEGORY": "AMACRO", "CATEG_ID": "20" },
    { "CATEGORY": "ATLANTIC_FOREST_LAW", "CATEG_ID": "21" },
    { "CATEGORY": "LEGAL_AMAZON", "CATEG_ID": "22" },
    { "CATEGORY": "SEMIARID", "CATEG_ID": "23" },
    { "CATEGORY": "MATOPIBA", "CATEG_ID": "24" },
    { "CATEGORY": "FLORESTAS_PUBLICAS_NAO_DESTINADAS", "CATEG_ID": "25" },
    { "CATEGORY": "AREAS_PRIORITARIAS_DO_MMA_2018", "CATEG_ID": "40" },
    { "CATEGORY": "GEOPARQUES", "CATEG_ID": "41" },
    { "CATEGORY": "RESERVA_DA_BIOSFERA", "CATEG_ID": "42" },
    { "CATEGORY": "PROTECTED_AREA", "CATEG_ID": "60" },
    { "CATEGORY": "INDIGENOUS_TERRITORIES", "CATEG_ID": "61" },
    { "CATEGORY": "QUILOMBOS", "CATEG_ID": "62" },
    { "CATEGORY": "SETTLEMENTS", "CATEG_ID": "63" },
    { "CATEGORY": "CONCESSOES_FLORESTAIS", "CATEG_ID": "71" },
    { "CATEGORY": "METROPOLITAN_REGIONS", "CATEG_ID": "91" },
    { "CATEGORY": "POPULATION_ARRANGEMENT", "CATEG_ID": "92" },
    { "CATEGORY": "URBAN_CONCENTRATION", "CATEG_ID": "93" },
    { "CATEGORY": "URBAN_PERIMETER", "CATEG_ID": "94" },
];

assets.forEach(function (asset) {

    // pega apenas a pasta (primeira parte antes da "/")
    var category = asset.split('/')[0];

    var collection = ee.FeatureCollection(asset_folder + '/' + asset)
        .map(function (feature) {
            return feature
                .set('GEOCODE', ee.String(feature.get('GEOCODE')))
                .set('CATEGORY', category); // só a pasta
        });

    print(category, collection.size());
    print(collection.limit(1));

    Export.table.toBigQuery({
        collection: collection,
        description: asset.replace(/\//g, '_'),
        table: "mapbiomas.mapbiomas_brazil_statistics.territories_c10_1",
        overwrite: false,
        append: true,
        selectors: [
            "CATEGORY",
            "CATEG_ID",
            "FEATURE_ID",
            "GEOCODE",
            "LEVEL_1",
            "LEVEL_2",
            "LEVEL_3",
            "LEVEL_4",
            "NAME",
            "NAME_STD",
            "SOURCE",
            "VERSION",
        ]
    });
});
