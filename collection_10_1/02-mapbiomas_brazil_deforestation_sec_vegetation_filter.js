/*

  Conversão para pixels de 30 m (cada pixel = 900 m² = 0,09 ha):
  1 ha ≈ 11 pixels → 0,99 ha
  2 ha ≈ 22 pixels → 1,98 ha
  3 ha ≈ 33 pixels → 2,97 ha

*/


// --- ---- ---- inputs
var asset = 'projects/ee-ipam/assets/MAPBIOMAS/LULC/DEFORESTATION/col10_1_def_sec_vegetation';
var coll = ee.ImageCollection(asset);
var no_filter = coll.filter(ee.Filter.eq('version', 'v00')).mosaic();

print('coll',coll);
print('no_filter',no_filter);

// --- auxiliar
var asset_lulc = 'projects/mapbiomas-public/assets/brazil/lulc/collection10_1/mapbiomas_brazil_collection10_1_coverage_v1';
var lulc = ee.Image(asset_lulc);

var palette = require('users/mapbiomas/modules:Palettes.js').get('brazil');


// --- - plot 1
var vis_def_sec_veg = {
  min: 0,
  max: 7,
  format: 'png',
  bands: 'classification_2020',
  palette: [
    '#ffffff', // [0] No data
    '#faf5d1', // [1] Antrópico
    '#3f7849', // [2] Veg. Primária
    '#5bcf20', // [3] Veg. Secundária
    '#ea1c1c', // [4] Supressão Veg. Primária
    '#b4f792', // [5] Recuperação para Veg. Secundária
    '#fe9934', // [6] Supressão Veg. Secundária
    '#303149', // [7] Outras transições
  ]
};

Map.addLayer(lulc,{min:0,max:75,bands:['classification_2024'],palette:palette},'lulc');
Map.addLayer(no_filter,vis_def_sec_veg,'no_filter');

// --- --- mascaras da série temporal
// - mascara binaria do que foi desmatado na série
var deforestation_mask = no_filter.multiply(0)
  .where(no_filter.eq(4),1)
  .where(no_filter.eq(6),1);

// - mascara binaria do que foi vegetação secundária na série
var secondary_mask = no_filter.multiply(0)
  .where(no_filter.eq(3),1)
  .where(no_filter.eq(5),1);

// --- --- contando número de pixels
// - contando o número de pixels nas manchas de desmatamento observados na série
var maxSize = 100;
var accumulated_deforestation_count  = deforestation_mask
  .reduce(ee.Reducer.max())
  .selfMask()
  .connectedPixelCount({
    maxSize: maxSize,
    eightConnected: true
  });

var annual_deforestation_count  = deforestation_mask
  .selfMask()
  .connectedPixelCount({
    maxSize: maxSize,
    eightConnected: true
  });

// - contando o número de pixels nas manchas de vegetação secundária observados no ultimo ano da série
var accumulated_secondary_count = secondary_mask
  .reduce(ee.Reducer.max())
  .selfMask()
  .connectedPixelCount({
    maxSize: maxSize,
    eightConnected: true
  });
  
print('accumulated_secondary_count',accumulated_secondary_count);

var annual_secondary_count = secondary_mask
  .selfMask()
  .connectedPixelCount({
    maxSize: maxSize,
    eightConnected: true
  });

// --- - plot 2
Map.addLayer(deforestation_mask.reduce(ee.Reducer.max()),{min:0,max:1},'deforestation_mask',false);
Map.addLayer(secondary_mask.reduce(ee.Reducer.max()),{min:0,max:1},'secondary_mask',false);

Map.addLayer(accumulated_deforestation_count.reduce(ee.Reducer.max()),{min:0,max:34},'accumulated_deforestation_count !zoom depend',false);
Map.addLayer(annual_deforestation_count.reduce(ee.Reducer.max()),{min:0,max:34},'annual_deforestation_count !zoom depend',false);
Map.addLayer(accumulated_secondary_count.reduce(ee.Reducer.max()),{min:0,max:34},'accumulated_secondary_count !zoom depend',false);
Map.addLayer(annual_secondary_count.reduce(ee.Reducer.max()),{min:0,max:34},'annual_secondary_count !zoom depend',false);


// --- --- matriz de versões para exportar
var matrix_versions = [
  {
    name:'v01',
    description:'v01 - Baseado nas Coleções 6 a 8: Remove manchas totais da série <1 ha em todas as classes', 
    annual_deforestation_size_filter: 11, 
    annual_secondary_size_filter: 11
    
  },
  
  {
    name:'v02',
    description:'v02 - Baseado na Coleção 9: Remove manchas anuais de desmatamento <1 ha, manchas de veg. secundária acumuladas na série <2 ha na série e <2 ha no último ano em todas as classes (ATBD Col 09: regra Amazônia)', 
    annual_deforestation_size_filter: 11, 
    accumulated_secondary_size_filter: 22,
    last_year_size_filter: 22
  },
  
  {
    name:'v03',
    description:'v03 - Baseado na Coleção 9: Remove manchas anuais de desmatamento <1 ha, manchas de veg. secundária  acumuladas na série <1 ha na série e <1 ha no último ano em todas as classes (ATBD Col 09: regra Caatinga e Mata Atlântica)', 
    annual_deforestation_size_filter: 11, 
    accumulated_secondary_size_filter: 11,
    last_year_size_filter: 11
  },

  {
    name:'v04',
    description:'v04 - Baseado na Coleção 9: Remove manchas anuais de desmatamento <1 ha, manchas de veg. secundária acumuladas na série <1 ha na série e <3ha no último ano em todas as classes (ATBD Col 09: regra Pantanal)', 
    annual_deforestation_size_filter: 11, 
    accumulated_secondary_size_filter: 11,
    last_year_size_filter: 33
  },

  {
    name:'v05',
    description:'v05 - Baseado na Coleção 9: Remove manchas anuais de desmatamento <1 ha, manchas de veg. secundária acumuladas na série <3 ha na série e <3ha no último ano em todas as classes (ATBD Col 09: regra Cerrado e Pampa)', 
    annual_deforestation_size_filter: 11, 
    accumulated_secondary_size_filter: 33,
    last_year_size_filter: 33
  },

  {
    name:'v06',
    description:'v06 - Baseado na Coleção 10: Remove manchas acumuladas <3 ha e <3 ha no último ano em em todas as classes', 
    accumulated_deforestation_size_filter: 33,
    accumulated_secondary_size_filter: 33,
    last_year_size_filter: 33
  },

  {
    name:'v07',
    description:'v07 - Proposta Coleção 10.1: Remove manchas acumuladas <1 ha e <3 ha no último ano em em todas as classes', 
    accumulated_deforestation_size_filter: 11,
    accumulated_secondary_size_filter: 11,
    last_year_size_filter: 33
  },
  
];

matrix_versions.forEach(function(obj){
  
  var version_filter = no_filter;
  
  // 1. Filtro de Desmatamento Acumulado (Classes 4 e 6)
  if(obj.accumulated_deforestation_size_filter){
    var mask_acc_def = accumulated_deforestation_count.lte(obj.accumulated_deforestation_size_filter)
                          .and(version_filter.eq(4).or(version_filter.eq(6)));

    version_filter = version_filter.where(mask_acc_def, 8);
  }
  
  // 2. Filtro de Vegetação Secundária Acumulada (Classes 3 e 5)
  if(obj.accumulated_secondary_size_filter){
    var mask_acc_sec = accumulated_secondary_count.lte(obj.accumulated_secondary_size_filter)
                          .and(version_filter.eq(3).or(version_filter.eq(5)));
                          
    version_filter = version_filter.where(mask_acc_sec, 8);
  }    
  
  // 3. Filtro de Desmatamento Anual (Classes 4 e 6)
  if(obj.annual_deforestation_size_filter){
    var mask_ann_def = annual_deforestation_count.lte(obj.annual_deforestation_size_filter)
                          .and(version_filter.eq(4).or(version_filter.eq(6)));
                          
    version_filter = version_filter.where(mask_ann_def, 8);
  }

  // 4. Filtro de Vegetação Secundária Anual (Classes 3 e 5) - Corrigido as classes!
  if(obj.annual_secondary_size_filter){
    var mask_ann_sec = annual_secondary_count.lte(obj.annual_secondary_size_filter)
                          .and(version_filter.eq(3).or(version_filter.eq(5)));
                          
    version_filter = version_filter.where(mask_ann_sec, 8);
  }
  
  // 5. Filtro do último ano da série
  if(obj.last_year_size_filter){
    var last_year = version_filter.slice(-1);
    var remainder = version_filter.slice(0, -1); // Mantém o resto da série seguro
    
    // Pegamos as contagens específicas apenas da última banda
    var def_last_year_count = annual_deforestation_count.slice(-1);
    var sec_last_year_count = annual_secondary_count.slice(-1);

    // Filtra Desmatamento (4 e 6) no último ano
    var mask_def_last = def_last_year_count.lte(obj.last_year_size_filter)
                          .and(last_year.eq(4).or(last_year.eq(6)));
    last_year = last_year.where(mask_def_last, 8);

    // Filtra Vegetação Secundária (3 e 5) no último ano usando a contagem correta
    var mask_sec_last = sec_last_year_count.lte(obj.last_year_size_filter)
                          .and(last_year.eq(3).or(last_year.eq(5)));
    last_year = last_year.where(mask_sec_last, 8);
    
    // Junta novamente com a série
    version_filter = remainder.addBands(last_year);
  }
  
  
  // --- - plot 3
  Map.addLayer(version_filter, vis_def_sec_veg,  obj.name + ' version_filter', false);
  
  var properties = {
    version: obj.name,
    description: obj.description, 
    territory: "BRAZIL",
    collection_id: 10.1,
    source: "GT Desmatamento",
    theme: "Desmatamento",
    cadence:'3'
  };
  
  exportPerCarta(version_filter, asset, obj.name, properties);
  
});


function exportPerCarta (image,assetOutput,name,properties){
 
  properties = properties === undefined ? {} : properties;
 
  // --- --- --- Export
  
  var assetGrids = 'projects/mapbiomas-workspace/AUXILIAR/cartas';
  var grids = ee.FeatureCollection(assetGrids);
  
  var gridNames = [
      "NA-19", "NA-20", "NA-21", "NA-22", "NB-20", "NB-21", "NB-22", "SA-19",
      "SA-20", "SA-21", "SA-22", "SA-23", "SA-24", "SB-18", "SB-19", "SB-20",
      "SB-21", "SB-22", "SB-23", "SB-24", "SB-25", "SC-18", "SC-19", "SC-20",
      "SC-21", "SC-22", "SC-23", "SC-24", "SC-25", "SD-20", "SD-21", "SD-22",
      "SD-23", "SD-24", "SE-20", "SE-21", "SE-22", "SE-23", "SE-24", "SF-21",
      "SF-22", "SF-23", "SF-24", "SG-21", "SG-22", "SG-23", "SH-21", "SH-22",
      "SI-22"
  ];
  
  gridNames.forEach(
      function (gridName) {
          var grid = grids.filter(ee.Filter.stringContains('grid_name', gridName));
  
          Export.image.toAsset({
              image: image.set(properties),
              description:  'WSilva-' + 'def_sec_vegetation-'+ name + '-' + gridName,
              assetId: assetOutput + "/" + name + '-' + gridName,
              pyramidingPolicy: {
                  '.default': 'mode'
              },
              region: grid.geometry().bounds(),
              scale: 30,
              maxPixels: 1e13,
              overwrite:true
          });
      }
  );

  
}
