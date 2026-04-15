/*
  v00 - sem filtros
  v01 - filtro de 1 ha acumulado na série
  v02 - filtro de 3 ha acumulado na série
  v03 - filtro de 0.5 ha no ano e 1 ha acumulado na série
  v04 - filtro de 0.5 ha no ano e 3 ha acumulado na série

  v05 - filtro de 1 ha em cada ano da série
  v06 - filtro de 3 ha em cada ano da série

  v07 - filtro de 1ha no ultimo ano + filtro de 1 ha acumulado na série
  v08 - filtro de 1ha no ultimo ano + filtro de 3 ha acumulado na série
  v09 - filtro de 1ha no ultimo ano + filtro de 0.5 ha no ano e 1 ha acumulado na série
  v10 - filtro de 1ha no ultimo ano + filtro de 0.5 ha no ano e 3 ha acumulado na série
  
  Conversão para pixels de 30 m (cada pixel = 900 m² = 0,09 ha):
  1 ha ≈ 11,11 pixels → 12 pixels = 1,08 ha
  3 ha ≈ 33,33 pixels → 34 pixels = 3,06 ha
  0,5 ha ≈ 5,56 pixels → 6 pixels = 0,54 ha
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
print('accumulated_secondary_count',accumulated_secondary_count)
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
  // {
  //   name:'v01',
  //   description:'v01 - filtro de 1 ha (12 pixels) acumulado na série', 
  //   accumulated_deforestation_size_filter: 12, 
  //   accumulated_secondary_size_filter:12,
  // },
  // {
  //   name:'v02',
  //   description:'v02 - filtro de 3 ha (34 pixels) acumulado na série', 
  //   accumulated_deforestation_size_filter: 34, 
  //   accumulated_secondary_size_filter:34,
  // },
  // {
  //   name:'v03',
  //   description:'v03 - filtro de 0.5 ha (6 pixels) no ano e 1 ha (12 pixels) acumulado na série', 
  //   accumulated_deforestation_size_filter: 12, 
  //   accumulated_secondary_size_filter:12,
  //   annual_deforestation_size_filter: 6, 
  //   annual_secondary_size_filter:6
  // },
  // {
  //   name:'v04',
  //   description:'v04 - filtro de 0.5 ha (6 pixels) no ano e 3 ha (34 pixels) acumulado na série', 
  //   accumulated_deforestation_size_filter: 34, 
  //   accumulated_secondary_size_filter:34,
  //   annual_deforestation_size_filter: 6, 
  //   annual_secondary_size_filter:6
  // },
  {
    name:'v05',
    description:'v05 - filtro de 1 ha em cada ano da série',
    annual_deforestation_size_filter:12,
  },
  {
    name:'v06',
    description:'v06 - filtro de 3 ha em cada ano da série',
    annual_deforestation_size_filter:34,
  },
  {
    name:'v07',
    description:'v07 - filtro de 1ha no ultimo ano + filtro de 1 ha acumulado na série',
    last_year_size_filter:12,
    accumulated_deforestation_size_filter: 12, 
    accumulated_secondary_size_filter:12,
  },
  {
    name:'v08',
    description:'v08 - filtro de 1ha no ultimo ano + filtro de 3 ha acumulado na série',
    last_year_size_filter:12,
    accumulated_deforestation_size_filter: 34, 
    accumulated_secondary_size_filter: 34,
  },
  {
    name:'v09',
    description:'v09 - filtro de 1ha no ultimo ano + filtro de 0.5 ha no ano e 1 ha acumulado na série',
    last_year_size_filter:12,
    accumulated_deforestation_size_filter: 12, 
    accumulated_secondary_size_filter:12,
    annual_deforestation_size_filter: 6, 
    annual_secondary_size_filter:6
  },
  {
    name:'v10',
    description:'v10 - filtro de 1ha no ultimo ano + filtro de 0.5 ha no ano e 3 ha acumulado na série',
    last_year_size_filter:12,
    accumulated_deforestation_size_filter: 34, 
    accumulated_secondary_size_filter:34,
    annual_deforestation_size_filter: 6, 
    annual_secondary_size_filter:6
  },
];

matrix_versions.forEach(function(obj){
  
  var version_filter = no_filter;
  // print('n',obj.name)
  if(obj.last_year_size_filter){
    var last_year = version_filter.slice(-1)
    version_filter = version_filter.slice(0,-1);
    last_year = last_year.where(annual_deforestation_count.slice(-1).lte(obj.last_year_size_filter).and(last_year.eq(3).or(last_year.eq(5))), 7);
    last_year = last_year.where(annual_deforestation_count.slice(-1).lte(obj.last_year_size_filter).and(last_year.eq(4).or(last_year.eq(6))), 7);
    // print('last_year_size_filter',version_filter,last_year)
    
    version_filter = version_filter.addBands(last_year);
  }
  if(obj.accumulated_deforestation_size_filter){
    version_filter = version_filter.where(accumulated_deforestation_count.lte(obj.accumulated_deforestation_size_filter).and(version_filter.eq(4)), 7);
    version_filter = version_filter.where(accumulated_deforestation_count.lte(obj.accumulated_deforestation_size_filter).and(version_filter.eq(6)), 7);
    // print('accumulated_deforestation_size_filter',version_filter)
  }
  if(obj.accumulated_secondary_size_filter){
    version_filter = version_filter.where(accumulated_secondary_count.lte(obj.accumulated_secondary_size_filter),7);
    version_filter = version_filter.where(version_filter.eq(3), 7);
    version_filter = version_filter.where(accumulated_secondary_count.lte(obj.accumulated_secondary_size_filter),7);
    version_filter = version_filter.where(version_filter.eq(5), 7);
    // print('accumulated_secondary_size_filter',version_filter,'accumulated_secondary_count',accumulated_secondary_count)
  }    
  if(obj.annual_deforestation_size_filter){
    version_filter = version_filter.where(annual_deforestation_count.select(version_filter.bandNames()).lte(obj.annual_deforestation_size_filter),7);
    version_filter = version_filter.where(version_filter.eq(4), 7);
    version_filter = version_filter.where(annual_deforestation_count.select(version_filter.bandNames()).lte(obj.annual_deforestation_size_filter),7);
    version_filter = version_filter.where(version_filter.eq(6), 7);
    // print('annual_secondary_size_filter',version_filter)
  }

  if(obj.annual_secondary_size_filter){
    version_filter = version_filter.where(annual_secondary_count.lte(obj.annual_secondary_size_filter).and(version_filter.eq(4)), 7);
    version_filter = version_filter.where(annual_secondary_count.lte(obj.annual_secondary_size_filter).and(version_filter.eq(6)), 7);
    // print('last_year_size_filter',version_filter)
  }
  
  
  // --- - plot 3
  Map.addLayer(version_filter, vis_def_sec_veg,  obj.name + 'version_filter ');
  // Map.addLayer(accumulated_deforestation_count.lte(obj.accumulated_deforestation_size_filter).selfMask(), {min: 0,max: 1,format: 'png',palette: ['#000000','#0c003d']}, obj.name + "deforestation_count !zoom depend",false,0.4);
  // Map.addLayer(accumulated_secondary_mask_count.lte(obj.accumulated_secondary_size_filter).selfMask(), {min: 0,max: 1,format: 'png',palette: ['#000000','#0c003d']}, obj.name + "secondary_mask_count !zoom depend",false,0.4);
  
  var properties = {
    version:obj.name,
    description:'WS_IPAM-' + obj.version  + '-' + obj.description,
    territory: "BRAZIL",
    collection_id: 10.1,
    source: "GT desmatamento",
    theme: "Desmatamento"
  };
  
  exportPerCarta (version_filter,asset,obj.name,properties);
  
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
              description:  'WSilva. ' + 'def_sec_vegetation-'+ name + '-' + gridName,
              assetId: assetOutput + "/" + name + '-' + gridName,
              pyramidingPolicy: {
                  '.default': 'mode'
              },
              region: grid.geometry().bounds(),
              scale: 30,
              maxPixels: 1e13
          });
      }
  );

  
}
