/**
 * 1. CONFIGURAÇÕES (Tradução das variáveis de ambiente)
 */
var REGION_NAME = 'brazil';
var REGION_ISO3 = 'BRA';
var COLLECTION_ID = 10.1;

var TERRITORY_CATEGORY_ID = '1';
var TERRITORY_FEATURE_ID  = '1';
var TERRITORY_VERSION     = '2';
var TERRITORY_IMAGE_ID    = TERRITORY_CATEGORY_ID + '_' + TERRITORY_FEATURE_ID + '_' + TERRITORY_VERSION;

// Caminhos dos Assets
var ASSET_COLLECTION  = 'projects/mapbiomas-' + REGION_NAME + '/assets/LAND-COVER/COLLECTION-' + String(COLLECTION_ID).replace('.', '_');
var ASSET_INTEGRATION = ASSET_COLLECTION + '/INTEGRATION/classification-ft';
var ASSET_DEF_SEC_VEG = ASSET_COLLECTION + '/DEFORESTATION/deforestation-secondary-vegetation';
var ASSET_DEF_SEC_VEG_FT = ASSET_COLLECTION + '/DEFORESTATION/deforestation-secondary-vegetation-ft';

// Constantes de Classe (Exemplos baseados no seu código anterior)
var BAND_PREFIX = 'classification_';
var CLASS_PRIMARY_VEG_SUPPRESSION = 4;
var CLASS_SECONDARY_VEG_SUPPRESSION = 6;
var CLASS_SECONDARY_VEGETATION = 3;
var CLASS_RECOVERY_TO_SECONDARY = 5;
var CLASS_ANTHROPIC = 1;
var CLASS_OTHER_TRANSITIONS = 7;
var GROUP_SIZE_FILTER = 5;
var GROUP_SIZE_EXCEPTION_FILTER = 10;

/**
 * 2. CLASSE DeforestationSecondaryVegetation
 */
/**
 * Classe para detecção de transições de desmatamento e vegetação secundária.
 * Refatorada para sintaxe ES6 (mais eficiente e organizada).
 */
/**
 * Classe para detecção de transições - Versão compatível com GEE (Prototype Style)
 */
var DeforestationSecondaryVegetation = function(image, years) {
  this.image = image;
  this.years = ee.List(years);
};

// --- MÉTODOS DE INSTÂNCIA (Usando Prototype) ---

DeforestationSecondaryVegetation.prototype.applyRuleKernel4 = function(rule, yearsToProcess) {
  var kernelBef = rule[0];
  var kernelAft = rule[1];
  var minStart = ee.Number(rule[2]);
  var yearsList = ee.List(yearsToProcess);

  var applyKernel4 = function(i, img) {
    i = ee.Number(i);
    img = ee.Image(img);
    
    var y = [0, 1, 2, 3].map(function(idx) { return ee.String(yearsList.get(i.add(idx))); });
    var b = y.map(function(year) { return ee.String(BAND_PREFIX).cat(year); });
    var t = b.map(function(band) { return img.select(band); });

    var mask = t[0].eq(kernelBef[0])
      .and(t[1].eq(kernelBef[1]))
      .and(t[2].eq(kernelBef[2]))
      .and(t[3].eq(kernelBef[3]))
      .and(i.gte(minStart));

    var tOut = t.map(function(imgB, idx) { 
      return imgB.where(mask, kernelAft[idx]).rename(b[idx]); 
    });
    
    return img.addBands(tOut, null, true);
  };

  var sequence = ee.List.sequence(0, yearsList.length().subtract(4));
  this.image = ee.Image(sequence.iterate(applyKernel4, this.image));
  return this;
};

DeforestationSecondaryVegetation.prototype.applyRuleKernel3 = function(rule, yearsToProcess) {
  var kernelBef = rule[0];
  var kernelAft = rule[1];
  var minStart = ee.Number(rule[2]);
  var yearsList = ee.List(yearsToProcess);

  var applyKernel3 = function(i, img) {
    i = ee.Number(i);
    img = ee.Image(img);
    
    var y = [0, 1, 2].map(function(idx) { return ee.String(yearsList.get(i.add(idx))); });
    var b = y.map(function(year) { return ee.String(BAND_PREFIX).cat(year); });
    var t = b.map(function(band) { return img.select(band); });

    var mask = t[0].eq(kernelBef[0])
      .and(t[1].eq(kernelBef[1]))
      .and(t[2].eq(kernelBef[2]))
      .and(i.gte(minStart));

    var tOut = t.map(function(imgB, idx) { 
      return imgB.where(mask, kernelAft[idx]).rename(b[idx]); 
    });
    
    return img.addBands(tOut, null, true);
  };

  var sequence = ee.List.sequence(0, yearsList.length().subtract(3));
  this.image = ee.Image(sequence.iterate(applyKernel3, this.image));
  return this;
};

DeforestationSecondaryVegetation.prototype.applyRules = function(rules, kernelSize, yearsOverride) {
  var yearsToUse = yearsOverride || this.years;
  var self = this;
  
  rules.forEach(function(rule) {
    if (kernelSize === 4) self.applyRuleKernel4(rule, yearsToUse);
    else if (kernelSize === 3) self.applyRuleKernel3(rule, yearsToUse);
  });
  
  return this;
};

DeforestationSecondaryVegetation.prototype.getImage = function() {
  return this.image;
};

// --- MÉTODOS ESTÁTICOS (Anexados diretamente à função construtora) ---
DeforestationSecondaryVegetation.aggregateClasses = function(image, lookupIn, lookupOut) {
  var bandNames = image.bandNames();
  return ee.Image(bandNames.iterate(function(b, acc) {
    return ee.Image(acc).addBands(
      image.select([b]).remap(lookupIn, lookupOut, 0).rename([b])
    );
  }, ee.Image().select()));
};

DeforestationSecondaryVegetation.getClassFrequency = function(image, classId) {
  var bands = image.bandNames();
  var base = image.select([bands.get(0)]).eq(classId);
  
  return ee.Image(bands.slice(1).iterate(function(band, acc) {
    var mask = image.select([band]).eq(classId);
    var prev = ee.Image(acc).select([ee.Image(acc).bandNames().get(-1)]);
    return ee.Image(acc).addBands(prev.add(mask).rename([band]));
  }, base));
};
/**
 * 3. FUNÇÕES DE PROCESSAMENTO E EXPORTAÇÃO
 */
DeforestationSecondaryVegetation.exportDeforestation = function (years, rules4, rules4Sec, rules3Sec, rules4End, yearsEnd) {
  var bandNames = years.map(function(y) { return BAND_PREFIX + y; });
  var integration = ee.ImageCollection(ASSET_INTEGRATION)
    .filter(ee.Filter.eq("version", VERSION_INPUT_INTEGRATION))
    .select(bandNames).min();
  
  var aggregated = DeforestationSecondaryVegetation.aggregateClasses(integration, LOOKUP_IN, LOOKUP_OUT);
  var processor = new DeforestationSecondaryVegetation(aggregated, years);

  processor.applyRules(rules4, 4);
  processor.applyRules(rules4Sec, 4);
  processor.applyRules(rules3Sec, 3);
  processor.applyRules(rules4End, 4, yearsEnd);

  var transitions = processor.getImage();
  var freqAnthropic = DeforestationSecondaryVegetation.getClassFrequency(aggregated, 1);

  transitions = transitions.where(freqAnthropic.gt(1).and(transitions.eq(CLASS_PRIMARY_VEG_SUPPRESSION)), CLASS_SECONDARY_VEG_SUPPRESSION);
  transitions = transitions.where(freqAnthropic.gt(0).and(transitions.eq(CLASS_SECONDARY_VEGETATION)), CLASS_SECONDARY_VEGETATION);

  // Pós-processamento últimos anos (Lógica de anos finais)
  var yIndices = [years.length-3, years.length-2, years.length-1];
  var bFinal = yIndices.map(function(idx) { return BAND_PREFIX + years[idx]; });
  var tF = bFinal.map(function(name) { return transitions.select(name); });

  tF[1] = tF[1].where(tF[0].eq(CLASS_ANTHROPIC).and(tF[1].eq(CLASS_SECONDARY_VEGETATION)), CLASS_ANTHROPIC);
  tF[2] = tF[2].where(tF[0].eq(CLASS_ANTHROPIC).and(tF[2].eq(CLASS_SECONDARY_VEGETATION)), CLASS_ANTHROPIC)
               .where(tF[1].eq(CLASS_ANTHROPIC).and(tF[2].eq(CLASS_SECONDARY_VEGETATION)), CLASS_ANTHROPIC)
               .where(tF[1].eq(CLASS_SECONDARY_VEGETATION).and(tF[2].eq(CLASS_ANTHROPIC)), CLASS_SECONDARY_VEGETATION);

  transitions = transitions.addBands(tF[1].rename(bFinal[1]), null, true).addBands(tF[2].rename(bFinal[2]), null, true);
  print("transitions",transitions);
  // Exemplo de retorno (Ajuste a função exportByGrid conforme seu projeto)
  print('Processamento concluído. Pronto para exportar para:', ASSET_DEF_SEC_VEG);
  return transitions;
};

/**
 * Aplica filtros espaciais para remover fragmentos pequenos de desmatamento e veg. secundária.
 */
DeforestationSecondaryVegetation.exportDeforestationFiltered = function(transitions) {
  
  // Gera máscara de desmatamento (classes 4 e 6) - similar ao list comprehension
  var dfMask = transitions.multiply(0)
    .where(transitions.eq(CLASS_PRIMARY_VEG_SUPPRESSION),1)
    .where(transitions.eq(CLASS_SECONDARY_VEG_SUPPRESSION),1)
    .reduce(ee.Reducer.anyNonZero()).rename("deforestation_mask");

  var svMask = transitions.multiply(0)
    .where(transitions.eq(CLASS_SECONDARY_VEGETATION),1)
    .where(transitions.eq(CLASS_RECOVERY_TO_SECONDARY),1)
    .reduce(ee.Reducer.max()).rename("secondary_vegetation_mask");

  var dfMaskException = transitions
    .select(YEARS_EXCEPTION.map(function(y){return BAND_PREFIX + year}))
    .where(CLASS_PRIMARY_VEG_SUPPRESSION, 1)
    .where(CLASS_SECONDARY_VEG_SUPPRESSION, 1)
    .rename("deforestation_mask_exception")
    .reduce(ee.Reducer.anyNonZero());
  
  // Análise de pixels conectados
  var dfConnected = dfMask.selfMask().connectedPixelCount({maxSize: 100, eightConnected: true});
  var svConnected = svMask.selfMask().connectedPixelCount({maxSize: 100, eightConnected: true});
  var dfConnectedException = dfMaskException.selfMask().connectedPixelCount({maxSize: 100, eightConnected: true});

  var filtered = transitions;

  // Filtros de desmatamento
  filtered = filtered.where(dfConnected.lte(GROUP_SIZE_FILTER).and(filtered.eq(CLASS_PRIMARY_VEG_SUPPRESSION)),CLASS_OTHER_TRANSITIONS);

  filtered = filtered.where(dfConnected.lte(GROUP_SIZE_FILTER).and(filtered.eq(CLASS_SECONDARY_VEG_SUPPRESSION)), CLASS_OTHER_TRANSITIONS);
  
  // Filtros de vegetação secundária
  filtered = filtered.where(svConnected.lte(GROUP_SIZE_FILTER).and(filtered.eq(CLASS_SECONDARY_VEGETATION)), CLASS_OTHER_TRANSITIONS);
  
  filtered = filtered.where(svConnected.lte(GROUP_SIZE_FILTER).and(filtered.eq(CLASS_RECOVERY_TO_SECONDARY)), CLASS_OTHER_TRANSITIONS);

  // Aplica filtros de exceção para anos específicos usando um loop for normal de JS
  for (var i = 0; i < YEARS_EXCEPTION.length; i++) {
    var year = YEARS_EXCEPTION[i];
    var band = BAND_PREFIX + year;
    var filteredLast = filtered.select(band);
    
    filteredLast = filteredLast.where(
      dfConnectedException.lte(GROUP_SIZE_EXCEPTION_FILTER)
        .and(filteredLast.eq(CLASS_PRIMARY_VEG_SUPPRESSION)), 
      CLASS_OTHER_TRANSITIONS
    );
    
    filteredLast = filteredLast.where(
      dfConnectedException.lte(GROUP_SIZE_EXCEPTION_FILTER)
        .and(filteredLast.eq(CLASS_SECONDARY_VEG_SUPPRESSION)), 
      CLASS_OTHER_TRANSITIONS
    );
    
    filtered = filtered.addBands(filteredLast.rename(band), null, true);
  }

  // Define metadados
  filtered = filtered.set({
    'group_size_filter': GROUP_SIZE_FILTER,
    'group_size_exception_filter': GROUP_SIZE_EXCEPTION_FILTER,
    'years_exception': YEARS_EXCEPTION
  });

  // Retorna a exportação (ajuste os parâmetros conforme sua função exportByGrid)
  return filtered;
};


/**
 * Exporta uma imagem para cada grade. 
 * Nota: No Code Editor, as tarefas aparecem na aba 'Tasks'.
 */
var exportByGrid = function(params) {
  // Desestruturando o objeto de parâmetros (estilo JS)
  var image = params.image;
  var assetOutput = params.assetOutput;
  var taskDescriptionPrefix = params.taskDescriptionPrefix;
  var version = params.version;
  var description = params.description;

  var grids = ee.FeatureCollection(ASSET_GRID);
  print('grids',grids.limit(10));
  var taskList = [];
  return
  // stop
  // Usando um loop for clássico para iterar sobre a lista global GRID_NAMES
  for (var i = 0; i < GRID_NAMES.length; i++) {
    var gridName = GRID_NAMES[i];
    
    // Concatenação de strings (evitando $)
    var assetId = assetOutput + '/' + gridName + '-' + version;
    var taskDescription = taskDescriptionPrefix + '-' + gridName + '-' + version;

    // Filtra a grade específica
    var grid = grids.filter(ee.Filter.stringContains('name', gridName));
    
    // Prepara a imagem com metadados
    var imageToExport = image.set({
      'version': version,
      'description': description,
      'territory': REGION_NAME,
      'grid_name': gridName
    });

    // Configura a exportação
    // Nota: scale e maxPixels devem estar definidos globalmente
    Export.image.toAsset({
      image: imageToExport,
      description: taskDescription,
      assetId: assetId,
      region: grid.geometry().buffer(300).bounds(),
      scale: EXPORT_SCALE,
      maxPixels: EXPORT_MAX_PIXELS,
      pyramidingPolicy: {'.default': 'mode'} // Equivalente ao seu helper de pyramiding
    });

    print('▶️ Task criada: ' + taskDescription + '. Verifique a aba Tasks.');
    
    // No JS do navegador, não há "task.start()" automático para Assets
    // O usuário deve clicar em "Run" na interface.
  }
};

/**
 * Gera a descrição em Markdown para o produto de transições de desmatamento.
 */
var getMarkdownDeforestation = function() {
  var lastYear = YEARS[YEARS.length - 1];
  // var gridNamesStr = GRID_NAMES.map(function(g) { return '`' + g + '`'; }).join(', ');

  return '### Deforestation and Secondary Vegetation Transitions – MapBiomas ' + REGION_NAME.toUpperCase() + ' – Collection ' + COLLECTION_ID + '\n\n' +
    '**Description:**\n\n' +
    'This product identifies land cover transitions related to **deforestation** and **secondary vegetation** using temporal pattern recognition rules. The classification is derived from MapBiomas integrated maps and follows a logic of historical validation using 3- and 4-year kernels.\n\n' +
    '**Technical details:**\n' +
    '- **Input asset:** `' + ASSET_INTEGRATION + '`\n' +
    '- **Input version:** `' + VERSION_INPUT_INTEGRATION + '`\n' +
    '- **Output version:** `' + VERSION_OUTPUT_DEF_SEC_VEG + '`\n' +
    '- **Analyzed years:** `' + YEARS[0] + ' to ' + lastYear + '`\n\n' +
    '**Class codes (after rule application):**\n' +
    '- `1`: Anthropogenic\n' +
    '- `2`: Primary Vegetation\n' +
    '- `3`: Secondary Vegetation\n' +
    '- `4`: Suppression of Primary Vegetation (deforestation)\n' +
    '- `5`: Recovery to Secondary Vegetation\n' +
    '- `6`: Suppression of Secondary Vegetation\n' +
    '- `7`: Other transitions\n\n' +
    '**Rule types applied:**\n' +
    '- **4-year kernels** (`RULES_KERNEL4`): Detect consistent transitions.\n' +
    '- **4-year kernels for secondary vegetation** (`RULES_KERNEL4_SECONDARY`): Identify regeneration.\n' +
    '- **3-year kernels for secondary loss** (`RULES_KERNEL3_SECONDARY`): Detect suppression of regenerating areas.\n' +
    '- **End-of-period 4-year rules** (`RULES_KERNEL4_END`): Confirm events in the last 4 years.\n\n' +
    '**Post-processing adjustments:**\n' +
    '- Reclassification based on anthropogenic frequency.\n' +
    '- Adjustment of secondary vegetation in the last three years.\n\n' +
    '**Export structure:**\n' +
    '- Exported to: `' + ASSET_DEF_SEC_VEG + '`\n' +
    // '- Exported grid cells: ' + gridNamesStr + '\n\n' +
    '**Visualization example:**\n' +
    '```javascript\n' +
    'var asset = \'' + ASSET_DEF_SEC_VEG + '\';\n' +
    'var version = \'' + VERSION_OUTPUT_DEF_SEC_VEG + '\';\n' +
    'var image = ee.ImageCollection(asset).filter(ee.Filter.eq(\'version\', version)).mosaic();\n' +
    'var band = image.select(\'classification_' + lastYear + '\');\n' +
    'var vis = {\n' +
    '  min: 0, max: 7,\n' +
    '  palette: [\'#ffffff\', \'#faf5d1\', \'#3f7849\', \'#5bcf20\', \'#ea1c1c\', \'#b4f792\', \'#fe9934\', \'#303149\']\n' +
    '};\n' +
    'Map.addLayer(band, vis, \'Deforestation - ' + lastYear + '\');\n' +
    '```';
};

/**
 * Gera a descrição em Markdown para o produto filtrado.
 */
var getMarkdownDeforestationFiltered = function() {
  var lastYear = YEARS[YEARS.length - 1];
  var yearsExStr = YEARS_EXCEPTION.join(', ');

  return '### Filtered Deforestation and Secondary Vegetation – MapBiomas ' + REGION_NAME.toUpperCase() + ' – Collection ' + COLLECTION_ID + '\n\n' +
    '**Description:**\n\n' +
    'This product applies **spatial filtering** to the base transition map, removing small patches considered noise. It ensures reliable spatial consistency.\n\n' +
    '**Technical details:**\n' +
    '- **Input asset:** `' + ASSET_DEF_SEC_VEG + '`\n' +
    '- **Output asset:** `' + ASSET_DEF_SEC_VEG_FT + '`\n' +
    '- **Input version:** `' + VERSION_OUTPUT_DEF_SEC_VEG + '`\n' +
    '- **Output version:** `' + VERSION_OUTPUT_DEF_SEC_VEG_FT + '`\n' +
    '- **Connected pixel threshold:** `' + GROUP_SIZE_FILTER + '` pixels\n' +
    '- **Exception year(s):** ' + yearsExStr + '\n' +
    '- **Exception threshold:** `' + GROUP_SIZE_EXCEPTION_FILTER + '` pixels\n\n' +
    '**Methodology:**\n' +
    '1. Classes `4` and `6` are masked and filtered by `connectedPixelCount()`.\n' +
    '2. If area < `' + GROUP_SIZE_FILTER + '` pixels, reassigned to class `7`.\n' +
    '3. For years (' + yearsExStr + '), threshold is `' + GROUP_SIZE_EXCEPTION_FILTER + '` pixels.\n\n' +
    '**Export structure:**\n' +
    '- Exported to: `' + ASSET_DEF_SEC_VEG_FT + '`\n\n' +
    '**Visualization script example:**\n' +
    '```javascript\n' +
    'var asset = \'' + ASSET_DEF_SEC_VEG_FT + '\';\n' +
    'var version = \'' + VERSION_OUTPUT_DEF_SEC_VEG_FT + '\';\n' +
    'var image = ee.ImageCollection(asset).filter(ee.Filter.eq(\'version\', version)).mosaic();\n' +
    'var vis = {\n' +
    '  min: 0, max: 7,\n' +
    '  palette: [\'#ffffff\', \'#faf5d1\', \'#3f7849\', \'#5bcf20\', \'#ea1c1c\', \'#b4f792\', \'#fe9934\', \'#303149\']\n' +
    '};\n' +
    'Map.addLayer(image.select(\'classification_' + lastYear + '\'), vis, \'Filtered ' + lastYear + '\');\n' +
    '```';
};
/**
 * 4. EXECUÇÃO (START)
 */
// Exemplo de chamada (Certifique-se que as listas de regras e anos estejam definidas)
var YEARS = [
  1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,
  1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,
  2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,
  2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,
];

// Deforestation and Secondary Vegetation class codes
var CLASS_NO_DATA                   = 0;
var CLASS_ANTHROPIC                 = 1;
var CLASS_PRIMARY_VEGETATION        = 2;
var CLASS_SECONDARY_VEGETATION      = 3;
var CLASS_PRIMARY_VEG_SUPPRESSION   = 4;
var CLASS_RECOVERY_TO_SECONDARY     = 5;
var CLASS_SECONDARY_VEG_SUPPRESSION = 6;
var CLASS_OTHER_TRANSITIONS         = 7;

// // Year range between 1985 and 2024 for annual calculations of deforestation and secondary vegetation sub-products
// var YEARS = list(range(1985, 2025))

// // Pairs [start, end] of years between 2001 and 2024 for accumulated calculations
// // 1999 and 2000 are used as base years for the deforestation and secondary vegetation logic
var PERIODS_ALL_YEARS = [];

// YEARS.slice(2) equivale ao YEARS[2:] do Python
YEARS.slice(2).forEach(function(i) {
  // O range(i, last + 1) do Python vira um loop for
  for (var j = i; j <= YEARS[YEARS.length - 1]; j++) {
    PERIODS_ALL_YEARS.push([i, j]);
  }
});

// // Last year to consider in post-processing logic
var LAST_YEAR = YEARS.slice(-1);

// // Last 4 years for end-of-period deforestation logic (e.g. 2021–2024, when last year is 2024)
var YEARS_END = YEARS.slice(-4);

// // Lookup table for class aggregation
// // Format: [original_class_id, aggregated_class_id]
var CLASS_LOOKUP = [
    [0, CLASS_NO_DATA],                     // 6. Not Observed
    [1, CLASS_PRIMARY_VEGETATION],          // 1. Forest formation
    [3, CLASS_PRIMARY_VEGETATION],          // 1.1. Forest
    [4, CLASS_PRIMARY_VEGETATION],          // 1.2. Dry forest
    [5, CLASS_PRIMARY_VEGETATION],          // 1.3. Mangrove
    [6, CLASS_PRIMARY_VEGETATION],          // 1.4. Floodable Forest
    [9, CLASS_ANTHROPIC],                   // 3.3. Forest Plantation
    [10, CLASS_PRIMARY_VEGETATION],         // 2. Non Forest Natural Formation
    [11, CLASS_PRIMARY_VEGETATION],         // 2.1. Swamp or Flooded Grassland
    [12, CLASS_PRIMARY_VEGETATION],         // 2.2. Grassland / Herbaceous Formation
    [13, CLASS_PRIMARY_VEGETATION],         // 2.6. Other non-forest formation
    [14, CLASS_ANTHROPIC],                  // 3. Farming
    [15, CLASS_ANTHROPIC],                  // 3.1. Pasture
    [18, CLASS_ANTHROPIC],                  // 3.2. Agriculture
    [19, CLASS_ANTHROPIC],                  // 3.2.1. Temporary Crop
    [20, CLASS_ANTHROPIC],                  // 3.2.1.2. Sugar cane
    [21, CLASS_ANTHROPIC],                  // 3.4. Mosaic of agriculture and pasture
    [22, CLASS_ANTHROPIC],                  // 4. Non vegetated area
    [23, CLASS_OTHER_TRANSITIONS],          // 4.1. Beach
    [24, CLASS_ANTHROPIC],                  // 4.2. Urban Area
    [25, CLASS_ANTHROPIC],                  // 4.7. Other non vegetated area
    [26, CLASS_OTHER_TRANSITIONS],          // 5. Water
    [27, CLASS_NO_DATA],                    // 6. Not Observed
    [29, CLASS_OTHER_TRANSITIONS],          // 2.3. Rocky Outcrop
    [30, CLASS_ANTHROPIC],                  // 4.3. Mining
    [31, CLASS_ANTHROPIC],                  // 5.2. Aquaculture
    [32, CLASS_OTHER_TRANSITIONS],          // 4.4. Coastal salt flats
    [33, CLASS_OTHER_TRANSITIONS],          // 5.1. River, Lake or Ocean
    [34, CLASS_OTHER_TRANSITIONS],          // 5.3. Glaciares
    [35, CLASS_ANTHROPIC],                  // 3.2.1. Palm Oil
    [36, CLASS_ANTHROPIC],                  // 3.2.2. Perennial Crop
    [39, CLASS_ANTHROPIC],                  // 3.2.1.1. Soybean
    [40, CLASS_ANTHROPIC],                  // 3.2.2. Rice
    [41, CLASS_ANTHROPIC],                  // 3.2.1.5. Other Temporary Crops
    [46, CLASS_ANTHROPIC],                  // 3.2.2.1. Coffee
    [47, CLASS_ANTHROPIC],                  // 3.2.2.2. Citrus
    [48, CLASS_ANTHROPIC],                  // 3.2.2.4. Other Perennial Crops
    [49, CLASS_PRIMARY_VEGETATION],         // 1.5. Wooded Sandbank Vegetation
    [50, CLASS_PRIMARY_VEGETATION],         // 2.4. Herbaceous Sandbank Vegetation
    [61, CLASS_OTHER_TRANSITIONS],          // 4.5. Salt flat
    [62, CLASS_ANTHROPIC],                  // 3.2.1.4. Cotton
    [66, CLASS_PRIMARY_VEGETATION],         // 2.4. Scrubland
    [68, CLASS_OTHER_TRANSITIONS],          // 4.6. Other natural non vegetated area
    [69, CLASS_OTHER_TRANSITIONS],          // 5.3. Coral Reef
    [70, CLASS_PRIMARY_VEGETATION],         // 2.5. Fog oasis
    [72, CLASS_ANTHROPIC],                  // 3.2.3. Other crops
    [75, CLASS_ANTHROPIC],                  // Fotovoltaico
];

// No JS, o pair[0] do Python vira o retorno da função anônima
var LOOKUP_IN = CLASS_LOOKUP.map(function(pair) {
  return pair[0];
});

var LOOKUP_OUT = CLASS_LOOKUP.map(function(pair) {
  return pair[1];
});


// // Rules for detecting 4-year deforestation (suppression of primary vegetation)
var RULES_KERNEL4 = [
    [[2, 2, 1, 1], [2, 2, 4, 1], 0],  // // Primary vegetation suppression
];

// // End-of-period deforestation rules (also 4-year) applied to last 3 years (unconfirmed transitions)
var RULES_KERNEL4_END = [
    [[2, 2, 2, 1], [2, 2, 2, 4], 0],  // // Unconfirmed primary deforestation
    [[3, 3, 3, 1], [3, 3, 3, 6], 0],  // // Unconfirmed secondary deforestation
];

// Rules for secondary vegetation establishment (4-year patterns)
var RULES_KERNEL4_SECONDARY = [
    [[1, 2, 2, 2], [1, 5, 3, 3], 1], // Establishment of secondary vegetation | t2 recebe 5: precisa de histórico antes de 1985
    [[5, 3, 3, 2], [5, 3, 3, 3], 0], // Recovery to secondary vegetation | t2 já é 3, sem risco
    [[3, 2, 2, 2], [3, 3, 3, 3], 1], // Secondary vegetation persistence | t2 recebe 3: mesmo motivo da regra 1
    [[3, 2, 2, 4], [3, 3, 3, 4], 1], // Suppression of secondary vegetation | t2 recebe 3: mesmo motivo
    [[3, 3, 2, 4], [3, 3, 3, 6], 0], // Suppression of secondary vegetation with recovery | t2 já é 3, sem risco
    [[3, 3, 2, 2], [3, 3, 3, 3], 0], // Suppression of secondary vegetation with persistence | t2 já é 3, sem risco
    [[3, 3, 3, 2], [3, 3, 3, 3], 0], // Suppression of secondary vegetation with persistence | t2 já é 3, sem risco
    [[1, 2, 2, 4], [1, 1, 1, 1], 0], // Suppression of primary vegetation with recovery | não escreve 3 ou 5, sem risco
];

// Rules for deforestation in secondary vegetation (3-year patterns)
var RULES_KERNEL3_SECONDARY = [
    [[3, 4, 1], [3, 6, 1], 0], // Suppression of secondary vegetation
];

// ================================
// FILTER CONFIGURATION FOR DEFORESTATION AND SECONDARY VEGETATION
// ================================

// Threshold (in pixels) for spatial filtering of deforestation and secondary vegetation patches
var GROUP_SIZE_FILTER = 33               // applied to all years by default
var GROUP_SIZE_EXCEPTION_FILTER = 33     // applied only to exception years (usually the last year)

// Years to apply the relaxed threshold (e.g., the most recent years)
var YEARS_EXCEPTION = [YEARS[-1]]       // List of years to treat as exceptions (default: last year only)

// ================================
// VERSIONING
// ================================

var VERSION_INPUT_INTEGRATION = '0-4-tra-1'
var VERSION_OUTPUT_TRANSITIONS     = VERSION_INPUT_INTEGRATION + '-1'; // for export_transitions()
var VERSION_OUTPUT_DEF_SEC_VEG     = VERSION_INPUT_INTEGRATION + '-2'; // for export_deforestation_secondary_vegetation()
var VERSION_OUTPUT_DEF_SEC_VEG_FT  = VERSION_INPUT_INTEGRATION + '-2'; // for export_deforestation_secondary_vegetation_ft()
var VERSION_OUTPUT_DEF_SEC_VEG_ANN = VERSION_INPUT_INTEGRATION + '-1'; // for export_deforestation_secondary_vegetation_annual()
var VERSION_OUTPUT_DEF_SEC_VEG_ACC = VERSION_INPUT_INTEGRATION + '-1'; // for export_deforestation_secondary_vegetation_accumulated()
var VERSION_OUTPUT_DEF_FREQ        = VERSION_INPUT_INTEGRATION + '-1'; // for export_deforestation_frequency()
var VERSION_OUTPUT_SEC_VEG_AGE     = VERSION_INPUT_INTEGRATION + '-1'; // for export_secondary_vegetation_age()
var ASSET_GRID = 'projects/mapbiomas-workspace/AUXILIAR/cim-world-1-250000';

var deforestation_and_secondary_vegetation = DeforestationSecondaryVegetation.exportDeforestation(YEARS, RULES_KERNEL4, RULES_KERNEL4_SECONDARY, RULES_KERNEL3_SECONDARY, RULES_KERNEL4_END, YEARS_END);
print("deforestation_and_secondary_vegetation",deforestation_and_secondary_vegetation);
Map.addLayer(deforestation_and_secondary_vegetation, {}, 'Transições de Desmatamento');


exportByGrid({
  image:deforestation_and_secondary_vegetation,
  asset_output:ASSET_DEF_SEC_VEG,
  task_description_prefix:'deforestation-secondary-vegetation',
  version:VERSION_OUTPUT_DEF_SEC_VEG,
  description:getMarkdownDeforestation()
});


  // Carrega a imagem de transições gerada anteriormente
// var deforestation_and_secondary_vegetation = ee.ImageCollection(ASSET_DEF_SEC_VEG)
//   .filter(ee.Filter.eq("version", VERSION_OUTPUT_DEF_SEC_VEG))
//   .min();

var deforestation_and_secondary_vegetation_filtered = DeforestationSecondaryVegetation.exportDeforestationFiltered(deforestation_and_secondary_vegetation);
print("deforestation_and_secondary_vegetation_filtered",deforestation_and_secondary_vegetation_filtered);
exportByGrid({
  image: deforestation_and_secondary_vegetation_filtered,
  assetOutput: ASSET_DEF_SEC_VEG_FT,
  taskDescriptionPrefix: 'deforestation-secondary-vegetation-ft',
  version: VERSION_OUTPUT_DEF_SEC_VEG_FT,
  description: getMarkdownDeforestationFiltered()
});
