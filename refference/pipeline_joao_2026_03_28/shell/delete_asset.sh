#!/bin/bash

earthengine ls -r projects/mapbiomas-mosaics/assets/LANDSAT/LULC/INDIA/mosaics-1 \
  | xargs -n 1 -P 10 -I {} earthengine rm -v {}

earthengine ls -r projects/mapbiomas-mosaics/assets/LANDSAT/LULC/ARGENTINA/mosaics-1 \
  | xargs -n 1 -P 10 -I {} earthengine rm -rv {}
