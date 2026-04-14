#!/bin/bash

ASSET_BASE="projects/mapbiomas-public/assets/brazil/lulc/v1/collection_10"
VERSION="_v1"

for YEAR in {1985..2024}; do

  # MacOS-compatible timestamp generation
  START=$(date -j -f "%Y-%m-%d" "${YEAR}-01-01" "+%s000")
  END=$(date -j -f "%Y-%m-%d" "${YEAR}-12-31" "+%s000")

  ASSET="${ASSET_BASE}_${YEAR}${VERSION}"

  echo "Atualizando: $ASSET"
  echo " - time_start = $START"
  echo " - time_end   = $END"

  # earthengine asset set \
  #   -p time_start=$START \
  #   -p time_end=$END \
  #   "$ASSET"
  earthengine asset set \
    -p time_start=null \
    -p time_end=null \
    "$ASSET"

done

echo "✔️ Finalizado!"
