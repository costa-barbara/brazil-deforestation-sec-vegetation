#!/usr/bin/env bash
set -euo pipefail

# Prefixo comum
COLLECTION_PREFIX="projects/mapbiomas-public/assets/brazil/soil/collection3"

# Imagem (carbono)
IMAGE_ASSET="${COLLECTION_PREFIX}/mapbiomas_brazil_collection3_soil_carbon_v1"

# ImageCollections (frações, pedregosidade, textura etc.)
IMAGE_COLLECTION_ASSETS=(
  "${COLLECTION_PREFIX}/mapbiomas_brazil_collection3_soil_clay_fraction_v1"
  "${COLLECTION_PREFIX}/mapbiomas_brazil_collection3_soil_sand_fraction_v1"
  "${COLLECTION_PREFIX}/mapbiomas_brazil_collection3_soil_silt_fraction_v1"
  "${COLLECTION_PREFIX}/mapbiomas_brazil_collection3_soil_stoniness_v1"
  "${COLLECTION_PREFIX}/mapbiomas_brazil_collection3_soil_textural_class_v1"
  "${COLLECTION_PREFIX}/mapbiomas_brazil_collection3_soil_textural_group_v1"
  "${COLLECTION_PREFIX}/mapbiomas_brazil_collection3_soil_textural_subgroup_v1"
)

echo "=================================================="
echo "IMAGE: $(basename "${IMAGE_ASSET}")"
echo "--------------------------------------------------"
echo "Bands:"
earthengine asset info "${IMAGE_ASSET}" | jq -r '.bands[].id'

echo
echo "Properties:"
earthengine asset info "${IMAGE_ASSET}" | jq '.properties'
echo "=================================================="
echo

# Loop nas ImageCollections
for ASSET in "${IMAGE_COLLECTION_ASSETS[@]}"; do
  echo "IMAGE COLLECTION: $(basename "${ASSET}")"
  echo "--------------------------------------------------"
  earthengine asset info "${ASSET}" | jq '.properties'
  echo
done
