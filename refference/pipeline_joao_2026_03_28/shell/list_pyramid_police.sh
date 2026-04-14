#!/bin/sh

COLLECTION_ID="projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10/DEFORESTATION/deforestation-secondary-vegetation"

earthengine ls "$COLLECTION_ID" | while read -r ASSET_ID
do
  earthengine asset info "$ASSET_ID" | jq -r --arg id "$ASSET_ID" '.bands[] | "\($id) | \(.id): \(.pyramidingPolicy)"'
done
