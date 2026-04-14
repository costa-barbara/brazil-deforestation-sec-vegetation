#!/bin/bash

ORIG="projects/mapbiomas-mosaics/assets/LANDSAT/LULC/ARGENTINA/mosaics-1"
DEST="projects/nexgenmap/MapBiomas2/LANDSAT/ARGENTINA/mosaics-1"

# # Lista todos os assets da origem
# assets=$(earthengine ls "$ORIG")

# for asset in $assets; do
#   name=$(basename "$asset")
#   new_asset="$DEST/$name"

#   echo "🔄 Copiando $asset para $new_asset..."
#   earthengine cp "$asset" "$new_asset"

#   # if [ $? -eq 0 ]; then
#   #   echo "🗑️  Deletando $asset..."
#   #   earthengine rm "$asset"
#   # else
#   #   echo "⚠️ Falha ao copiar $asset. Não será deletado."
#   # fi
# done

earthengine ls "$ORIG" | \
xargs -I {} -P 10 bash -c '
  asset="$1"     # <- vem do xargs (linha do earthengine ls)
  dest="$2"      # <- passado por fora

  name=$(basename "$asset")
  new_asset="$dest/$name"

  echo "🔄 $asset → $new_asset"
  earthengine cp "$asset" "$new_asset"
' _ {} "$DEST"





