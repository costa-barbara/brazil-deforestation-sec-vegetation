#!/usr/bin/env bash
# ee_public_loop_parallel.sh
# Lista assets de um diretório do EE e aplica acl set public em cada um (multithread).

ROOT="projects/mapbiomas-territories/assets/TERRITORIES"

# guarda todos os assets em uma variável
ASSETS=$(earthengine ls -r "$ROOT")

# define o número de processos em paralelo (ajuste conforme sua máquina/rede)
NPROC=5

# envia cada asset para ser processado em paralelo
echo "$ASSETS" | xargs -n 1 -P "$NPROC" bash -c '
  asset="$0"
  echo "Processando: $asset"
  earthengine acl set public "$asset"
'
