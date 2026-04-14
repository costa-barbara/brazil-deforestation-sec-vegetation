#!/bin/bash

# Padrão a ser buscado na descrição da tarefa
PADRAO="integration-filter-general"

N_THREADS=8  # Número de tarefas em paralelo (pode ajustar)

echo "Buscando tarefas com padrão: \"$PADRAO\""
echo

# Filtra tarefas READY ou RUNNING com o padrão, extrai ID e descrição
earthengine task list\
    | grep "$PADRAO" \
    | grep -E 'READY|RUNNING' \
    | awk '{print $1 " " $3}' \
    | xargs -n2 -P "$N_THREADS" bash -c '
        ID=$0
        DESC=$1
        echo "Cancelando tarefa: ID=$ID, descrição=\"$DESC\""
        earthengine task cancel "$ID"
    '

echo
echo "Processo concluído."

