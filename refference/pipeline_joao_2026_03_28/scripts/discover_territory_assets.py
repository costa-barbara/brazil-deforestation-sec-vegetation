#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Discover and list available territory assets in GEE with their versions.

Usage:
    python scripts/discover_territory_assets.py [REGION]

Examples:
    python scripts/discover_territory_assets.py BRAZIL
    python scripts/discover_territory_assets.py  # lists all regions
"""

import ee
import re
import sys
from collections import defaultdict

def initialize_ee():
    """Initialize Earth Engine."""
    try:
        ee.Initialize(project='mapbiomas-brazil')
    except Exception:
        try:
            ee.Authenticate()
            ee.Initialize(project='mapbiomas-brazil')
        except Exception as e:
            print(f"❌ Erro ao autenticar GEE: {e}")
            sys.exit(1)

def extract_version(asset_name):
    """Extract version number from asset name (e.g., 'POLITICAL_LEVEL_1_v1' -> 1)."""
    match = re.search(r'_v(\d+)$', asset_name)
    if match:
        return int(match.group(1))
    return 0

def get_territory_assets(region):
    """
    List all territory assets for a given region and find the highest versions.

    Args:
        region (str): Region name (e.g., 'BRAZIL')

    Returns:
        dict: Dictionary mapping category names to their highest version asset paths
    """
    base_path = f"projects/mapbiomas-territories/assets/TERRITORIES/{region}/WORKSPACE"

    print(f"\n🔍 Procurando assets em: {base_path}")

    # Listar pastas de categorias
    try:
        workspace_children = ee.data.listAssets({'parent': base_path})
    except Exception as e:
        print(f"❌ Erro ao acessar {base_path}: {e}")
        return {}

    # Agrupar assets por categoria (nome da pasta) e versão (nome do asset dentro)
    categories = defaultdict(list)

    assets_list = workspace_children.get('assets', [])
    print(f"📦 Encontradas {len(assets_list)} categorias no WORKSPACE")

    for category_folder in assets_list:
        category_path = category_folder['id']
        category_name = category_folder['id'].split('/')[-1]

        # Listar versões dentro de cada categoria
        try:
            versions = ee.data.listAssets({'parent': category_path})
            for version_asset in versions.get('assets', []):
                asset_id = version_asset['id']
                asset_name = asset_id.split('/')[-1]

                # Extrair versão
                # Exemplo: POLITICAL_LEVEL_1_v1
                match = re.match(r'.+_v(\d+)$', asset_name)
                if match:
                    version = int(match.group(1))
                    categories[category_name].append({
                        'name': asset_name,
                        'version': version,
                        'path': asset_id
                    })
                else:
                    # Asset sem versão
                    categories[category_name].append({
                        'name': asset_name,
                        'version': 0,
                        'path': asset_id
                    })
        except Exception as e:
            print(f"⚠️  Erro ao listar versões de {category_name}: {e}")
            continue

    # Para cada categoria, pegar apenas a de maior versão
    highest_versions = {}
    for category, assets in sorted(categories.items()):
        if assets:
            # Ordenar por versão (decrescente)
            sorted_assets = sorted(assets, key=lambda x: x['version'], reverse=True)
            highest = sorted_assets[0]
            highest_versions[category] = highest

    return highest_versions

def discover_all_regions():
    """Discover all available regions in the territories project."""
    base_path = "projects/mapbiomas-territories/assets/TERRITORIES"

    print(f"\n🔍 Listando regiões em: {base_path}\n")

    try:
        territories_list = ee.data.listAssets({'parent': base_path})
    except Exception as e:
        print(f"❌ Erro ao acessar {base_path}: {e}")
        return []

    regions = []
    for asset in territories_list.get('assets', []):
        region_name = asset['id'].split('/')[-1]
        if region_name != 'WORKSPACE':  # Pular se for WORKSPACE
            regions.append(region_name)

    return sorted(regions)

def main():
    initialize_ee()

    if len(sys.argv) > 1:
        region = sys.argv[1].upper()
        assets = get_territory_assets(region)

        if not assets:
            print(f"❌ Nenhum asset encontrado para {region}")
            return

        print(f"\n✅ Territory assets com MAIOR VERSÃO para {region}:\n")
        print(f"{'Categoria':<50} | {'Versão':<3} | {'Path':<80}")
        print("-" * 150)

        for category in sorted(assets.keys()):
            asset = assets[category]
            print(f"{category:<50} | {asset['version']:<3} | {asset['path']}")

        # Exibir em formato Python dict
        print(f"\n📋 Formato Python (para copiar):\n")
        print("TERRITORY_ASSETS = {")
        for category in sorted(assets.keys()):
            asset = assets[category]
            print(f"    '{category}': '{asset['path']}',")
        print("}")

    else:
        # Listar todas as regiões
        regions = discover_all_regions()
        print(f"📍 Regiões disponíveis ({len(regions)}):\n")
        for region in regions:
            print(f"  - {region}")

        print(f"\nUse: python scripts/discover_territory_assets.py <REGION>")
        print(f"     python scripts/discover_territory_assets.py BRAZIL")

if __name__ == "__main__":
    main()
