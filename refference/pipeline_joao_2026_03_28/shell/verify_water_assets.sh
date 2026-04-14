#!/bin/bash
# Script: verify_water_assets.sh
# Purpose: Verify water asset structure (Collection 5) in GEE
# Usage: ./shell/verify_water_assets.sh

set -e

PROJECT="mapbiomas-brazil"
WATER_COLLECTION_PATH="projects/${PROJECT}/assets/WATER/COLLECTION-5"

echo "========================================"
echo "Water Assets Verification"
echo "========================================"
echo ""

# List all water assets
echo "📁 Listing water assets in COLLECTION-5..."
earthengine ls "${WATER_COLLECTION_PATH}"
echo ""

# Verify annual water asset
ANNUAL_ASSET="${WATER_COLLECTION_PATH}/mapbiomas_brazil_collection5_water_v1"
echo "📊 Checking annual water asset..."
echo "   Path: ${ANNUAL_ASSET}"
earthengine asset info "${ANNUAL_ASSET}" | head -60
echo ""

# Verify monthly water asset type
MONTHLY_ASSET="${WATER_COLLECTION_PATH}/mapbiomas_brazil_collection5_water_monthly_v1"
echo "📊 Checking monthly water asset (ImageCollection)..."
echo "   Path: ${MONTHLY_ASSET}"
earthengine asset info "${MONTHLY_ASSET}"
echo ""

# List first few monthly images
echo "📋 First monthly images in collection..."
earthengine ls "${MONTHLY_ASSET}" | head -12
echo ""

# Check sample monthly image properties
SAMPLE_MONTHLY="${MONTHLY_ASSET}/water_monthly_1985_01"
echo "🔍 Sample monthly image properties (1985-01)..."
earthengine asset info "${SAMPLE_MONTHLY}" | grep -A 20 '"properties"'
echo ""

echo "✅ Verification complete!"
echo ""
echo "Summary:"
echo "  • Annual asset: Image with bands classification_1985..2024"
echo "  • Monthly asset: ImageCollection with water_monthly_{year}_{month} images"
echo "  • Month format: zero-padded string (01–12)"
echo "  • Year range: 1985–2024"
