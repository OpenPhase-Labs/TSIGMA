#!/bin/bash
# Download vendor libraries for TSIGMA web UI
# Run once, commit the downloaded files, never download again.
# Air-gapped compatible after initial download.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENDOR_DIR="$SCRIPT_DIR/../tsigma/static/vendor"

# Version pinning
ALPINE_VERSION="3.14.9"
ECHARTS_VERSION="5.6.0"
MAPLIBRE_VERSION="4.7.1"
TAILWIND_VERSION="3.4.17"

echo "Downloading vendor libraries..."

# Create directories
mkdir -p "$VENDOR_DIR"/{alpine,echarts,maplibre,tailwind}

# Alpine.js
echo "  Alpine.js $ALPINE_VERSION..."
curl -sL -o "$VENDOR_DIR/alpine/alpine.min.js" \
  "https://cdn.jsdelivr.net/npm/alpinejs@${ALPINE_VERSION}/dist/cdn.min.js"

# ECharts
echo "  ECharts $ECHARTS_VERSION..."
curl -sL -o "$VENDOR_DIR/echarts/echarts.min.js" \
  "https://cdn.jsdelivr.net/npm/echarts@${ECHARTS_VERSION}/dist/echarts.min.js"

# MapLibre GL JS
echo "  MapLibre GL JS $MAPLIBRE_VERSION..."
curl -sL -o "$VENDOR_DIR/maplibre/maplibre-gl.js" \
  "https://unpkg.com/maplibre-gl@${MAPLIBRE_VERSION}/dist/maplibre-gl.js"
curl -sL -o "$VENDOR_DIR/maplibre/maplibre-gl.css" \
  "https://unpkg.com/maplibre-gl@${MAPLIBRE_VERSION}/dist/maplibre-gl.css"

# Tailwind CSS (CDN play build — development use)
echo "  Tailwind CSS $TAILWIND_VERSION..."
curl -sL -o "$VENDOR_DIR/tailwind/tailwind.min.js" \
  "https://cdn.tailwindcss.com/${TAILWIND_VERSION}"

echo ""
echo "Done. Vendor libraries downloaded to: $VENDOR_DIR"
echo "Commit these files to git for air-gapped deployment."
