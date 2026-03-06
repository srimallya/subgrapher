#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ICONS_DIR="$ROOT_DIR/assets/icons"
SVG_PATH="$ICONS_DIR/app-icon.svg"
ICONSET_DIR="$ICONS_DIR/app-icon.iconset"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

if [[ ! -f "$SVG_PATH" ]]; then
  echo "Missing source SVG: $SVG_PATH" >&2
  exit 1
fi

for cmd in sips iconutil; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd" >&2
    exit 1
  fi
done

mkdir -p "$ICONSET_DIR"

sips -s format png "$SVG_PATH" --out "$TMP_DIR/base.png" >/dev/null
BASE_PNG="$TMP_DIR/base.png"

if [[ ! -f "$BASE_PNG" ]]; then
  echo "Failed to render preview from SVG." >&2
  exit 1
fi

resize_png() {
  local size="$1"
  local output="$2"
  sips -z "$size" "$size" "$BASE_PNG" --out "$output" >/dev/null
}

resize_png 16 "$ICONS_DIR/app-icon-16.png"
resize_png 24 "$ICONS_DIR/app-icon-24.png"
resize_png 32 "$ICONS_DIR/app-icon-32.png"
resize_png 48 "$ICONS_DIR/app-icon-48.png"
resize_png 64 "$ICONS_DIR/app-icon-64.png"
resize_png 128 "$ICONS_DIR/app-icon-128.png"
resize_png 256 "$ICONS_DIR/app-icon-256.png"
resize_png 512 "$ICONS_DIR/app-icon-512.png"
cp "$BASE_PNG" "$ICONS_DIR/app-icon-1024.png"

resize_png 16 "$ICONSET_DIR/icon_16x16.png"
resize_png 32 "$ICONSET_DIR/icon_16x16@2x.png"
resize_png 32 "$ICONSET_DIR/icon_32x32.png"
resize_png 64 "$ICONSET_DIR/icon_32x32@2x.png"
resize_png 128 "$ICONSET_DIR/icon_128x128.png"
resize_png 256 "$ICONSET_DIR/icon_128x128@2x.png"
resize_png 256 "$ICONSET_DIR/icon_256x256.png"
resize_png 512 "$ICONSET_DIR/icon_256x256@2x.png"
resize_png 512 "$ICONSET_DIR/icon_512x512.png"
cp "$BASE_PNG" "$ICONSET_DIR/icon_512x512@2x.png"

iconutil -c icns "$ICONSET_DIR" -o "$ICONS_DIR/app-icon.icns"
