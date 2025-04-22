#!/bin/bash
set -euo pipefail

# Usage check
if [[ $# -ne 1 || ( "$1" != "genomes" && "$1" != "sra" ) ]]; then
    echo "Usage: $0 [genomes|sra]"
    exit 1
fi

# Set user input
DATA_TYPE="$1"

# Base paths
BASE_DIR="/group/ctbrowngrp/irber/data/wort-data"
SIG_DIR="$BASE_DIR/wort-$DATA_TYPE/sigs"

# Date and file paths
NOW=$(date +"%Y-%m-%d")
updated_siglist="$NOW.wort.$DATA_TYPE.sigs.txt"
# previous_manifest="./2023-05-26.wort.$DATA_TYPE.sqlmf" # Uncomment if you have a previous manifest
updated_manifest="./$NOW.wort.$DATA_TYPE.csv.gz"

# Print context
echo "Building manifest for: $DATA_TYPE"
echo "Signature directory: $SIG_DIR"
echo "Signature list: $updated_siglist"
# echo "Previous manifest: $previous_manifest"

# Step 1: Find signatures, only if sig file doesn't already exist
if [[ -f "$updated_siglist" ]]; then
    echo "Signature list $updated_siglist already exists. Skipping find step."
else
    echo "Finding $DATA_TYPE signatures..."
    find "$SIG_DIR" -type f > "$updated_siglist"
    echo "Found $(wc -l < "$updated_siglist") signature files."
fi

# Step 2: Build manifest
echo "Building new manifest '$updated_manifest' with sourmash sig collect..."
sourmash sig collect --from-file "$updated_siglist" \
    --no-require-manifest --debug \
    # --merge --previous "$previous_manifest" \
    --output "$updated_manifest" --manifest-format csv

# Step 3: Make manifest read-only
chmod a-w "$updated_manifest"
echo "Manifest written to $updated_manifest and made read-only."
