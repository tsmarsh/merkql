#!/bin/bash
set -e

DURATION=${1:-30}
TARGETS=(
    decompress
    node_deser
    record_deser
    snapshot_deser
    hash_from_hex
    group_offsets
    topic_config
    retention_marker
    index_entry
)

echo "Running ${#TARGETS[@]} fuzz targets for ${DURATION}s each..."
echo ""

for target in "${TARGETS[@]}"; do
    echo "=== Fuzzing: $target (${DURATION}s) ==="
    cargo fuzz run "$target" -- -max_total_time="$DURATION"
    echo ""
done

echo "All fuzz targets completed."
