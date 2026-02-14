#!/bin/bash
set -e

echo "Running Miri on pure-computation tests..."
echo ""

echo "=== hash::tests ==="
cargo +nightly miri test hash::tests

echo "=== node::tests ==="
cargo +nightly miri test node::tests

echo "=== compression::tests (skip large_data) ==="
cargo +nightly miri test compression::tests -- --skip large_data

echo "=== record::tests ==="
cargo +nightly miri test record::tests

echo ""
echo "Miri checks passed."
