#!/bin/bash
set -e

echo "Running Thread Sanitizer on concurrent tests..."
echo ""

RUSTFLAGS="-Z sanitizer=thread" \
  cargo +nightly test --test v2_features_test -- concurrent

echo ""
echo "TSAN checks passed."
