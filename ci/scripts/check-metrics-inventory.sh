#!/usr/bin/env bash
# Verify docs/metrics/inventory.tsv is up to date with the metric registrations
# in src/. Fails if the extractor would produce a different file — i.e. if
# someone added, renamed, or removed a metric without regenerating the TSV.
#
# To fix a failure, run:
#   python3 docs/metrics/extract.py
# and commit the resulting docs/metrics/inventory.tsv.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if ! diff -u docs/metrics/inventory.tsv <(python3 docs/metrics/extract.py --stdout); then
    echo
    echo 'docs/metrics/inventory.tsv is out of date.'
    echo 'Regenerate with: python3 docs/metrics/extract.py'
    exit 1
fi
