#!/usr/bin/env bash
#
# Clean-build every supported platform gem, smoke-test it in a prod-equivalent
# container, and push the resulting .gem files to Cassette (the RWGPS private
# gem registry).
#
# Usage:
#     scripts/gem-publish.sh           Full pipeline: clean → build → smoke → push
#     scripts/gem-publish.sh skip      Skip clean/build/smoke, push existing .gem files
#
# Environment:
#     CASSETTE_API_KEY  required — from 1Password / cassette admin
#     CASSETTE_HOST     optional — defaults to https://cassette.ridewithgps.com/rubygems
#
# The build/smoke/publish steps are chained through `just` so any changes to
# the underlying recipes (extra lint, new smoke image, etc.) automatically
# flow into publish without touching this script.

set -euo pipefail

MODE="${1:-full}"

case "$MODE" in
  full|skip) ;;
  *)
    echo "usage: $0 [full|skip]" >&2
    echo "  full  (default)  clean + build + smoke + push" >&2
    echo "  skip             push existing gems in ruby_magnus_2/pkg/ only" >&2
    exit 1 ;;
esac

: "${CASSETTE_HOST:=https://cassette.ridewithgps.com/rubygems}"

# Check API key FIRST — no point spending 5 minutes building only to fail at the push.
if [[ -z "${CASSETTE_API_KEY:-}" ]]; then
  echo "error: CASSETTE_API_KEY is not set" >&2
  echo "       export CASSETTE_API_KEY=... (from 1Password / cassette admin) and retry" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONOREPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$MONOREPO_ROOT"

if [[ "$MODE" == "full" ]]; then
  echo ">>> gem-clean"
  just gem-clean

  echo ">>> gem-build (all platforms)"
  just gem-build

  echo ">>> gem-smoke (prod-equivalent roundtrip)"
  just gem-smoke
else
  echo ">>> skip mode: using existing gems in ruby_magnus_2/pkg/"
fi

shopt -s nullglob
gems=( ruby_magnus_2/pkg/ruby_tracklib-*.gem )
if (( ${#gems[@]} == 0 )); then
  echo "error: no .gem files in ruby_magnus_2/pkg/ after gem-build" >&2
  exit 1
fi

echo ">>> pushing ${#gems[@]} gem(s) to ${CASSETTE_HOST}"
for gem_file in "${gems[@]}"; do
  echo "    ${gem_file}"
  GEM_HOST_API_KEY="$CASSETTE_API_KEY" gem push "${gem_file}" --host "${CASSETTE_HOST}"
done

echo ">>> publish complete"
