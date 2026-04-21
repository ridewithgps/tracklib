#!/usr/bin/env bash
#
# Smoke-test a built x86_64 gem inside a production-equivalent container
# (jemalloc preloaded via LD_PRELOAD, matching the RWGPS Rails runtime from
# ridewithgps/ridewithgps PR #22066). Disposable — nothing is written to the
# host, the container is removed on exit.
#
# Usage:
#     scripts/gem-smoke.sh
#
# Environment overrides:
#     PROD_RUBY_IMAGE  Base image to test against. Should track the base image
#                      in the ridewithgps/ridewithgps Dockerfile
#                      (default: ruby:3.3.5-bookworm).

set -euo pipefail

: "${PROD_RUBY_IMAGE:=ruby:3.3.5-bookworm}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONOREPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PKG_DIR="$MONOREPO_ROOT/ruby_magnus_2/pkg"

if ! ls "$PKG_DIR"/ruby_tracklib-*-x86_64-linux.gem >/dev/null 2>&1; then
  echo "error: no x86_64-linux gem found in $PKG_DIR" >&2
  echo "hint:  run 'just gem-build-x86' first" >&2
  exit 1
fi

docker run --rm \
  -v "$PKG_DIR":/pkg \
  "$PROD_RUBY_IMAGE" \
  bash -c '
    set -e
    apt-get update -qq
    apt-get install -y -qq libjemalloc2
    gem install /pkg/ruby_tracklib-*-x86_64-linux.gem
    LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ruby -rruby_tracklib <<'"'"'RUBY'"'"'
data = [{"a" => 0}, {}, {"a" => 40}, {"a" => -40}]
schema = Tracklib::Schema.new([["a", :i64]])
section = Tracklib::Section.standard(schema, data)
buf = Tracklib.write_track([], [section])
reader = Tracklib::TrackReader.new(buf)
result = reader.section_data(0)
raise "ROUNDTRIP FAILED: expected #{data.inspect}, got #{result.inspect}" unless result == data
puts "loaded OK on #{RUBY_PLATFORM}, gem version=#{RubyTracklib::VERSION}"
puts "roundtrip OK: #{data.length} rows -> #{buf.bytesize}-byte .rwtf -> #{result.length} rows"
RUBY
  '
