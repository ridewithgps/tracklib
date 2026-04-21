#!/usr/bin/env bash
#
# Build a precompiled Ruby gem for one (or all) platforms via rb-sys-dock.
#
# Usage:
#     scripts/gem-build.sh [platform]
#
# Platform:
#     x86_64-linux     Intel/AMD 64-bit Linux
#     aarch64-linux    ARM 64-bit Linux
#     all              Build every platform sequentially (default)
#
# Environment overrides:
#     RUBY_VERSIONS  Comma-separated Ruby ABIs to include in the gem. Must be
#                    EXACT patch versions shipped in the rbsys image; list them
#                    with:
#                      docker run --rm rbsys/x86_64-linux:<tag> \
#                        ls /usr/local/rake-compiler/ruby/x86_64-linux-gnu/
#     RB_SYS_TAG     rb-sys-dock image tag. Bump when host-side crates need a
#                    newer Rust than the default image ships (e.g. h3o-bit
#                    requires edition2024 → Rust 1.85+).
#
# Why RB_SYS_DOCK_UID=0?
#     rb-sys-dock's default `runas` shim sudos to the host UID inside the
#     container. The container's PAM config caps non-root users at nproc=4096,
#     but a dev workstation's host UID typically owns >4K threads system-wide
#     (IDEs, browsers, other containers). The kernel checks nproc globally per
#     UID, so fork() fails with EAGAIN before any real work runs. Running as
#     root sidesteps the PAM cap (root has nproc=unlimited). The trailing
#     chown inside the container hands ownership back to the host user before
#     the container exits so build artifacts aren't root-owned on the host.

set -euo pipefail

ALL_PLATFORMS=("x86_64-linux" "aarch64-linux" "arm64-darwin")
PLATFORM="${1:-all}"

if [[ "$PLATFORM" == "all" ]]; then
  TARGETS=("${ALL_PLATFORMS[@]}")
elif [[ " ${ALL_PLATFORMS[*]} " == *" $PLATFORM "* ]]; then
  TARGETS=("$PLATFORM")
else
  echo "usage: $0 [platform]" >&2
  echo "  platform: ${ALL_PLATFORMS[*]} | all (default)" >&2
  exit 1
fi

: "${RUBY_VERSIONS:=3.4.8,3.3.10,3.2.9}"
: "${RB_SYS_TAG:=0.9.124}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONOREPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HOST_UID="$(id -u)"
HOST_GID="$(id -g)"

# Local bundle needed to find the rb-sys-dock binary before we enter Docker.
cd "$MONOREPO_ROOT/ruby_magnus_2"
bundle install

CHOWN_PATHS="${MONOREPO_ROOT}/ruby_magnus_2/pkg ${MONOREPO_ROOT}/ruby_magnus_2/tmp ${MONOREPO_ROOT}/ruby_magnus_2/lib/ruby_tracklib ${MONOREPO_ROOT}/tmp /usr/local/cargo/registry /tmp/rb-sys-dock/bundle"

for target in "${TARGETS[@]}"; do
  echo ">>> Building ruby_tracklib gem for $target"

  # Build the command that runs inside the container. rb-sys-dock collapses all
  # whitespace (including newlines) with cmd.gsub!(/\s+/, " "), so statements
  # must be separated with `;` (no heredoc). \$? and \$RC are escaped so they
  # survive to the container shell and refer to its exit code, not ours.
  INNER_CMD="(cd ruby_magnus_2 && bundle install --jobs 4 --retry 3 && bundle exec rake native:${target} gem); RC=\$?; chown -R ${HOST_UID}:${HOST_GID} ${CHOWN_PATHS} 2>/dev/null || true; exit \$RC"

  cd "$MONOREPO_ROOT"
  BUNDLE_GEMFILE="./ruby_magnus_2/Gemfile" \
  RB_SYS_DOCK_UID=0 RB_SYS_DOCK_GID=0 \
  bundle exec rb-sys-dock \
    --platform "$target" \
    --ruby-versions "$RUBY_VERSIONS" \
    --tag "$RB_SYS_TAG" \
    --directory "$MONOREPO_ROOT" \
    -- "$INNER_CMD"
done
