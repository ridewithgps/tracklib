# Tracklib — developer notes

Monorepo layout:

```
tracklib/           Core Rust crate (.rwtf format encode/decode)
ruby_magnus_2/      Ruby gem wrapping the core via magnus (current)
ruby_tracklib/      Legacy rutie-based gem (kept for reference; not built)
rwtfinspect/        CLI for inspecting .rwtf files
java_tracklib/      Java bindings
scripts/            Shell helpers (gem-build.sh, gem-smoke.sh)
justfile            Task runner — `just --list` for the full menu
```

All routine tasks are wired through the root `justfile`. Run `just` with no args for a listing. The rest of this doc is the short version of when to reach for each recipe.

## Everyday development

| Task | Command |
|---|---|
| Run all tests (core + gem) | `just test` |
| Run just the core Rust tests | `just rust-test` |
| Run the gem tests (cargo tests + RSpec) | `just ruby-test` |
| Format everything | `just fmt` |
| Lint everything | `just lint` |
| Full CI check locally | `just ci` |

`just ruby-test` compiles the native extension via `rake compile` before running RSpec, so it's safe to run after code changes to the Rust ext — no separate build step needed.

## Building precompiled gems

Precompiled (platform-specific) gems ship pre-built `.so` files for each supported Ruby ABI so consumers don't need a Rust toolchain to install. The build happens inside a `rbsys/<platform>:<tag>` Docker container via `rb-sys-dock`.

| Task | Command |
|---|---|
| Build gems for **all platforms** (x86_64-linux, aarch64-linux) | `just gem-build` |
| Build one platform | `just gem-build x86_64-linux` or `just gem-build aarch64-linux` |
| Wipe build artifacts (handles root-owned leftovers via a container) | `just gem-clean` |

Outputs land in `ruby_magnus_2/pkg/ruby_tracklib-<version>-<platform>.gem`. First build per platform pulls a ~1–2 GB Docker image; subsequent builds are cached.

### Configuration for gem builds

The defaults live in `scripts/gem-build.sh` and can be overridden via env vars:

| Var | Default | When to change |
|---|---|---|
| `RUBY_VERSIONS` | `3.4.8,3.3.10,3.2.9` | Match the exact patch versions in the rbsys image. List them with `docker run --rm rbsys/x86_64-linux:<tag> ls /usr/local/rake-compiler/ruby/x86_64-linux-gnu/`. |
| `RB_SYS_TAG` | `0.9.124` | Bump if a crate requires a newer Rust than the image ships (e.g. `edition2024` needs ≥1.85 → image ≥0.9.120). |

To add a new platform, edit `ALL_PLATFORMS` in `scripts/gem-build.sh` — that's the single source of truth for which platforms `gem-build` (with no argument) builds.

### Quirks worth knowing about

**The build runs as root inside the container** (`RB_SYS_DOCK_UID=0`). The default rb-sys-dock workflow sudos to the host UID, and a PAM config inside the image caps non-root `nproc` at 4096 — on a workstation where UID 1000 owns thousands of threads (IDEs, browsers, other containers), `fork()` fails with `EAGAIN` before any real work runs. Running as root sidesteps the cap; the script chowns outputs back to the host user before the container exits.

**Docker must be running** and your user must have permissions to run containers. The mounts assume the monorepo root is reachable at its absolute path from inside the container.

## Smoke-testing a built gem

After building, run:

```
just gem-smoke
```

This spins up `ruby:3.3.5-bookworm` (the base image from the RWGPS Rails Dockerfile), `apt-get install`s `libjemalloc2`, preloads it via `LD_PRELOAD` (matching prod), installs the `.gem` from `ruby_magnus_2/pkg/`, and performs a small roundtrip: builds an I64-column section, serializes it with `Tracklib.write_track`, reads it back with `Tracklib::TrackReader.new`, and asserts byte-equality.

A passing run looks like:

```
loaded OK on x86_64-linux, gem version=0.1.0
roundtrip OK: 4 rows -> 55-byte .rwtf -> 4 rows
```

The script is disposable — the container has `--rm`, so nothing is written to your filesystem.

### Smoke-testing against other Ruby versions

Override the base image via the `PROD_RUBY_IMAGE` env var:

```
PROD_RUBY_IMAGE=ruby:3.4-bookworm scripts/gem-smoke.sh
PROD_RUBY_IMAGE=ruby:3.2-bookworm scripts/gem-smoke.sh
```

Useful for verifying each ABI `.so` in the gem actually loads on its target Ruby. The container must remain glibc-compatible; `alpine` variants won't work because the gem was built against glibc.

## Typical release flow

For a one-shot clean-build-smoke-publish to Cassette:

```sh
export CASSETTE_API_KEY=...   # from 1Password / cassette admin
just gem-publish
```

`just gem-publish` chains `gem-clean` → `gem-build` (all platforms) → `gem-smoke` (prod-equivalent roundtrip) → `gem push` for each produced `.gem`. Any failure in the chain aborts before anything hits Cassette. The `CASSETTE_API_KEY` check runs *first* so forgetting to export it doesn't cost you a 5-minute build.

**Skip mode** — push existing gems in `ruby_magnus_2/pkg/` without rebuilding:

```sh
just gem-publish skip
```

Useful for re-pushing after a transient Cassette failure, or when you've already verified a build in a prior session. The API key check still runs; clean/build/smoke do not.

To manually walk the same stages (e.g. when iterating on the build itself):

```sh
just gem-clean          # start from a clean slate
just gem-build          # produce both platforms
just gem-smoke          # verify the x86_64 gem loads and roundtrips
# gem push each .gem in ruby_magnus_2/pkg/ manually
```

Two artifacts when green:

```
ruby_magnus_2/pkg/ruby_tracklib-<version>-x86_64-linux.gem
ruby_magnus_2/pkg/ruby_tracklib-<version>-aarch64-linux.gem
```

### Publishing internals

`gem-publish` is a shell script (`scripts/gem-publish.sh`), not an inline justfile recipe, so the chaining is done by invoking `just gem-clean`, `just gem-build`, `just gem-smoke` from inside the script. This means any changes to those underlying recipes automatically flow into publish — no duplication.

| Env var | Default | Purpose |
|---|---|---|
| `CASSETTE_API_KEY` | (required) | API token for `gem push`. Fails fast if unset. |
| `CASSETTE_HOST` | `https://cassette.ridewithgps.com/rubygems` | Override if pointing at a staging Cassette or a different registry. |

## Ruby ABI loader convention

The gem ships one `.so` per supported Ruby minor version, staged under `lib/ruby_tracklib/<X.Y>/`. `lib/ruby_tracklib.rb` picks the right one at require time:

```ruby
begin
  ruby_abi = RUBY_VERSION.match(/\d+\.\d+/)[0]
  require_relative "ruby_tracklib/#{ruby_abi}/ruby_tracklib"
rescue LoadError
  require_relative "ruby_tracklib/ruby_tracklib"  # source-built fallback
end
```

This is the standard rake-compiler / rb_sys pattern (nokogiri, sqlite3, ffi all do the same). Keys on MAJOR.MINOR because Ruby's C ABI is guaranteed stable across patch ("teeny") versions but **not** across minor versions.

## Consumption pattern (for main RWGPS app)

The current `ridewithgps` Gemfile pins this gem via git ref:

```ruby
gem "ruby_tracklib",
    git: "https://github.com/ridewithgps/tracklib",
    branch: "magnus",
    glob: "ruby_magnus_2/*.gemspec",
    ref: "<commit>"
```

In this mode, Bundler clones the repo and runs `extconf.rb` at install time — no precompiled gem needed. Precompiled gems are for publishing / speeding up cold-start CI, not for replacing the git-ref path.
