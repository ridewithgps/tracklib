# Tracklib monorepo justfile
# Run `just --list` to see all available commands

default:
    @just --list

# ─────────────────────────────────────────────────────────────────────────────
# Core Rust (tracklib crate)
# ─────────────────────────────────────────────────────────────────────────────

# Run core Rust tests
rust-test:
    cargo test --manifest-path tracklib/Cargo.toml

# Format core Rust code
rust-fmt:
    cargo fmt --manifest-path tracklib/Cargo.toml

# Lint core Rust code
rust-lint:
    cargo clippy --manifest-path tracklib/Cargo.toml -- -D warnings

# ─────────────────────────────────────────────────────────────────────────────
# Ruby Gem (ruby_magnus_2 - includes Rust extension)
# ─────────────────────────────────────────────────────────────────────────────

# Run all Ruby tests (Rust extension tests + RSpec)
ruby-test:
    cargo test --manifest-path ruby_magnus_2/ext/ruby_tracklib/Cargo.toml
    cd ruby_magnus_2 && bundle exec rake compile && bundle exec rspec

# Run Ruby benchmarks (Rust Criterion + Ruby benchmark-ips)
ruby-bench:
    cargo bench --manifest-path ruby_magnus_2/ext/ruby_tracklib/Cargo.toml
    cd ruby_magnus_2 && bundle exec rake compile && bundle exec ruby benchmarks/hex_benchmark.rb

# Format all Ruby code (Rust + Ruby)
ruby-fmt:
    cargo fmt --manifest-path ruby_magnus_2/ext/ruby_tracklib/Cargo.toml
    cd ruby_magnus_2 && bundle exec rubocop -a

# Lint all Ruby code (Rust clippy + RuboCop)
ruby-lint:
    cargo clippy --manifest-path ruby_magnus_2/ext/ruby_tracklib/Cargo.toml -- -D warnings
    cd ruby_magnus_2 && bundle exec rubocop

# Compile Ruby native extension
ruby-compile:
    cd ruby_magnus_2 && bundle exec rake compile

# Install Ruby dependencies
ruby-install:
    cd ruby_magnus_2 && bundle install

# ─────────────────────────────────────────────────────────────────────────────
# All / CI
# ─────────────────────────────────────────────────────────────────────────────

# Run all tests
test: rust-test ruby-test

# Format all code
fmt: rust-fmt ruby-fmt

# Lint all code
lint: rust-lint ruby-lint

# CI checks (format check + lint + test)
ci:
    cargo fmt --manifest-path tracklib/Cargo.toml --check
    cargo fmt --manifest-path ruby_magnus_2/ext/ruby_tracklib/Cargo.toml --check
    just lint
    just test
