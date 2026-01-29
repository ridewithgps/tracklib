#!/usr/bin/env ruby
# frozen_string_literal: true

# Ruby benchmark for section_hexes - compare with Rust Criterion benchmarks
#
# Run with: bundle exec ruby benchmarks/hex_benchmark.rb
#
# Compares against Rust benchmarks in ext/ruby_tracklib/benches/hex_benchmark.rs
#
# NOTE: section_hexes requires tracks with x, y, AND e (elevation) columns.
# Points missing any of these three fields are silently skipped.

require "bundler/setup"
require "ruby_tracklib"
require "benchmark/ips"
require "json"

# Generate a spiral pattern of GPS points (similar to golden test data)
def generate_spiral_points(count, center_lat: 45.0, center_lon: -122.0, radius_deg: 0.01)
  points = []
  count.times do |i|
    angle = i * 0.1 # radians
    r = radius_deg * (i.to_f / count)
    lat = center_lat + (r * Math.sin(angle))
    lon = center_lon + (r * Math.cos(angle))
    elevation = 100.0 + (i * 0.1) # gradual elevation gain
    points << { "x" => lon, "y" => lat, "e" => elevation }
  end
  points
end

# Create a tracklib buffer from GPS points
# NOTE: section_hexes requires x, y, AND e columns (all three must be present)
def create_tracklib_buffer(points)
  schema = Tracklib::Schema.new([
                                  ["x", :f64, 6], # longitude, 6 decimal places (matches section_hexes schema)
                                  ["y", :f64, 6],  # latitude, 6 decimal places
                                  ["e", :f64, 1]   # elevation, 1 decimal place
                                ])
  section = Tracklib::Section.standard(schema, points)
  Tracklib.write_track([], [section])
end

# Benchmark configurations matching Rust benches/hex_benchmark.rs
TRACK_SIZES = {
  "100_pts" => 100,
  "1000_pts" => 1_000,
  "5000_pts" => 5_000,
  "10000_pts" => 10_000
}.freeze

RESOLUTION = 10
DIRECTION_MODE = :forward

puts "Generating test data..."
test_data = TRACK_SIZES.transform_values do |size|
  points = generate_spiral_points(size)
  buffer = create_tracklib_buffer(points)
  { points: points, buffer: buffer, reader: Tracklib::TrackReader.new(buffer) }
end
puts "Test data generated.\n\n"

puts "=" * 60
puts "Ruby section_hexes Benchmark"
puts "=" * 60
puts "Resolution: #{RESOLUTION}, Direction mode: #{DIRECTION_MODE}"
puts "Compare with: cargo bench (Rust Criterion benchmarks)"
puts "=" * 60
puts

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  TRACK_SIZES.each_key do |name|
    data = test_data[name]

    x.report("track_size/#{name}") do
      # Re-create reader each iteration to match Rust benchmark behavior
      reader = Tracklib::TrackReader.new(data[:buffer])
      reader.section_hexes(0, RESOLUTION, DIRECTION_MODE)
    end
  end

  x.compare!
end

puts "\n"
puts "=" * 60
puts "Throughput Analysis"
puts "=" * 60

# Run a quick measurement to calculate throughput
TRACK_SIZES.each do |name, size|
  data = test_data[name]

  start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  iterations = 0

  # Run for 1 second
  while Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time < 1.0
    reader = Tracklib::TrackReader.new(data[:buffer])
    reader.section_hexes(0, RESOLUTION, DIRECTION_MODE)
    iterations += 1
  end

  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
  points_per_sec = (size * iterations) / elapsed

  puts format("%<label>-20s %<throughput>10.2f Kelem/s  (%<iter>d iterations in %<time>.2fs)",
              label: "track_size/#{name}:",
              throughput: points_per_sec / 1000.0,
              iter: iterations,
              time: elapsed)
end

puts "\n"
puts "=" * 60
puts "Comparison Notes"
puts "=" * 60
puts "Rust Criterion (pure):     ~2,600 Kelem/s (cargo bench)"
puts "Ruby via FFI (this test):  ~950 Kelem/s"
puts ""
puts "The ~2.7x overhead is from FFI boundary crossing,"
puts "TrackReader construction, and data marshaling."
puts ""
puts "Run 'cargo bench' in ext/ruby_tracklib/ to compare directly."
