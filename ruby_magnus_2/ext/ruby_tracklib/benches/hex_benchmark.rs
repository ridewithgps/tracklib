//! Benchmarks for the hex building algorithm
//!
//! Run with: cargo bench
//! Results appear in target/criterion/

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ruby_tracklib::geometry::Point;
use ruby_tracklib::hex::{build_via_interpolation, DirectionMode};
use serde::Deserialize;
use std::fs;
use std::hint::black_box;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct GoldenTest {
    input: Input,
}

#[derive(Debug, Deserialize)]
struct Input {
    track_points: Vec<TrackPoint>,
}

#[derive(Debug, Deserialize)]
struct TrackPoint {
    x: String,
    y: String,
}

fn load_points_from_golden(filename: &str) -> Vec<Point> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("test_data/golden_hexes")
        .join(filename);
    let content = fs::read_to_string(&path).expect("Failed to read golden test file");
    let test: GoldenTest = serde_json::from_str(&content).expect("Failed to parse golden test JSON");

    test.input
        .track_points
        .iter()
        .enumerate()
        .map(|(i, tp)| {
            let x: f64 = tp.x.parse().expect("Failed to parse x coordinate");
            let y: f64 = tp.y.parse().expect("Failed to parse y coordinate");
            Point::new(i, x, y, 0.0, 0.0, None, None)
        })
        .collect()
}

fn bench_by_track_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("track_size");

    let test_cases = [
        ("100_pts", "F5_dense_sampling.json"),
        ("1000_pts", "G1_long_track_1k.json"),
        ("5000_pts", "G2_long_track_5k.json"),
        ("10000_pts", "G3_long_track_10k.json"),
    ];

    for (name, filename) in test_cases.iter() {
        let points = load_points_from_golden(filename);
        let point_count = points.len();

        group.throughput(Throughput::Elements(point_count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &points, |b, pts| {
            b.iter(|| build_via_interpolation(black_box(pts), 10, DirectionMode::Forward))
        });
    }

    group.finish();
}

criterion_group!(benches, bench_by_track_size);
criterion_main!(benches);
