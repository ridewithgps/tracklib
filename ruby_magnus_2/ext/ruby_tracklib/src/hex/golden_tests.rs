//! Golden file tests for hex building algorithm
//!
//! These tests read pre-generated test cases from JSON files and verify
//! that the Rust implementation produces the same output as the Ruby reference.

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::geometry::Point;
    use crate::hex::{build_via_interpolation, DirectionMode};
    use serde::Deserialize;
    use std::collections::HashSet;
    use std::fs;
    use std::path::Path;

    #[derive(Debug, Deserialize)]
    struct GoldenTest {
        meta: Meta,
        input: Input,
        output: Output,
    }

    #[derive(Debug, Deserialize)]
    struct Meta {
        test_id: String,
    }

    #[derive(Debug, Deserialize)]
    struct Input {
        track_points: Vec<TrackPoint>,
        resolution: u8,
        direction_mode: String,
    }

    #[derive(Debug, Deserialize)]
    struct TrackPoint {
        x: String,
        y: String,
    }

    #[derive(Debug, Deserialize)]
    struct Output {
        cells: Vec<Cell>,
    }

    #[derive(Debug, Deserialize)]
    struct Cell {
        packed: u64,
    }

    fn parse_direction_mode(s: &str) -> DirectionMode {
        match s {
            "none" => DirectionMode::None,
            "forward" => DirectionMode::Forward,
            "both" => DirectionMode::Both,
            _ => panic!("Unknown direction mode: {}", s),
        }
    }

    fn load_golden_test(path: &Path) -> GoldenTest {
        let content = fs::read_to_string(path).expect("Failed to read golden test file");
        serde_json::from_str(&content).expect("Failed to parse golden test JSON")
    }

    fn run_golden_test(test: &GoldenTest) -> Result<(), String> {
        let points: Vec<Point> = test
            .input
            .track_points
            .iter()
            .enumerate()
            .map(|(i, tp)| {
                let x: f64 = tp.x.parse().expect("Failed to parse x coordinate");
                let y: f64 = tp.y.parse().expect("Failed to parse y coordinate");
                Point::new(i, x, y, 0.0, 0.0, None, None)
            })
            .collect();

        let direction_mode = parse_direction_mode(&test.input.direction_mode);
        let resolution = test.input.resolution;

        let result = build_via_interpolation(&points, resolution, direction_mode)?;

        let expected: HashSet<u64> = test.output.cells.iter().map(|c| c.packed).collect();
        let actual: HashSet<u64> = result.iter().cloned().collect();

        if expected != actual {
            let missing: Vec<_> = expected.difference(&actual).collect();
            let extra: Vec<_> = actual.difference(&expected).collect();

            return Err(format!(
                "Mismatch in {}: expected {} cells, got {}. Missing: {:?}, Extra: {:?}",
                test.meta.test_id,
                expected.len(),
                actual.len(),
                missing,
                extra
            ));
        }

        Ok(())
    }

    macro_rules! golden_test {
        ($name:ident, $file:expr) => {
            #[test]
            fn $name() {
                let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("test_data/golden_hexes")
                    .join($file);
                let test = load_golden_test(&path);
                run_golden_test(&test).expect("Golden test failed");
            }
        };
    }

    // A: Edge cases - empty, single point, minimal inputs
    golden_test!(A1_empty_array, "A1_empty_array.json");
    golden_test!(A2_single_point, "A2_single_point.json");
    golden_test!(A3_single_point_forward, "A3_single_point_forward.json");
    golden_test!(A4_single_point_both, "A4_single_point_both.json");
    golden_test!(A5_two_identical, "A5_two_identical.json");
    golden_test!(A6_very_close, "A6_very_close.json");

    // B: Direction tests - cardinal directions and patterns
    golden_test!(B1_pure_north, "B1_pure_north.json");
    golden_test!(B2_pure_east, "B2_pure_east.json");
    golden_test!(B3_pure_south, "B3_pure_south.json");
    golden_test!(B4_pure_west, "B4_pure_west.json");
    golden_test!(B5_northeast, "B5_northeast.json");
    golden_test!(B6_l_shape, "B6_l_shape.json");
    golden_test!(B7_out_back_forward, "B7_out_back_forward.json");
    golden_test!(B8_out_back_both, "B8_out_back_both.json");
    golden_test!(B9_all_directions, "B9_all_directions.json");

    // C: Gap detection tests
    golden_test!(C1_small_gap, "C1_small_gap.json");
    golden_test!(C2_medium_gap, "C2_medium_gap.json");
    golden_test!(C3_large_gap, "C3_large_gap.json");
    golden_test!(C4_gap_with_directions, "C4_gap_with_directions.json");

    // D: Resolution tests
    golden_test!(D1_res_0, "D1_res_0.json");
    golden_test!(D2_res_5, "D2_res_5.json");
    golden_test!(D3_res_9, "D3_res_9.json");
    golden_test!(D4_res_10, "D4_res_10.json");
    golden_test!(D5_res_11, "D5_res_11.json");
    golden_test!(D6_res_11_directions, "D6_res_11_directions.json");

    // E: Geographic edge cases
    golden_test!(E1_null_island, "E1_null_island.json");
    golden_test!(E2_near_north_pole, "E2_near_north_pole.json");
    golden_test!(E3_near_south_pole, "E3_near_south_pole.json");
    golden_test!(E4_antimeridian, "E4_antimeridian.json");

    // F: Complex patterns - spirals, dense sampling
    golden_test!(F1_spiral, "F1_spiral.json");
    golden_test!(F2_dense_sampling, "F2_dense_sampling.json");
    golden_test!(F2_tight_spiral, "F2_tight_spiral.json");
    golden_test!(F3_loose_spiral, "F3_loose_spiral.json");
    golden_test!(F4_spiral_both_directions, "F4_spiral_both_directions.json");
    golden_test!(F5_dense_sampling, "F5_dense_sampling.json");

    // G: Large tracks - performance and scale
    golden_test!(G1_long_track_1k, "G1_long_track_1k.json");
    golden_test!(G2_long_track_5k, "G2_long_track_5k.json");
    golden_test!(G3_long_track_10k, "G3_long_track_10k.json");
    golden_test!(G4_super_dense, "G4_super_dense.json");
    golden_test!(G5_pathological_gaps, "G5_pathological_gaps.json");

    // H: Direction edge cases - zigzags, boundaries
    golden_test!(H1_zigzag, "H1_zigzag.json");
    golden_test!(H2_zigzag_both, "H2_zigzag_both.json");
    golden_test!(H3_bearing_boundaries, "H3_bearing_boundaries.json");
    golden_test!(H4_all_directions_both, "H4_all_directions_both.json");
}
