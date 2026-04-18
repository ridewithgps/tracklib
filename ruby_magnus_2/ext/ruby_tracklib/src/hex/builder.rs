use super::constants::*;
use super::geo_utils::*;
use super::DirectionMode;
use crate::geometry::Point;
use h3o::{CellIndex, LatLng, Resolution};
use std::collections::HashMap;

/// Build H3 indexes from points using linear interpolation
/// Matches Ruby: H3Helpers::HexSetBuilder#build_via_interpolation
///
/// # Arguments
/// * `points` - Slice of Point structs with x (lng) and y (lat)
/// * `resolution` - H3 resolution 0-15 (max 11 if direction encoding)
/// * `direction_mode` - None, Forward, Backward, or Both
///
/// # Returns
/// * `Ok(Vec<u64>)` - H3 cell indices (packed with direction if enabled)
/// * `Err(String)` - Error message if invalid parameters
pub fn build_via_interpolation(
    points: &[Point],
    resolution: u8,
    direction_mode: DirectionMode,
) -> Result<Vec<u64>, String> {
    // Validate resolution
    if resolution > 15 {
        return Err(format!("Resolution must be 0-15, got {}", resolution));
    }

    let track_dirs = direction_mode.tracks_directions();
    if track_dirs && resolution > MAX_DIRECTION_RESOLUTION {
        return Err(format!(
            "Resolution must be <= {} when using direction encoding, got {}",
            MAX_DIRECTION_RESOLUTION, resolution
        ));
    }

    let res = Resolution::try_from(resolution).map_err(|_| format!("Invalid H3 resolution: {}", resolution))?;

    // Filter to valid points
    let valid_points: Vec<&Point> = points.iter().filter(|p| is_valid_point(p)).collect();

    // Edge case: no valid points
    if valid_points.is_empty() {
        return Ok(Vec::new());
    }

    // Edge case: single point
    if valid_points.len() == 1 {
        let p = valid_points[0];
        let cell = point_to_cell(p, res)?;
        let cell_u64 = u64::from(cell);

        let packed = if track_dirs {
            pack_direction(cell_u64, ALL_DIRECTIONS)
        } else {
            cell_u64
        };
        return Ok(vec![packed]);
    }

    // Main algorithm: interpolate between consecutive points
    let edge_m = EDGE_LENGTH_M[resolution as usize];
    let step_m = edge_m * STEP_FRACTION;
    let skip_threshold_m = edge_m * CELL_SKIP_FRACTION;

    // Accumulate cell => direction_mask
    let mut cell_masks: HashMap<u64, u16> = HashMap::new();

    // Optimization state: track current cell to skip redundant H3 calls
    let mut current_cell: Option<CellIndex> = None;
    let mut current_center: Option<(f64, f64)> = None;

    // Process consecutive point pairs
    for window in valid_points.windows(2) {
        let (p1, p2) = (window[0], window[1]);
        let (lat1, lng1) = (p1.y(), p1.x()); // Point: y=lat, x=lng
        let (lat2, lng2) = (p2.y(), p2.x());

        let dist = fast_distance_m(lat1, lng1, lat2, lng2);

        // Gap detection: segment too long indicates GPS gap
        if dist > GAP_THRESHOLD_M {
            // Record the starting point with all directions
            if let Ok(cell) = latlng_to_cell(lat1, lng1, res) {
                let cell_u64 = u64::from(cell);
                if track_dirs {
                    *cell_masks.entry(cell_u64).or_insert(0) |= ALL_DIRECTIONS;
                } else {
                    cell_masks.entry(cell_u64).or_insert(0);
                }
            }
            // Reset optimization state
            current_cell = None;
            current_center = None;
            continue;
        }

        // Compute direction masks for this segment
        let (fwd_mask, back_mask) = if track_dirs {
            let deg = bearing_deg(lat1, lng1, lat2, lng2);
            let fwd = direction_bitmask(deg);
            let back = direction_bitmask((deg + 180.0) % 360.0);
            (fwd, back)
        } else {
            (0, 0)
        };

        // Calculate interpolation steps
        let steps = (dist / step_m).ceil().max(1.0) as usize;

        // Track previous cell within this segment for transition detection
        let mut segment_prev_cell: Option<CellIndex> = None;

        // Interpolate along segment (inclusive: 0 to steps)
        for j in 0..=steps {
            let t = j as f64 / steps as f64;
            let (lat, lng) = lerp_point(lat1, lng1, lat2, lng2, t);

            // Optimization: skip H3 call if still within current cell
            if let (Some(_), Some((center_lat, center_lng))) = (current_cell, current_center) {
                let d = fast_distance_m(lat, lng, center_lat, center_lng);
                if d < skip_threshold_m {
                    segment_prev_cell = current_cell;
                    continue;
                }
            }

            // Get H3 cell for this position
            let cell = match latlng_to_cell(lat, lng, res) {
                Ok(c) => c,
                Err(_) => continue, // Skip invalid coordinates
            };
            let cell_u64 = u64::from(cell);

            // Update optimization state
            current_cell = Some(cell);
            // h3o 0.9: Use From trait to get cell center
            let center: LatLng = cell.into();
            current_center = Some((center.lat(), center.lng()));

            // Direction tagging on cell transitions
            if track_dirs {
                if let Some(prev) = segment_prev_cell {
                    if cell != prev {
                        if direction_mode.writes_forward() {
                            let prev_u64 = u64::from(prev);
                            *cell_masks.entry(prev_u64).or_insert(0) |= fwd_mask;
                        }
                        if direction_mode.writes_backward() {
                            *cell_masks.entry(cell_u64).or_insert(0) |= back_mask;
                        }
                    }
                }
            }

            // Record this cell (ensures it's in the result even with mask=0)
            cell_masks.entry(cell_u64).or_insert(0);

            segment_prev_cell = Some(cell);
        }
    }

    // Ensure final point is recorded
    let last = valid_points.last().unwrap();
    if let Ok(cell) = latlng_to_cell(last.y(), last.x(), res) {
        cell_masks.entry(u64::from(cell)).or_insert(0);
    }

    // Convert to output format
    let mut result: Vec<u64> = if track_dirs {
        cell_masks
            .into_iter()
            .map(|(cell, mask)| pack_direction(cell, mask))
            .collect()
    } else {
        cell_masks.into_keys().collect()
    };

    // Sort for deterministic output — HashMap iteration order is randomized,
    // so callers that compare or cache hex arrays would otherwise see churn.
    // unstable is safe: packed values are unique (one per H3 cell).
    result.sort_unstable();

    Ok(result)
}

/// Check if a point has valid coordinates
fn is_valid_point(p: &Point) -> bool {
    let lat = p.y();
    let lng = p.x();

    // Reject Null Island (common GPS error)
    if lat == 0.0 && lng == 0.0 {
        return false;
    }

    // Check bounds
    (-180.0..=180.0).contains(&lng) && (-90.0..=90.0).contains(&lat)
}

/// Convert Point to H3 CellIndex
fn point_to_cell(p: &Point, res: Resolution) -> Result<CellIndex, String> {
    latlng_to_cell(p.y(), p.x(), res)
}

/// Convert lat/lng to H3 CellIndex
fn latlng_to_cell(lat: f64, lng: f64, res: Resolution) -> Result<CellIndex, String> {
    let latlng = LatLng::new(lat, lng).map_err(|e| format!("Invalid coordinates ({}, {}): {:?}", lat, lng, e))?;
    Ok(latlng.to_cell(res))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_point(lng: f64, lat: f64) -> Point {
        // Point::new(index, x, y, d, e, s, r)
        Point::new(0, lng, lat, 0.0, Some(0.0), None, None)
    }

    #[test]
    fn test_empty_input() {
        let points: Vec<Point> = vec![];
        let result = build_via_interpolation(&points, 11, DirectionMode::None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_point_no_direction() {
        let points = vec![make_point(-122.5, 45.5)];
        let result = build_via_interpolation(&points, 11, DirectionMode::None).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_single_point_with_direction() {
        let points = vec![make_point(-122.5, 45.5)];
        let result = build_via_interpolation(&points, 10, DirectionMode::Forward).unwrap();
        assert_eq!(result.len(), 1);
        // Should have ALL_DIRECTIONS mask
        assert_eq!(result[0] & DIR_MASK, ALL_DIRECTIONS as u64);
    }

    #[test]
    fn test_null_island_rejected() {
        let points = vec![make_point(0.0, 0.0)];
        let result = build_via_interpolation(&points, 11, DirectionMode::None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_resolution_validation() {
        let points = vec![make_point(-122.5, 45.5)];

        // Valid resolution
        assert!(build_via_interpolation(&points, 15, DirectionMode::None).is_ok());

        // Invalid resolution
        assert!(build_via_interpolation(&points, 16, DirectionMode::None).is_err());
    }

    #[test]
    fn test_direction_resolution_limit() {
        let points = vec![make_point(-122.5, 45.5)];

        // Resolution 11 with direction: OK
        assert!(build_via_interpolation(&points, 11, DirectionMode::Forward).is_ok());

        // Resolution 12 with direction: Error
        assert!(build_via_interpolation(&points, 12, DirectionMode::Forward).is_err());

        // Resolution 12 without direction: OK
        assert!(build_via_interpolation(&points, 12, DirectionMode::None).is_ok());
    }

    #[test]
    fn test_two_close_points() {
        // Two points ~100m apart at res 10 (edge ~66m) should produce multiple cells
        let points = vec![make_point(-122.5, 45.5), make_point(-122.499, 45.501)];
        let result = build_via_interpolation(&points, 10, DirectionMode::None).unwrap();
        assert!(result.len() >= 2, "Expected at least 2 cells, got {}", result.len());
    }

    #[test]
    fn test_two_identical_points() {
        let points = vec![make_point(-122.5, 45.5), make_point(-122.5, 45.5)];
        let result = build_via_interpolation(&points, 10, DirectionMode::None).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_gap_detection() {
        // Two points 5km apart (> GAP_THRESHOLD_M of 2km)
        let points = vec![
            make_point(-122.5, 45.5),
            make_point(-122.5, 45.55), // ~5.5km north
        ];
        let result = build_via_interpolation(&points, 10, DirectionMode::Forward).unwrap();
        // Gap should result in first point with ALL_DIRECTIONS
        // Both points should be in result
        assert!(result.len() >= 1);
    }

    #[test]
    fn test_output_sorted() {
        // Dense zig-zag to produce many cells
        let mut points = Vec::new();
        for i in 0..100 {
            let d = i as f64 * 0.0005;
            let y_off = if i % 2 == 0 { 0.0 } else { 0.0003 };
            points.push(make_point(-122.5 + d, 45.5 + y_off));
        }
        let result = build_via_interpolation(&points, 10, DirectionMode::Both).unwrap();
        assert!(result.len() > 2, "need multiple cells to test sort");
        assert!(
            result.windows(2).all(|w| w[0] < w[1]),
            "output must be sorted ascending"
        );
    }

    #[test]
    fn test_direction_forward_mode() {
        // Line going north - should have north direction bit set on transition cells
        let points = vec![
            make_point(-122.5, 45.5),
            make_point(-122.5, 45.502), // ~220m north
        ];
        let result = build_via_interpolation(&points, 10, DirectionMode::Forward).unwrap();
        assert!(!result.is_empty());
        // All cells should have direction mask (lower 12 bits not all zero)
        // Note: not all cells need direction if they're not transition cells
    }
}
