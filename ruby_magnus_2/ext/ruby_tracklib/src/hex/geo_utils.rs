use super::constants::{DIR_MASK, GEO_MASK};

const EARTH_RADIUS_M: f64 = 6_371_000.0;
const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;
const RAD_TO_DEG: f64 = 180.0 / std::f64::consts::PI;

/// Fast distance approximation using equirectangular projection.
/// Accurate enough for short distances (< 100km).
pub fn fast_distance_m(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let avg_lat = (lat1 + lat2) * 0.5 * DEG_TO_RAD;
    let dx = (lng2 - lng1) * DEG_TO_RAD * avg_lat.cos();
    let dy = (lat2 - lat1) * DEG_TO_RAD;
    (dx * dx + dy * dy).sqrt() * EARTH_RADIUS_M
}

/// Linear interpolation between two points.
/// Returns (lat, lng) at position t where t=0 is start, t=1 is end.
/// LIMITATION: Does NOT handle antimeridian crossing correctly.
pub fn lerp_point(lat1: f64, lng1: f64, lat2: f64, lng2: f64, t: f64) -> (f64, f64) {
    let lat = lat1 + (lat2 - lat1) * t;
    let lng = lng1 + (lng2 - lng1) * t;
    (lat, lng)
}

/// Calculate bearing in degrees from point 1 to point 2.
/// Returns value in range [0, 360).
pub fn bearing_deg(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let lat1_rad = lat1 * DEG_TO_RAD;
    let lat2_rad = lat2 * DEG_TO_RAD;
    let d_lng = (lng2 - lng1) * DEG_TO_RAD;

    // CRITICAL: The +360 before modulo handles negative atan2 results
    let y_component = d_lng.sin() * lat2_rad.cos();
    let x_component = lat1_rad.cos() * lat2_rad.sin() - lat1_rad.sin() * lat2_rad.cos() * d_lng.cos();

    (y_component.atan2(x_component) * RAD_TO_DEG + 360.0) % 360.0
}

/// Convert bearing to one of 12 direction buckets (0-11).
/// Each bucket covers 30 degrees, centered on N, NNE, NE, etc.
/// Bucket 0 = North (345-15°), Bucket 1 = NNE (15-45°), etc.
pub fn direction_bucket(bearing_degrees: f64) -> u8 {
    (((bearing_degrees + 15.0) % 360.0) / 30.0).floor() as u8
}

/// Convert bearing to a bitmask with single bit set for the direction bucket.
pub fn direction_bitmask(bearing_degrees: f64) -> u16 {
    1 << direction_bucket(bearing_degrees)
}

/// Pack a direction mask into an H3 cell index.
/// Lower 12 bits store direction mask, upper 52 bits store geometry.
pub fn pack_direction(cell: u64, mask: u16) -> u64 {
    (cell & GEO_MASK) | (mask as u64 & DIR_MASK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bearing_north() {
        // Moving due north from (45.5, -122.5) to (46.5, -122.5)
        let bearing = bearing_deg(45.5, -122.5, 46.5, -122.5);
        assert!(
            (bearing - 0.0).abs() < 1.0,
            "North bearing should be ~0°, got {}",
            bearing
        );
    }

    #[test]
    fn test_bearing_east() {
        // Moving due east
        let bearing = bearing_deg(45.5, -122.5, 45.5, -121.5);
        assert!(
            (bearing - 90.0).abs() < 1.0,
            "East bearing should be ~90°, got {}",
            bearing
        );
    }

    #[test]
    fn test_bearing_south() {
        // Moving due south
        let bearing = bearing_deg(45.5, -122.5, 44.5, -122.5);
        assert!(
            (bearing - 180.0).abs() < 1.0,
            "South bearing should be ~180°, got {}",
            bearing
        );
    }

    #[test]
    fn test_bearing_west() {
        // Moving due west
        let bearing = bearing_deg(45.5, -122.5, 45.5, -123.5);
        assert!(
            (bearing - 270.0).abs() < 1.0,
            "West bearing should be ~270°, got {}",
            bearing
        );
    }

    #[test]
    fn test_direction_bucket_north() {
        // North (0°) should be bucket 0
        assert_eq!(direction_bucket(0.0), 0);
        // Just after boundary at 15°
        assert_eq!(direction_bucket(16.0), 1);
        // Just before boundary at 15°
        assert_eq!(direction_bucket(14.0), 0);
    }

    #[test]
    fn test_direction_bucket_east() {
        // East (90°) should be bucket 3
        assert_eq!(direction_bucket(90.0), 3);
    }

    #[test]
    fn test_direction_bucket_all() {
        // Test all 12 buckets at their centers
        assert_eq!(direction_bucket(0.0), 0); // N
        assert_eq!(direction_bucket(30.0), 1); // NNE
        assert_eq!(direction_bucket(60.0), 2); // ENE
        assert_eq!(direction_bucket(90.0), 3); // E
        assert_eq!(direction_bucket(120.0), 4); // ESE
        assert_eq!(direction_bucket(150.0), 5); // SSE
        assert_eq!(direction_bucket(180.0), 6); // S
        assert_eq!(direction_bucket(210.0), 7); // SSW
        assert_eq!(direction_bucket(240.0), 8); // WSW
        assert_eq!(direction_bucket(270.0), 9); // W
        assert_eq!(direction_bucket(300.0), 10); // WNW
        assert_eq!(direction_bucket(330.0), 11); // NNW
    }

    #[test]
    fn test_direction_bitmask() {
        assert_eq!(direction_bitmask(0.0), 0b000000000001); // bucket 0 = bit 0
        assert_eq!(direction_bitmask(90.0), 0b000000001000); // bucket 3 = bit 3
        assert_eq!(direction_bitmask(180.0), 0b000001000000); // bucket 6 = bit 6
        assert_eq!(direction_bitmask(270.0), 0b001000000000); // bucket 9 = bit 9
    }

    #[test]
    fn test_fast_distance_approx() {
        // 1 degree of latitude is approximately 111 km
        let dist = fast_distance_m(45.0, -122.0, 46.0, -122.0);
        assert!(
            (dist - 111_000.0).abs() < 500.0,
            "1° lat should be ~111km, got {}m",
            dist
        );
    }

    #[test]
    fn test_fast_distance_short() {
        // Very short distance
        let dist = fast_distance_m(45.5, -122.5, 45.5001, -122.5001);
        assert!(dist < 20.0, "Very short distance should be <20m, got {}m", dist);
        assert!(dist > 5.0, "Very short distance should be >5m, got {}m", dist);
    }

    #[test]
    fn test_lerp_point_start() {
        let (lat, lng) = lerp_point(45.0, -122.0, 46.0, -121.0, 0.0);
        assert!((lat - 45.0).abs() < 1e-10);
        assert!((lng - (-122.0)).abs() < 1e-10);
    }

    #[test]
    fn test_lerp_point_end() {
        let (lat, lng) = lerp_point(45.0, -122.0, 46.0, -121.0, 1.0);
        assert!((lat - 46.0).abs() < 1e-10);
        assert!((lng - (-121.0)).abs() < 1e-10);
    }

    #[test]
    fn test_lerp_point_midpoint() {
        let (lat, lng) = lerp_point(45.0, -122.0, 46.0, -121.0, 0.5);
        assert!((lat - 45.5).abs() < 1e-10);
        assert!((lng - (-121.5)).abs() < 1e-10);
    }

    #[test]
    fn test_pack_direction() {
        let cell: u64 = 0x8a28f0181097fff;
        let mask: u16 = 0b000000001001; // bits 0 and 3 set

        let packed = pack_direction(cell, mask);

        // Upper bits should be preserved (with lower 12 masked off)
        assert_eq!(packed & GEO_MASK, cell & GEO_MASK);
        // Lower 12 bits should be the mask
        assert_eq!(packed & DIR_MASK, 0b000000001001);
    }

    #[test]
    fn test_pack_direction_zero_mask() {
        let cell: u64 = 0x8a28f0181097fff;
        let packed = pack_direction(cell, 0);
        assert_eq!(packed & DIR_MASK, 0);
        assert_eq!(packed & GEO_MASK, cell & GEO_MASK);
    }
}
