/// H3 edge lengths at each resolution in meters
/// Source: Must match Ruby H3Helpers::EDGE_LENGTH_M exactly (verified in tracklib-0lq)
#[allow(clippy::inconsistent_digit_grouping)]
pub const EDGE_LENGTH_M: [f64; 16] = [
    1_107_712.591, // res 0
    418_676.0055,  // res 1
    158_244.6558,  // res 2
    59_810.85794,  // res 3
    22_606.3794,   // res 4
    8_544.408276,  // res 5
    3_229.482772,  // res 6
    1_220.629759,  // res 7
    461.3540313,   // res 8
    174.3754569,   // res 9
    65.90780749,   // res 10
    24.9105614,    // res 11
    9.415526211,   // res 12
    3.559893033,   // res 13
    1.348574562,   // res 14
    0.509713273,   // res 15
];

/// Interpolation step as fraction of edge length
/// Sample every 25% of edge length for smooth coverage
pub const STEP_FRACTION: f64 = 0.25;

/// Gap threshold in meters
/// Gaps larger than 2km are detected and handled specially
pub const GAP_THRESHOLD_M: f64 = 2000.0;

/// Cell skip optimization threshold
/// Skip H3 lookup if within 85% of current cell center
pub const CELL_SKIP_FRACTION: f64 = 0.85;

/// Direction encoding: lower 12 bits for direction bitmask
pub const DIR_MASK: u64 = 0x0000_0000_0000_0FFF;

/// Direction encoding: upper 52 bits for H3 cell geometry
pub const GEO_MASK: u64 = 0xFFFF_FFFF_FFFF_F000;

/// All 12 direction bits set (used for single points, gaps)
pub const ALL_DIRECTIONS: u16 = 0x0FFF;

/// Maximum resolution when using direction encoding
/// At res 12+, lower bits conflict with H3 cell representation
pub const MAX_DIRECTION_RESOLUTION: u8 = 11;
