//! # Hex Building Module
//!
//! Converts GPS track points to H3 hexagonal cell indices using linear interpolation.
//! This module provides the core algorithm for the `section_hexes` Ruby API.
//!
//! ## Ruby API
//!
//! ```ruby
//! reader.section_hexes(section_index, resolution, direction_mode, [key_material])
//! ```
//!
//! ### Parameters
//! - `section_index` - Integer, which section (0-based)
//! - `resolution` - Integer 0-15 (max 11 with direction encoding)
//! - `direction_mode` - Symbol `:none`, `:forward`, `:backward`, or `:both`
//! - `key_material` - String (required for encrypted sections only)
//!
//! ### Returns
//! `Array<Integer>` - H3 cell indices, optionally packed with direction bits
//!
//! ### Errors
//! - `IndexError` - section doesn't exist
//! - `ArgumentError` - invalid resolution, direction_mode, or missing key_material
//!
//! ## Direction Encoding
//!
//! When `direction_mode` is `:forward` or `:both`, the lower 12 bits of each
//! returned integer contain a direction bitmask:
//!
//! - Bits 0-11 represent 12 compass directions (30° buckets)
//! - Bit 0 = North (345°-15°), Bit 3 = East (75°-105°), etc.
//! - Upper 52 bits contain the H3 cell index
//!
//! Use `value & 0xFFFFFFFFFFFFF000` to extract the H3 cell.
//! Use `value & 0x0FFF` to extract the direction mask.
//!
//! ## Known Limitations
//!
//! 1. **Antimeridian crossing** - Tracks crossing ±180° longitude will
//!    interpolate "the long way around" the globe. This affects a very
//!    small number of real-world tracks.
//!
//! 2. **Polar regions** - The equirectangular distance approximation is
//!    less accurate above ~85° latitude. H3 cells are also irregular at poles.
//!
//! 3. **Direction precision** - Direction is computed per GPS segment, not
//!    per individual H3 cell. For very long segments, intermediate cells
//!    may have slightly imprecise direction encoding.
//!
//! ## Performance
//!
//! Benchmarked at ~2.6 million points/second on typical hardware:
//! - 1,000 points: ~350 µs
//! - 10,000 points: ~3.8 ms

#![allow(dead_code)]
#![allow(unused_imports)]

pub mod builder;
pub mod constants;
pub mod geo_utils;
#[cfg(test)]
mod golden_tests;

pub use builder::build_via_interpolation;
pub use constants::*;

/// Direction encoding mode for hex building
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectionMode {
    /// No direction encoding - return raw H3 cell indices
    None,
    /// Encode forward direction only (direction of travel)
    Forward,
    /// Encode backward direction only (reverse of travel)
    Backward,
    /// Encode both forward and backward directions (bidirectional)
    Both,
}

impl DirectionMode {
    /// Returns true if direction tracking is enabled
    pub fn tracks_directions(&self) -> bool {
        *self != DirectionMode::None
    }

    /// Returns true if forward direction is encoded
    pub fn writes_forward(&self) -> bool {
        matches!(self, DirectionMode::Forward | DirectionMode::Both)
    }

    /// Returns true if backward direction is encoded
    pub fn writes_backward(&self) -> bool {
        matches!(self, DirectionMode::Backward | DirectionMode::Both)
    }
}
