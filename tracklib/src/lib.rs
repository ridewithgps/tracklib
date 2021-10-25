mod section;
mod rwtfile;
mod utils;
mod decode;
mod metadata;
mod flagscolumn;
pub mod simplification;

pub use rwtfile::{RWTFMAGIC, RWTFile, DataField};
pub use metadata::{RWTFMetadata, TrackType};
pub use section::{Column, SectionType, Section};
pub use decode::{parse_rwtf};
