mod section;
mod rwtfile;
mod utils;
mod decode;
mod metadata;
mod flagscolumn;
mod surface;
mod polyline;
mod simplify;

pub use rwtfile::{RWTFMAGIC, RWTFile, DataField};
pub use metadata::{RWTFMetadata, TrackType};
pub use section::{Column, SectionType, Section};
pub use decode::{parse_rwtf};
pub use polyline::{FieldEncodeOptions, PointField};
pub use surface::{RoadClassMapping, SurfaceMapping};
