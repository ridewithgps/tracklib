#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum FieldValue {
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    String(String),
    BoolArray(Vec<bool>),
    U64Array(Vec<u64>),
    ByteArray(Vec<u8>),
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum TrackType {
    Trip(u32),
    Route(u32),
    Segment(u32),
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum MetadataEntry {
    TrackType(TrackType),
    CreatedAt(u64),
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum SectionEncoding {
    Standard,
    // Encrypted,
}
