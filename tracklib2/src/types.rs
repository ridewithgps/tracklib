#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub enum TrackType {
    Trip(u64),
    Route(u64),
    Segment(u64),
}

#[derive(Debug, PartialEq)]
pub enum MetadataEntry {
    TrackType(TrackType),
    CreatedAt(u64),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SectionEncoding {
    Standard,
    Encrypted,
}
