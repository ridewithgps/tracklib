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

impl FieldValue {
    pub fn into_i64(self) -> Option<i64> {
        if let Self::I64(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn into_u64(self) -> Option<u64> {
        if let Self::U64(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn into_f64(self) -> Option<f64> {
        if let Self::F64(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn into_bool(self) -> Option<bool> {
        if let Self::Bool(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn into_string(self) -> Option<String> {
        if let Self::String(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn into_bool_array(self) -> Option<Vec<bool>> {
        if let Self::BoolArray(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn into_u64_array(self) -> Option<Vec<u64>> {
        if let Self::U64Array(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn into_byte_array(self) -> Option<Vec<u8>> {
        if let Self::ByteArray(v) = self {
            Some(v)
        } else {
            None
        }
    }
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
