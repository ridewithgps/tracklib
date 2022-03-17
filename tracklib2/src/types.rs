#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub enum FieldType {
    I64,
    F64,
    String,
    Bool,
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct FieldDescription {
    name: String,
    fieldtype: FieldType,
}

impl FieldDescription {
    pub fn new(name: String, fieldtype: FieldType) -> Self {
        Self { name, fieldtype }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn fieldtype(&self) -> &FieldType {
        &self.fieldtype
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum FieldValue {
    I64(i64),
    F64(f64),
    Bool(bool),
    String(String),
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

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum SectionType {
    TrackPoints,
    CoursePoints,
}
