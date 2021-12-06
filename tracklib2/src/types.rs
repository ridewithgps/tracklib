#[derive(Debug, Clone)]
pub enum FieldType {
    I64,
    String,
    Bool,
}

#[derive(Clone, Debug)]
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

#[derive(Debug, PartialEq)]
pub enum TrackType {
    Trip(u32),
    Route(u32),
    Segment(u32),
}

#[derive(Debug, PartialEq)]
pub enum MetadataEntry {
    TrackType(TrackType),
    CreatedAt(u64),
}
