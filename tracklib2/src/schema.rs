#[derive(Debug)]
pub struct Schema {
    fields: Vec<FieldDefinition>,
}

impl Schema {
    pub fn with_fields(fields: Vec<FieldDefinition>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &[FieldDefinition] {
        &self.fields
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct FieldDefinition {
    name: String,
    data_type: DataType,
}

impl FieldDefinition {
    pub fn new<S: Into<String>>(name: S, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[derive(Debug)]
pub enum BitstreamType {
    Bytes,
    I64,
    Bool,
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum DataType {
    I64,
    Bool,
    String,
    F64,
}

impl DataType {
    pub fn bitstream_type(&self) -> BitstreamType {
        match self {
            Self::I64 => BitstreamType::I64,
            Self::Bool => BitstreamType::Bool,
            Self::String => BitstreamType::Bytes,
            Self::F64 => BitstreamType::I64,
        }
    }
}
