#[derive(Debug, Clone)]
pub(crate) enum FieldType {
    I64,
    String,
    Bool,
}

impl FieldType {
    pub(crate) fn type_tag(&self) -> u8 {
        match self {
            Self::I64 => 0x00,
            Self::String => 0x04,
            Self::Bool => 0x05,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FieldDescription {
    name: String,
    fieldtype: FieldType,
}

impl FieldDescription {
    pub(crate) fn new(name: String, fieldtype: FieldType) -> Self {
        Self { name, fieldtype }
    }

    /// Get a reference to the field description's name.
    pub(crate) fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Get a reference to the field description's fieldtype.
    pub(crate) fn fieldtype(&self) -> &FieldType {
        &self.fieldtype
    }
}
