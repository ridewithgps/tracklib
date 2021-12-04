#[derive(Debug, Clone)]
pub enum FieldType {
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
