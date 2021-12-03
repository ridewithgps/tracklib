#[derive(Debug, Clone)]
pub(crate) enum FieldType {
    I64,
    Bool,
    String,
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
