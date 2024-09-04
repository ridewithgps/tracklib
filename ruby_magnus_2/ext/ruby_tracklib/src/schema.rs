use magnus::{r_hash::ForEach, wrap, Error, RArray, RHash, Ruby, TryConvert, Value};
use std::collections::HashSet;

#[wrap(class = "Tracklib::Schema", size)]
pub struct Schema {
    schema: tracklib::schema::Schema,
}

impl Schema {
    pub fn create(handle: &Ruby, data: RArray) -> Result<Self, Error> {
        let fields = data
            .into_iter()
            .map(|entry| {
                let field_def_array: RArray = TryConvert::try_convert(entry)?;

                let field_name: String = field_def_array.entry(0)?;
                let field_type: magnus::symbol::Symbol = field_def_array.entry(1)?;

                let data_type = match core::ops::Deref::deref(&field_type.name()?) {
                    "i64" => tracklib::schema::DataType::I64,
                    "f64" => {
                        let scale: u8 = field_def_array.entry(2)?;
                        tracklib::schema::DataType::F64 { scale }
                    }
                    "u64" => tracklib::schema::DataType::U64,
                    "bool" => tracklib::schema::DataType::Bool,
                    "string" => tracklib::schema::DataType::String,
                    "bool_array" => tracklib::schema::DataType::BoolArray,
                    "u64_array" => tracklib::schema::DataType::U64Array,
                    "byte_array" => tracklib::schema::DataType::ByteArray,
                    val => {
                        return Err(Error::new(
                            handle.exception_arg_error(),
                            format!("Schema Data Type '{val}' unknown"),
                        ));
                    }
                };

                Ok(tracklib::schema::FieldDefinition::new(field_name, data_type))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(Self {
            schema: tracklib::schema::Schema::with_fields(fields),
        })
    }

    pub fn trim(&self, handle: &Ruby, data: RArray) -> Result<tracklib::schema::Schema, Error> {
        // Find all the fields used in all rows of the data
        let mut keys = HashSet::new();
        let checked_data = data.typecheck::<RHash>()?;
        for row in checked_data {
            row.foreach(|k: String, _: Value| {
                keys.insert(k);
                Ok(ForEach::Continue)
            })?;
        }

        // subset original schema
        let fields = self
            .schema
            .fields()
            .iter()
            .filter_map(|field_def| {
                if keys.contains(field_def.name()) {
                    Some(field_def.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if fields.len() != keys.len() {
            Err(Error::new(handle.exception_exception(), "Schema is missing field(s)"))
        } else {
            Ok(tracklib::schema::Schema::with_fields(fields))
        }
    }
}
