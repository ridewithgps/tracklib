use rutie::{Array, Class, Object, RString, Symbol, VM};

pub(crate) fn create_schema(ruby_schema: Array) -> tracklib2::schema::Schema {
    let fields = ruby_schema
        .into_iter()
        .map(|ele| {
            let ruby_schema_entry = ele
                .try_convert_to::<Array>()
                .map_err(|e| VM::raise_ex(e))
                .unwrap();

            if ruby_schema_entry.length() == 2 {
                let ruby_field_name = ruby_schema_entry
                    .at(0)
                    .try_convert_to::<RString>()
                    .map_err(|e| VM::raise_ex(e))
                    .unwrap();
                let ruby_data_type = ruby_schema_entry
                    .at(1)
                    .try_convert_to::<Symbol>()
                    .map_err(|e| VM::raise_ex(e))
                    .unwrap();

                let data_type = match ruby_data_type.to_str() {
                    "i64" => tracklib2::schema::DataType::I64,
                    "f64" => tracklib2::schema::DataType::F64,
                    "bool" => tracklib2::schema::DataType::Bool,
                    "string" => tracklib2::schema::DataType::String,
                    val @ _ => {
                        VM::raise(
                            Class::from_existing("Exception"),
                            &format!("Schema Data Type '{val}' unknown"),
                        );
                        unreachable!();
                    }
                };

                tracklib2::schema::FieldDefinition::new(ruby_field_name.to_string(), data_type)
            } else {
                VM::raise(
                    Class::from_existing("Exception"),
                    "Schema array entries must have length 2",
                );
                unreachable!();
            }
        })
        .collect::<Vec<_>>();

    tracklib2::schema::Schema::with_fields(fields)
}
