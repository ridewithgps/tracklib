use rutie::{
    class, methods, wrappable_struct, AnyObject, Array, Class, Integer, Module, Object, RString,
    Symbol, VerifiedObject, VM,
};

pub struct WrappableSchema {
    schema: tracklib2::schema::Schema,
}

wrappable_struct!(WrappableSchema, SchemaWrapper, SCHEMA_WRAPPER_INSTANCE);

class!(Schema);

methods!(
    Schema,
    rtself,
    fn schema_new(ruby_schema: Array) -> AnyObject {
        let fields = ruby_schema
            .map_err(|e| VM::raise_ex(e))
            .unwrap()
            .into_iter()
            .map(|ele| {
                let ruby_schema_entry = ele
                    .try_convert_to::<Array>()
                    .map_err(|e| VM::raise_ex(e))
                    .unwrap();

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
                    "f64" => {
                        let ruby_scale = ruby_schema_entry
                            .at(2)
                            .try_convert_to::<Integer>()
                            .map_err(|e| VM::raise_ex(e))
                            .unwrap();
                        let scale = u8::try_from(ruby_scale.to_u64())
                            .map_err(|e| {
                                VM::raise(Class::from_existing("Exception"), &format!("{}", e))
                            })
                            .unwrap();
                        tracklib2::schema::DataType::F64 { scale }
                    }
                    "u64" => tracklib2::schema::DataType::U64,
                    "bool" => tracklib2::schema::DataType::Bool,
                    "string" => tracklib2::schema::DataType::String,
                    "bool_array" => tracklib2::schema::DataType::BoolArray,
                    "u64_array" => tracklib2::schema::DataType::U64Array,
                    "byte_array" => tracklib2::schema::DataType::ByteArray,
                    val @ _ => {
                        VM::raise(
                            Class::from_existing("Exception"),
                            &format!("Schema Data Type '{val}' unknown"),
                        );
                        unreachable!();
                    }
                };

                tracklib2::schema::FieldDefinition::new(ruby_field_name.to_string(), data_type)
            })
            .collect::<Vec<_>>();

        Module::from_existing("Tracklib")
            .get_nested_class("Schema")
            .wrap_data(
                WrappableSchema {
                    schema: tracklib2::schema::Schema::with_fields(fields),
                },
                &*SCHEMA_WRAPPER_INSTANCE,
            )
    },
);

impl Schema {
    pub(crate) fn inner(&self) -> &tracklib2::schema::Schema {
        &self.get_data(&*SCHEMA_WRAPPER_INSTANCE).schema
    }
}

impl VerifiedObject for Schema {
    fn is_correct_type<T: Object>(object: &T) -> bool {
        object.class() == Module::from_existing("Tracklib").get_nested_class("Schema")
    }

    fn error_message() -> &'static str {
        "Error converting to Schema"
    }
}
