use ouroboros::self_referencing;
use rutie::{
    class, methods, wrappable_struct, AnyObject, Array, Boolean, Class, Encoding, Float, Hash,
    Integer, Module, NilClass, Object, RString, Symbol, VM,
};
use tracklib2::read::section::SectionRead;

#[self_referencing]
pub struct WrappableTrackReader {
    data: Vec<u8>,
    #[borrows(data)]
    #[not_covariant]
    track_reader: tracklib2::read::track::TrackReader<'this>,
}

wrappable_struct!(
    WrappableTrackReader,
    TrackReaderWrapper,
    TRACK_READER_WRAPPER_INSTANCE
);

class!(TrackReader);

methods!(
    TrackReader,
    rtself,
    fn trackreader_new(bytes: RString) -> AnyObject {
        let source = bytes.map_err(VM::raise_ex).unwrap();
        let data = source.to_bytes_unchecked().to_vec();
        let wrapper = WrappableTrackReader::new(data, |d| {
            tracklib2::read::track::TrackReader::new(d)
                .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{}", e)))
                .unwrap()
        });

        Module::from_existing("Tracklib")
            .get_nested_class("TrackReader")
            .wrap_data(wrapper, &*TRACK_READER_WRAPPER_INSTANCE)
    },
    fn trackreader_metadata() -> Array {
        let metadata_entries = rtself
            .get_data(&*TRACK_READER_WRAPPER_INSTANCE)
            .with_track_reader(|track_reader| track_reader.metadata());

        let mut metadata_array = Array::new();

        for metadata_entry in metadata_entries {
            let metadata_entry_array = match metadata_entry {
                tracklib2::types::MetadataEntry::TrackType(track_type) => {
                    let mut metadata_entry_array = Array::new();

                    let (type_name, id) = match track_type {
                        tracklib2::types::TrackType::Trip(id) => {
                            (Symbol::new("trip"), Integer::from(*id))
                        }
                        tracklib2::types::TrackType::Route(id) => {
                            (Symbol::new("route"), Integer::from(*id))
                        }
                        tracklib2::types::TrackType::Segment(id) => {
                            (Symbol::new("segment"), Integer::from(*id))
                        }
                    };

                    metadata_entry_array.push(Symbol::new("track_type"));
                    metadata_entry_array.push(type_name);
                    metadata_entry_array.push(id);

                    metadata_entry_array
                }
                tracklib2::types::MetadataEntry::CreatedAt(created_at) => {
                    let mut metadata_entry_array = Array::new();

                    metadata_entry_array.push(Symbol::new("created_at"));

                    let time_obj = Class::from_existing("Time")
                        .protect_send("at", &[Integer::from(*created_at).to_any_object()])
                        .map_err(VM::raise_ex)
                        .unwrap()
                        .protect_send("utc", &[])
                        .map_err(VM::raise_ex)
                        .unwrap();

                    metadata_entry_array.push(time_obj);

                    metadata_entry_array
                }
            };

            metadata_array.push(metadata_entry_array);
        }

        metadata_array
    },
    fn trackreader_file_version() -> Integer {
        Integer::from(u32::from(
            rtself
                .get_data(&*TRACK_READER_WRAPPER_INSTANCE)
                .with_track_reader(|track_reader| track_reader.file_version()),
        ))
    },
    fn trackreader_creator_version() -> Integer {
        Integer::from(u32::from(
            rtself
                .get_data(&*TRACK_READER_WRAPPER_INSTANCE)
                .with_track_reader(|track_reader| track_reader.creator_version()),
        ))
    },
    fn trackreader_section_count() -> Integer {
        Integer::from(
            u64::try_from(
                rtself
                    .get_data(&*TRACK_READER_WRAPPER_INSTANCE)
                    .with_track_reader(|track_reader| track_reader.section_count()),
            )
            .map_err(|_| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap(),
        )
    },
    fn trackreader_section_encoding(index: Integer) -> Symbol {
        let ruby_index = index.map_err(VM::raise_ex).unwrap();
        let rust_index = usize::try_from(ruby_index.to_u64())
            .map_err(|_| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap();
        let encoding = rtself
            .get_data(&*TRACK_READER_WRAPPER_INSTANCE)
            .with_track_reader(|track_reader| {
                track_reader
                    .section(rust_index)
                    .map(|section| match section {
                        tracklib2::read::section::Section::Standard(section) => section.encoding(),
                        tracklib2::read::section::Section::Encrypted(section) => section.encoding(),
                    })
            })
            .ok_or_else(|| VM::raise(Class::from_existing("Exception"), "Section does not exist"))
            .unwrap();

        Symbol::new(match encoding {
            tracklib2::types::SectionEncoding::Standard => "standard",
            tracklib2::types::SectionEncoding::Encrypted => "encrypted",
        })
    },
    fn trackreader_section_schema(index: Integer) -> Array {
        let ruby_index = index.map_err(VM::raise_ex).unwrap();
        let rust_index = usize::try_from(ruby_index.to_u64())
            .map_err(|_| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap();
        let schema = rtself
            .get_data(&*TRACK_READER_WRAPPER_INSTANCE)
            .with_track_reader(|track_reader| {
                track_reader
                    .section(rust_index)
                    .map(|section| match section {
                        tracklib2::read::section::Section::Standard(section) => section.schema(),
                        tracklib2::read::section::Section::Encrypted(section) => section.schema(),
                    })
            })
            .ok_or_else(|| VM::raise(Class::from_existing("Exception"), "Section does not exist"))
            .unwrap();

        let mut schema_array = Array::new();

        for field_def in schema.fields() {
            let mut field_array = Array::new();
            field_array.push(RString::from(String::from(field_def.name())));
            match field_def.data_type() {
                tracklib2::schema::DataType::I64 => {
                    field_array.push(Symbol::new("i64"));
                }
                tracklib2::schema::DataType::F64 { scale } => {
                    field_array.push(Symbol::new("f64"));
                    field_array.push(Integer::from(u32::from(*scale)));
                }
                tracklib2::schema::DataType::U64 => {
                    field_array.push(Symbol::new("u64"));
                }
                tracklib2::schema::DataType::Bool => {
                    field_array.push(Symbol::new("bool"));
                }
                tracklib2::schema::DataType::String => {
                    field_array.push(Symbol::new("string"));
                }
                tracklib2::schema::DataType::BoolArray => {
                    field_array.push(Symbol::new("bool_array"));
                }
                tracklib2::schema::DataType::U64Array => {
                    field_array.push(Symbol::new("u64_array"));
                }
                tracklib2::schema::DataType::ByteArray => {
                    field_array.push(Symbol::new("byte_array"));
                }
            };
            schema_array.push(field_array);
        }

        schema_array
    },
    fn trackreader_section_rows(index: Integer) -> Integer {
        let ruby_index = index.map_err(VM::raise_ex).unwrap();
        let rust_index = usize::try_from(ruby_index.to_u64())
            .map_err(|_| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap();
        let rows = rtself
            .get_data(&*TRACK_READER_WRAPPER_INSTANCE)
            .with_track_reader(|track_reader| {
                track_reader
                    .section(rust_index)
                    .map(|section| match section {
                        tracklib2::read::section::Section::Standard(section) => section.rows(),
                        tracklib2::read::section::Section::Encrypted(section) => section.rows(),
                    })
            })
            .ok_or_else(|| VM::raise(Class::from_existing("Exception"), "Section does not exist"))
            .unwrap();

        Integer::from(
            u64::try_from(rows)
                .map_err(|_| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
                .unwrap(),
        )
    },
    fn trackreader_section_data(index: Integer, key_material: RString) -> Array {
        let ruby_index = index.map_err(VM::raise_ex).unwrap();
        let rust_index = usize::try_from(ruby_index.to_u64())
            .map_err(|_| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap();
        let data = rtself
            .get_data(&*TRACK_READER_WRAPPER_INSTANCE)
            .with_track_reader(|track_reader| {
                track_reader
                    .section(rust_index)
                    .map(|section| match section {
                        tracklib2::read::section::Section::Standard(section) => {
                            reader_to_array_of_hashes(
                                section
                                    .reader()
                                    .map_err(|e| {
                                        VM::raise(
                                            Class::from_existing("Exception"),
                                            &format!("{}", e),
                                        )
                                    })
                                    .unwrap(),
                            )
                        }
                        tracklib2::read::section::Section::Encrypted(mut section) => {
                            let ruby_key_material = key_material.map_err(VM::raise_ex).unwrap();
                            let rust_key_material = ruby_key_material.to_bytes_unchecked();
                            reader_to_array_of_hashes(
                                section
                                    .reader(rust_key_material)
                                    .map_err(|e| {
                                        VM::raise(
                                            Class::from_existing("Exception"),
                                            &format!("{}", e),
                                        )
                                    })
                                    .unwrap(),
                            )
                        }
                    })
            })
            .ok_or_else(|| VM::raise(Class::from_existing("Exception"), "Section does not exist"))
            .unwrap();

        data
    },
    fn trackreader_section_column(index: Integer, column_name: RString, key_material: RString) -> AnyObject {
        let ruby_index = index.map_err(VM::raise_ex).unwrap();
        let rust_index = usize::try_from(ruby_index.to_u64())
            .map_err(|_| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap();
        let data = rtself
            .get_data(&*TRACK_READER_WRAPPER_INSTANCE)
            .with_track_reader(|track_reader| {
                track_reader.section(rust_index).map(|section| {
                    let ruby_field_name = column_name.map_err(VM::raise_ex).unwrap();
                    let field_name = ruby_field_name.to_str();

                    let schema = match section {
                        tracklib2::read::section::Section::Standard(ref section) => {
                            section.schema()
                        }
                        tracklib2::read::section::Section::Encrypted(ref section) => {
                            section.schema()
                        }
                    };
                    let maybe_field_def = schema
                        .fields()
                        .iter()
                        .find(|field_def| field_def.name() == field_name);

                    if let Some(field_def) = maybe_field_def {
                        let schema =
                            tracklib2::schema::Schema::with_fields(vec![field_def.clone()]);
                        match section {
                            tracklib2::read::section::Section::Standard(section) => {
                                reader_to_single_column_array(
                                    section
                                        .reader_for_schema(&schema)
                                        .map_err(|e| {
                                            VM::raise(
                                                Class::from_existing("Exception"),
                                                &format!("{}", e),
                                            )
                                        })
                                        .unwrap(),
                                )
                                .to_any_object()
                            }
                            tracklib2::read::section::Section::Encrypted(mut section) => {
                                let ruby_key_material = key_material.map_err(VM::raise_ex).unwrap();
                                let rust_key_material = ruby_key_material.to_bytes_unchecked();
                                reader_to_single_column_array(
                                    section
                                        .reader_for_schema(rust_key_material, &schema)
                                        .map_err(|e| {
                                            VM::raise(
                                                Class::from_existing("Exception"),
                                                &format!("{}", e),
                                            )
                                        })
                                        .unwrap(),
                                )
                                .to_any_object()
                            }
                        }
                    } else {
                        NilClass::new().to_any_object()
                    }
                })
            })
            .ok_or_else(|| VM::raise(Class::from_existing("Exception"), "Section does not exist"))
            .unwrap();

        data
    }
);

fn fieldvalue_to_ruby(value: tracklib2::types::FieldValue) -> AnyObject {
    match value {
        tracklib2::types::FieldValue::I64(v) => Integer::new(v).to_any_object(),
        tracklib2::types::FieldValue::F64(v) => Float::new(v).to_any_object(),
        tracklib2::types::FieldValue::U64(v) => Integer::from(v).to_any_object(),
        tracklib2::types::FieldValue::Bool(v) => Boolean::new(v).to_any_object(),
        tracklib2::types::FieldValue::String(v) => RString::from(v).to_any_object(),
        tracklib2::types::FieldValue::BoolArray(v) => {
            let mut a = Array::new();
            for b in v {
                a.push(Boolean::new(b).to_any_object());
            }
            a.to_any_object()
        }
        tracklib2::types::FieldValue::U64Array(v) => {
            let mut a = Array::new();
            for u in v {
                a.push(Integer::from(u).to_any_object());
            }
            a.to_any_object()
        }
        tracklib2::types::FieldValue::ByteArray(v) => {
            let encoding = Encoding::find("ASCII-8BIT").map_err(VM::raise_ex).unwrap();

            RString::from_bytes(&v, &encoding).to_any_object()
        }
    }
}

fn reader_to_array_of_hashes(mut reader: tracklib2::read::section::reader::SectionReader) -> Array {
    let mut data_array = Array::new();
    while let Some(columniter) = reader.open_column_iter() {
        let mut row_hash = Hash::new();
        for row in columniter {
            let (field_def, maybe_value) = row
                .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{}", e)))
                .unwrap();

            if let Some(value) = maybe_value {
                row_hash.store(
                    RString::from(String::from(field_def.name())),
                    fieldvalue_to_ruby(value),
                );
            }
        }
        data_array.push(row_hash);
    }

    data_array
}

fn reader_to_single_column_array(
    mut reader: tracklib2::read::section::reader::SectionReader,
) -> Array {
    let mut data_array = Array::new();
    while let Some(mut columniter) = reader.open_column_iter() {
        let (_field_def, maybe_value) = columniter
            .next()
            .ok_or_else(|| {
                VM::raise(
                    Class::from_existing("Exception"),
                    "Missing field inside iterator",
                )
            })
            .unwrap()
            .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{}", e)))
            .unwrap();

        let ruby_value = if let Some(value) = maybe_value {
            fieldvalue_to_ruby(value)
        } else {
            NilClass::new().to_any_object()
        };

        data_array.push(ruby_value);
    }

    data_array
}
