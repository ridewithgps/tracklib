use ouroboros::self_referencing;
use rutie::{
    class, methods, wrappable_struct, AnyObject, Array, Boolean, Class, Float, Hash, Integer,
    Object, RString, Symbol, VM,
};
use tracklib2;

#[self_referencing]
pub struct TrackReaderWrapper {
    data: Vec<u8>,
    #[borrows(data)]
    #[not_covariant]
    track_reader: tracklib2::read::track::TrackReader<'this>,
}

wrappable_struct!(
    TrackReaderWrapper,
    TrackReaderWrapperWrapper,
    TRACK_READER_WRAPPER
);

class!(TrackReader);

methods!(
    TrackReader,
    rtself,
    fn trackreader_new(bytes: RString) -> AnyObject {
        let source = bytes.map_err(|e| VM::raise_ex(e)).unwrap();
        let data = source.to_bytes_unchecked().to_vec();
        let wrapper = TrackReaderWrapper::new(data, |d| {
            tracklib2::read::track::TrackReader::new(d)
                .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{}", e)))
                .unwrap()
        });

        Class::from_existing("TrackReader").wrap_data(wrapper, &*TRACK_READER_WRAPPER)
    },
    fn trackreader_metadata() -> Array {
        let metadata_entries = rtself
            .get_data(&*TRACK_READER_WRAPPER)
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
                        .map_err(|e| VM::raise_ex(e))
                        .unwrap()
                        .protect_send("utc", &[])
                        .map_err(|e| VM::raise_ex(e))
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
                .get_data(&*TRACK_READER_WRAPPER)
                .with_track_reader(|track_reader| track_reader.file_version()),
        ))
    },
    fn trackreader_creator_version() -> Integer {
        Integer::from(u32::from(
            rtself
                .get_data(&*TRACK_READER_WRAPPER)
                .with_track_reader(|track_reader| track_reader.creator_version()),
        ))
    },
    fn trackreader_section_count() -> Integer {
        Integer::from(
            u64::try_from(
                rtself
                    .get_data(&*TRACK_READER_WRAPPER)
                    .with_track_reader(|track_reader| track_reader.section_count()),
            )
            .map_err(|e| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap(),
        )
    },
    fn trackreader_section_encoding(index: Integer) -> Symbol {
        let ruby_index = index.map_err(|e| VM::raise_ex(e)).unwrap();
        let rust_index = usize::try_from(ruby_index.to_u64())
            .map_err(|e| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap();
        let encoding = rtself
            .get_data(&*TRACK_READER_WRAPPER)
            .with_track_reader(|track_reader| {
                track_reader
                    .section(rust_index)
                    .map(|section| section.encoding())
            })
            .ok_or_else(|| VM::raise(Class::from_existing("Exception"), "Section does not exist"))
            .unwrap();

        Symbol::new(match encoding {
            tracklib2::types::SectionEncoding::Standard => "standard",
        })
    },
    fn trackreader_section_schema(index: Integer) -> Array {
        let ruby_index = index.map_err(|e| VM::raise_ex(e)).unwrap();
        let rust_index = usize::try_from(ruby_index.to_u64())
            .map_err(|e| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap();
        let schema = rtself
            .get_data(&*TRACK_READER_WRAPPER)
            .with_track_reader(|track_reader| {
                track_reader
                    .section(rust_index)
                    .map(|section| section.schema())
            })
            .ok_or_else(|| VM::raise(Class::from_existing("Exception"), "Section does not exist"))
            .unwrap();

        let mut schema_array = Array::new();

        for field_def in schema.fields() {
            let mut field_array = Array::new();
            field_array.push(RString::from(String::from(field_def.name())));
            field_array.push(Symbol::new(match field_def.data_type() {
                tracklib2::schema::DataType::I64 => "i64",
                tracklib2::schema::DataType::Bool => "bool",
                tracklib2::schema::DataType::String => "string",
                tracklib2::schema::DataType::F64 => "f64",
            }));
            schema_array.push(field_array);
        }

        schema_array
    },
    fn trackreader_section_rows(index: Integer) -> Integer {
        let ruby_index = index.map_err(|e| VM::raise_ex(e)).unwrap();
        let rust_index = usize::try_from(ruby_index.to_u64())
            .map_err(|e| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap();
        let rows = rtself
            .get_data(&*TRACK_READER_WRAPPER)
            .with_track_reader(|track_reader| {
                track_reader
                    .section(rust_index)
                    .map(|section| section.rows())
            })
            .ok_or_else(|| VM::raise(Class::from_existing("Exception"), "Section does not exist"))
            .unwrap();

        Integer::from(
            u64::try_from(rows)
                .map_err(|e| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
                .unwrap(),
        )
    },
    fn trackreader_section_data(index: Integer) -> Array {
        let ruby_index = index.map_err(|e| VM::raise_ex(e)).unwrap();
        let rust_index = usize::try_from(ruby_index.to_u64())
            .map_err(|e| VM::raise(Class::from_existing("Exception"), "u64 != usize"))
            .unwrap();
        let data = rtself
            .get_data(&*TRACK_READER_WRAPPER)
            .with_track_reader(|track_reader| {
                track_reader.section(rust_index).map(|section| {
                    let mut section_reader = section
                        .reader()
                        .map_err(|e| {
                            VM::raise(Class::from_existing("Exception"), &format!("{}", e))
                        })
                        .unwrap();

                    let mut data_array = Array::new();
                    while let Some(columniter) = section_reader.open_column_iter() {
                        let mut row_hash = Hash::new();
                        for row in columniter {
                            let (field_def, maybe_value) = row
                                .map_err(|e| {
                                    VM::raise(Class::from_existing("Exception"), &format!("{}", e))
                                })
                                .unwrap();

                            if let Some(value) = maybe_value {
                                row_hash.store(
                                    RString::from(String::from(field_def.name())),
                                    match value {
                                        tracklib2::types::FieldValue::I64(v) => {
                                            Integer::new(v).to_any_object()
                                        }
                                        tracklib2::types::FieldValue::F64(v) => {
                                            Float::new(v).to_any_object()
                                        }
                                        tracklib2::types::FieldValue::Bool(v) => {
                                            Boolean::new(v).to_any_object()
                                        }
                                        tracklib2::types::FieldValue::String(v) => {
                                            RString::from(String::from(v)).to_any_object()
                                        }
                                    },
                                );
                            }
                        }
                        data_array.push(row_hash);
                    }

                    data_array
                })
            })
            .ok_or_else(|| VM::raise(Class::from_existing("Exception"), "Section does not exist"))
            .unwrap();

        data
    }
);
