use magnus::{
    data_type_builder,
    typed_data::{DataType, DataTypeFunctions, Obj},
    value::{Lazy, ReprValue},
    Class, Error, IntoValue, Module, RArray, RClass, RString, Ruby, TryConvert, Value,
};
use ouroboros::self_referencing;
use tracklib::read::section::SectionRead;

#[self_referencing]
pub struct TrackReader {
    data: Vec<u8>,
    #[borrows(data)]
    #[not_covariant]
    reader: tracklib::read::track::TrackReader<'this>,
}

impl TrackReader {
    pub fn create(handle: &Ruby, data: RString) -> Result<Self, Error> {
        TrackReaderTryBuilder {
            data: unsafe { data.as_slice().to_vec() },
            reader_builder: |data: &Vec<u8>| {
                tracklib::read::track::TrackReader::new(data)
                    .map_err(|e| Error::new(handle.exception_arg_error(), format!("invalid tracklib file: {e:?}")))
            },
        }
        .try_build()
    }

    pub fn file_version(&self) -> u8 {
        self.with_reader(|reader| reader.file_version())
    }

    pub fn creator_version(&self) -> u8 {
        self.with_reader(|reader| reader.creator_version())
    }

    pub fn metadata(handle: &Ruby, rb_self: Obj<Self>) -> Result<RArray, Error> {
        rb_self.with_reader(|reader| {
            handle.ary_try_from_iter(reader.metadata().iter().map(|entry| {
                match entry {
                    tracklib::types::MetadataEntry::TrackType(track_type) => match track_type {
                        tracklib::types::TrackType::Trip(id) => Ok((
                            handle.to_symbol("track_type"),
                            handle.to_symbol("trip"),
                            handle.integer_from_u64(*id),
                        )
                            .into_value_with(handle)),
                        tracklib::types::TrackType::Route(id) => Ok((
                            handle.to_symbol("track_type"),
                            handle.to_symbol("route"),
                            handle.integer_from_u64(*id),
                        )
                            .into_value_with(handle)),
                        tracklib::types::TrackType::Segment(id) => Ok((
                            handle.to_symbol("track_type"),
                            handle.to_symbol("segment"),
                            handle.integer_from_u64(*id),
                        )
                            .into_value_with(handle)),
                    },
                    tracklib::types::MetadataEntry::CreatedAt(created_at) => Ok((
                        handle.to_symbol("created_at").as_value(),
                        handle
                            .class_time()
                            .funcall_public::<_, _, magnus::Time>("at", (*created_at,))?
                            .funcall_public::<_, _, magnus::Time>("utc", ())?
                            .as_value(),
                    )
                        .into_value_with(handle)),
                }
            }))
        })
    }

    pub fn section_count(&self) -> usize {
        self.with_reader(|reader| reader.section_count())
    }

    pub fn section_encoding(handle: &Ruby, rb_self: Obj<Self>, index: usize) -> Result<magnus::symbol::Symbol, Error> {
        rb_self.with_reader(|reader| {
            reader
                .section(index)
                .map(|section| match section {
                    tracklib::read::section::Section::Standard(_section) => handle.to_symbol("standard"),
                    tracklib::read::section::Section::Encrypted(_section) => handle.to_symbol("encrypted"),
                })
                .ok_or_else(|| {
                    Error::new(
                        handle.exception_index_error(),
                        format!("Section {index} does not exist"),
                    )
                })
        })
    }

    pub fn section_schema(handle: &Ruby, rb_self: Obj<Self>, index: usize) -> Result<RArray, Error> {
        let schema = rb_self.with_reader(|reader| {
            reader
                .section(index)
                .map(|section| match section {
                    tracklib::read::section::Section::Standard(section) => section.schema(),
                    tracklib::read::section::Section::Encrypted(section) => section.schema(),
                })
                .ok_or_else(|| {
                    Error::new(
                        handle.exception_index_error(),
                        format!("Section {index} does not exist"),
                    )
                })
        })?;

        Ok(handle.ary_from_iter(schema.fields().iter().map(|field_def| {
            match field_def.data_type() {
                tracklib::schema::DataType::I64 => {
                    (handle.str_new(field_def.name()), handle.to_symbol("i64")).into_value_with(handle)
                }
                tracklib::schema::DataType::F64 { scale } => (
                    handle.str_new(field_def.name()),
                    handle.to_symbol("f64"),
                    handle.integer_from_u64(u64::from(*scale)),
                )
                    .into_value_with(handle),
                tracklib::schema::DataType::U64 => {
                    (handle.str_new(field_def.name()), handle.to_symbol("u64")).into_value_with(handle)
                }
                tracklib::schema::DataType::Bool => {
                    (handle.str_new(field_def.name()), handle.to_symbol("bool")).into_value_with(handle)
                }
                tracklib::schema::DataType::String => {
                    (handle.str_new(field_def.name()), handle.to_symbol("string")).into_value_with(handle)
                }
                tracklib::schema::DataType::BoolArray => {
                    (handle.str_new(field_def.name()), handle.to_symbol("bool_array")).into_value_with(handle)
                }
                tracklib::schema::DataType::U64Array => {
                    (handle.str_new(field_def.name()), handle.to_symbol("u64_array")).into_value_with(handle)
                }
                tracklib::schema::DataType::ByteArray => {
                    (handle.str_new(field_def.name()), handle.to_symbol("byte_array")).into_value_with(handle)
                }
            }
        })))
    }

    pub fn section_rows(handle: &Ruby, rb_self: Obj<Self>, index: usize) -> Result<usize, Error> {
        rb_self.with_reader(|reader| {
            reader
                .section(index)
                .map(|section| match section {
                    tracklib::read::section::Section::Standard(section) => section.rows(),
                    tracklib::read::section::Section::Encrypted(section) => section.rows(),
                })
                .ok_or_else(|| {
                    Error::new(
                        handle.exception_index_error(),
                        format!("Section {index} does not exist"),
                    )
                })
        })
    }

    pub fn section_data(handle: &Ruby, rb_self: Obj<Self>, arguments: &[Value]) -> Result<RArray, Error> {
        let args = magnus::scan_args::scan_args::<_, _, (), (), (), ()>(arguments)?;

        let (index,): (usize,) = args.required;
        let (key_material,): (Option<Value>,) = args.optional;

        rb_self.with_reader(|reader| {
            let section = reader.section(index).ok_or_else(|| {
                Error::new(
                    handle.exception_index_error(),
                    format!("Section {index} does not exist"),
                )
            })?;

            match section {
                tracklib::read::section::Section::Standard(section) => {
                    let reader = section.reader().map_err(|e| {
                        Error::new(handle.exception_exception(), format!("Could not parse section: {e:?}"))
                    })?;

                    reader_to_array_of_hashes(handle, reader)
                }
                tracklib::read::section::Section::Encrypted(mut section) => {
                    let key_material = key_material
                        .ok_or_else(|| Error::new(handle.exception_arg_error(), "Missing 'key_material' argument"))?;
                    let key_material = RString::try_convert(key_material)?;
                    let key_bytes = unsafe { key_material.as_slice().to_vec() };
                    let reader = section.reader(&key_bytes).map_err(|e| {
                        Error::new(handle.exception_exception(), format!("Could not parse section: {e:?}"))
                    })?;

                    reader_to_array_of_hashes(handle, reader)
                }
            }
        })
    }

    pub fn section_column(handle: &Ruby, rb_self: Obj<Self>, arguments: &[Value]) -> Result<RArray, Error> {
        let args = magnus::scan_args::scan_args::<_, _, (), (), (), ()>(arguments)?;

        let (index, column_name): (usize, String) = args.required;
        let (key_material,): (Option<Value>,) = args.optional;

        rb_self.with_reader(|reader| {
            let section = reader.section(index).ok_or_else(|| {
                Error::new(
                    handle.exception_index_error(),
                    format!("Section {index} does not exist"),
                )
            })?;

            let schema = match section {
                tracklib::read::section::Section::Standard(ref section) => section.schema(),
                tracklib::read::section::Section::Encrypted(ref section) => section.schema(),
            };
            let maybe_field_def = schema.fields().iter().find(|field_def| field_def.name() == column_name);

            if let Some(field_def) = maybe_field_def {
                let schema = tracklib::schema::Schema::with_fields(vec![field_def.clone()]);

                match section {
                    tracklib::read::section::Section::Standard(section) => {
                        let reader = section.reader_for_schema(&schema).map_err(|e| {
                            Error::new(handle.exception_exception(), format!("Could not parse section: {e:?}"))
                        })?;

                        reader_to_single_column_array(handle, reader)
                    }
                    tracklib::read::section::Section::Encrypted(mut section) => {
                        let key_material = key_material.ok_or_else(|| {
                            Error::new(handle.exception_arg_error(), "Missing 'key_material' argument")
                        })?;
                        let key_material = RString::try_convert(key_material)?;
                        let key_bytes = unsafe { key_material.as_slice().to_vec() };

                        let reader = section.reader_for_schema(&key_bytes, &schema).map_err(|e| {
                            Error::new(handle.exception_exception(), format!("Could not parse section: {e:?}"))
                        })?;

                        reader_to_single_column_array(handle, reader)
                    }
                }
            } else {
                Ok(handle.ary_new())
            }
        })
    }
}

fn fieldvalue_to_ruby(handle: &Ruby, value: tracklib::types::FieldValue) -> Value {
    match value {
        tracklib::types::FieldValue::I64(v) => handle.integer_from_i64(v).into_value_with(handle),
        tracklib::types::FieldValue::F64(v) => handle.float_from_f64(v).into_value_with(handle),
        tracklib::types::FieldValue::U64(v) => handle.integer_from_u64(v).into_value_with(handle),
        tracklib::types::FieldValue::Bool(v) => {
            if v {
                handle.qtrue().into_value_with(handle)
            } else {
                handle.qfalse().into_value_with(handle)
            }
        }
        tracklib::types::FieldValue::String(v) => {
            handle.enc_str_new(&v, handle.utf8_encoding()).into_value_with(handle)
        }
        tracklib::types::FieldValue::BoolArray(v) => handle
            .ary_from_iter(v.iter().map(|b| {
                if *b {
                    handle.qtrue().into_value_with(handle)
                } else {
                    handle.qfalse().into_value_with(handle)
                }
            }))
            .into_value_with(handle),
        tracklib::types::FieldValue::U64Array(v) => handle
            .ary_from_iter(v.iter().map(|u| handle.integer_from_u64(*u).into_value_with(handle)))
            .into_value_with(handle),
        tracklib::types::FieldValue::ByteArray(v) => handle
            .enc_str_new(&v, handle.ascii8bit_encoding())
            .into_value_with(handle),
    }
}

fn reader_to_array_of_hashes(
    handle: &Ruby,
    mut reader: tracklib::read::section::reader::SectionReader,
) -> Result<RArray, Error> {
    handle.ary_try_from_iter(std::iter::from_fn(|| {
        let columniter = reader.open_column_iter()?;

        Some(
            handle.hash_try_from_iter(
                columniter
                    .map(|row| {
                        row.map_err(|e| {
                            Error::new(handle.exception_exception(), format!("Could not parse section: {e:?}"))
                        })
                    })
                    .map(|row| {
                        row.map(|(field_def, maybe_value)| {
                            maybe_value.map(|value| (field_def.name(), fieldvalue_to_ruby(handle, value)))
                        })
                    })
                    .filter(|row| match row {
                        Ok(Some(_)) => true,
                        Ok(None) => false,
                        Err(_) => true,
                    })
                    .map(|row| match row {
                        Ok(Some(v)) => Ok(v),
                        Ok(None) => unreachable!("the filter should remove Ok(None)"),
                        Err(e) => Err(e),
                    }),
            ),
        )
    }))
}

fn reader_to_single_column_array(
    handle: &Ruby,
    mut reader: tracklib::read::section::reader::SectionReader,
) -> Result<RArray, Error> {
    handle.ary_try_from_iter(std::iter::from_fn(|| {
        reader.open_column_iter()?.next().map(|field| match field {
            Ok((_field_def, Some(field_value))) => Ok(fieldvalue_to_ruby(handle, field_value)),
            Ok((_field_def, None)) => Ok(handle.qnil().into_value_with(handle)),
            Err(e) => Err(Error::new(
                handle.exception_exception(),
                format!("Error reading tracklib data: {e:?}"),
            )),
        })
    }))
}

impl DataTypeFunctions for TrackReader {}

unsafe impl magnus::typed_data::TypedData for TrackReader {
    fn class(handle: &Ruby) -> RClass {
        static CLASS: Lazy<RClass> = Lazy::new(|handle| {
            let module = handle.define_module("Tracklib").unwrap();
            let class = module.define_class("TrackReader", handle.class_object()).unwrap();
            class.undef_default_alloc_func();
            class
        });
        handle.get_inner(&CLASS)
    }

    fn data_type() -> &'static DataType {
        static DATA_TYPE: DataType = data_type_builder!(TrackReader, "TrackReader").build();
        &DATA_TYPE
    }
}
