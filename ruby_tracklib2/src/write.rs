use rutie::{
    class, methods, module, wrappable_struct, AnyObject, Array, Boolean, Class, Encoding, Float,
    Hash, Integer, Module, Object, RString, Symbol, VerifiedObject, VM,
};

pub struct SectionInner {
    inner: tracklib2::write::section::Section,
}

wrappable_struct!(SectionInner, SectionWrapper, SECTION_WRAPPER);

class!(Section);

methods!(
    Section,
    rtself,
    fn section_new(encoding: Symbol, schema: Array, data: Array) -> AnyObject {
        let tracklib_schema =
            crate::schema::create_schema(schema.map_err(|e| VM::raise_ex(e)).unwrap());
        let tracklib_encoding = match encoding.map_err(|e| VM::raise_ex(e)).unwrap().to_str() {
            "standard" => tracklib2::types::SectionEncoding::Standard,
            val @ _ => {
                VM::raise(
                    Class::from_existing("Exception"),
                    &format!("SectionEncoding '{val}' unknown"),
                );
                unreachable!();
            }
        };
        let mut tracklib_section =
            tracklib2::write::section::Section::new(tracklib_encoding, tracklib_schema);

        for ruby_row_obj in data.map_err(|e| VM::raise_ex(e)).unwrap() {
            let ruby_row = ruby_row_obj
                .try_convert_to::<Hash>()
                .map_err(|e| VM::raise_ex(e))
                .unwrap();

            let mut rowbuilder = tracklib_section.open_row_builder();

            while let Some(column_writer) = rowbuilder.next_column_writer() {
                match column_writer {
                    tracklib2::write::section::ColumnWriter::I64ColumnWriter(cwi) => {
                        let ruby_field_name =
                            RString::from(String::from(cwi.field_definition().name()));
                        let ruby_field = ruby_row.at(&ruby_field_name);

                        let write_result = if ruby_field.is_nil() {
                            cwi.write(None)
                        } else {
                            cwi.write(Some(&match ruby_field.try_convert_to::<Integer>() {
                                Ok(i) => i.to_i64(),
                                Err(int_e) => ruby_field
                                    .try_convert_to::<Float>()
                                    .map_err(|_| VM::raise_ex(int_e))
                                    .unwrap()
                                    .to_f64()
                                    .round() as i64,
                            }))
                        };

                        if let Err(e) = write_result {
                            VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                        }
                    }
                    tracklib2::write::section::ColumnWriter::BoolColumnWriter(cwi) => {
                        let ruby_field_name =
                            RString::from(String::from(cwi.field_definition().name()));
                        let ruby_field = ruby_row.at(&ruby_field_name);

                        let write_result = if ruby_field.is_nil() {
                            cwi.write(None)
                        } else {
                            cwi.write(Some(
                                &ruby_field
                                    .try_convert_to::<Boolean>()
                                    .map_err(|e| VM::raise_ex(e))
                                    .unwrap()
                                    .to_bool(),
                            ))
                        };

                        if let Err(e) = write_result {
                            VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                        }
                    }
                    tracklib2::write::section::ColumnWriter::StringColumnWriter(cwi) => {
                        let ruby_field_name =
                            RString::from(String::from(cwi.field_definition().name()));
                        let ruby_field = ruby_row.at(&ruby_field_name);

                        let write_result = if ruby_field.is_nil() {
                            cwi.write(None)
                        } else {
                            cwi.write(Some(
                                &ruby_field
                                    .try_convert_to::<RString>()
                                    .map_err(|e| VM::raise_ex(e))
                                    .unwrap()
                                    .to_string(),
                            ))
                        };

                        if let Err(e) = write_result {
                            VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                        }
                    }
                    tracklib2::write::section::ColumnWriter::F64ColumnWriter(cwi) => {
                        let ruby_field_name =
                            RString::from(String::from(cwi.field_definition().name()));
                        let ruby_field = ruby_row.at(&ruby_field_name);

                        let write_result = if ruby_field.is_nil() {
                            cwi.write(None)
                        } else {
                            cwi.write(Some(
                                &Float::implicit_to_f(ruby_field)
                                    .map_err(|e| VM::raise_ex(e))
                                    .unwrap()
                                    .to_f64(),
                            ))
                        };

                        if let Err(e) = write_result {
                            VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                        }
                    }
                }
            }
        }

        Module::from_existing("Tracklib")
            .get_nested_class("Section")
            .wrap_data(
                SectionInner {
                    inner: tracklib_section,
                },
                &*SECTION_WRAPPER,
            )
    },
);

impl VerifiedObject for Section {
    fn is_correct_type<T: Object>(object: &T) -> bool {
        object.class() == Module::from_existing("Tracklib").get_nested_class("Section")
    }

    fn error_message() -> &'static str {
        "Error converting to Section"
    }
}

class!(Time);
impl VerifiedObject for Time {
    fn is_correct_type<T: Object>(object: &T) -> bool {
        object.class() == Class::from_existing("Time")
    }

    fn error_message() -> &'static str {
        "Error converting to Time"
    }
}

module!(Tracklib);

methods!(
    Tracklib,
    rtself,
    fn write_track(metadata: Array, sections: Array) -> RString {
        let metadata_array = metadata.map_err(|e| VM::raise_ex(e)).unwrap();

        let metadata_entries = metadata_array
            .into_iter()
            .map(|metadata_ele| {
                let metadata_ele_array = metadata_ele
                    .try_convert_to::<Array>()
                    .map_err(|e| VM::raise_ex(e))
                    .unwrap();
                if metadata_ele_array.length() >= 1 {
                    let metadata_type = metadata_ele_array
                        .at(0)
                        .try_convert_to::<Symbol>()
                        .map_err(|e| VM::raise_ex(e))
                        .unwrap();
                    match metadata_type.to_str() {
                        "track_type" => {
                            if metadata_ele_array.length() == 3 {
                                let track_type_symbol = metadata_ele_array
                                    .at(1)
                                    .try_convert_to::<Symbol>()
                                    .map_err(|e| VM::raise_ex(e))
                                    .unwrap();
                                let track_id = metadata_ele_array
                                    .at(2)
                                    .try_convert_to::<Integer>()
                                    .map_err(|e| VM::raise_ex(e))
                                    .unwrap()
                                    .to_u32();

                                let track_type = match track_type_symbol.to_str() {
                                    "route" => tracklib2::types::TrackType::Route(track_id),
                                    "trip" => tracklib2::types::TrackType::Trip(track_id),
                                    "segment" => tracklib2::types::TrackType::Segment(track_id),
                                    val @ _ => {
                                        VM::raise(
                                            Class::from_existing("Exception"),
                                            &format!("Metadata Entry Track Type '{val}' unknown"),
                                        );
                                        unreachable!();
                                    }
                                };

                                tracklib2::types::MetadataEntry::TrackType(track_type)
                            } else {
                                VM::raise(
                                    Class::from_existing("Exception"),
                                    "Metadata Entries for 'track_type' must have length 3",
                                );
                                unreachable!();
                            }
                        }
                        "created_at" => {
                            if metadata_ele_array.length() == 2 {
                                let created_at_time_obj = metadata_ele_array
                                    .at(1)
                                    .try_convert_to::<Time>()
                                    .map_err(|e| VM::raise_ex(e))
                                    .unwrap();
                                let created_at_val = created_at_time_obj
                                    .protect_send("utc", &[])
                                    .map_err(|e| VM::raise_ex(e))
                                    .unwrap()
                                    .protect_send("to_i", &[])
                                    .map_err(|e| VM::raise_ex(e))
                                    .unwrap()
                                    .try_convert_to::<Integer>()
                                    .map_err(|e| VM::raise_ex(e))
                                    .unwrap()
                                    .to_u64();
                                tracklib2::types::MetadataEntry::CreatedAt(created_at_val)
                            } else {
                                VM::raise(
                                    Class::from_existing("Exception"),
                                    "Metadata Entries for 'created_at' must have length 2",
                                );
                                unreachable!();
                            }
                        }
                        val @ _ => {
                            VM::raise(
                                Class::from_existing("Exception"),
                                &format!("Metadata Type '{val}' unknown"),
                            );
                            unreachable!();
                        }
                    }
                } else {
                    VM::raise(Class::from_existing("Exception"), "Invalid Metadata Entry");
                    unreachable!();
                }
            })
            .collect::<Vec<_>>();

        let sections_array = sections.map_err(|e| VM::raise_ex(e)).unwrap();

        let section_wrappers = sections_array
            .into_iter()
            .map(|ruby_section| {
                ruby_section
                    .try_convert_to::<Section>()
                    .map_err(|e| VM::raise_ex(e))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let tracklib_sections = section_wrappers
            .iter()
            .map(|section_wrapper| &section_wrapper.get_data(&*SECTION_WRAPPER).inner)
            .collect::<Vec<_>>();

        let mut buf = vec![];
        tracklib2::write::track::write_track(&mut buf, &metadata_entries, &tracklib_sections)
            .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{:?}", e)))
            .unwrap();

        let encoding = Encoding::find("ASCII-8BIT")
            .map_err(|e| VM::raise_ex(e))
            .unwrap();

        RString::from_bytes(&buf, &encoding)
    }
);
