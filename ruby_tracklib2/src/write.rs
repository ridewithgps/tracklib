use rutie::{
    class, methods, module, wrappable_struct, AnyObject, Array, Boolean, Class, Encoding, Float,
    Hash, Integer, Module, Object, RString, Symbol, VerifiedObject, VM,
};

pub struct WrappableSection {
    section: tracklib2::write::section::Section,
}
use tracklib2::write::section::SectionWrite;

wrappable_struct!(WrappableSection, SectionWrapper, SECTION_WRAPPER_INSTANCE);

class!(Section);

methods!(
    Section,
    rtself,
    fn section_standard(schema: crate::schema::Schema, data: Array) -> AnyObject {
        let tracklib_schema = schema.map_err(VM::raise_ex).unwrap().inner().clone();
        let mut tracklib_section =
            tracklib2::write::section::standard::Section::new(tracklib_schema);
        write_ruby_array_into_section(&mut tracklib_section, data.map_err(VM::raise_ex).unwrap());

        Module::from_existing("Tracklib")
            .get_nested_class("Section")
            .wrap_data(
                WrappableSection {
                    section: tracklib2::write::section::Section::Standard(tracklib_section),
                },
                &*SECTION_WRAPPER_INSTANCE,
            )
    },
    fn section_encrypted(schema: crate::schema::Schema, data: Array, key_material: RString) -> AnyObject {
        let ruby_key_material = key_material.map_err(VM::raise_ex).unwrap();
        let rust_key_material = ruby_key_material.to_bytes_unchecked();
        let tracklib_schema = schema.map_err(VM::raise_ex).unwrap().inner().clone();
        let mut tracklib_section =
            tracklib2::write::section::encrypted::Section::new(rust_key_material, tracklib_schema)
                .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{:?}", e)))
                .unwrap();
        write_ruby_array_into_section(&mut tracklib_section, data.map_err(VM::raise_ex).unwrap());

        Module::from_existing("Tracklib")
            .get_nested_class("Section")
            .wrap_data(
                WrappableSection {
                    section: tracklib2::write::section::Section::Encrypted(tracklib_section),
                },
                &*SECTION_WRAPPER_INSTANCE,
            )
    },
);

fn write_ruby_array_into_section<SW: SectionWrite>(section: &mut SW, data: Array) {
    for ruby_row_obj in data {
        let ruby_row = ruby_row_obj
            .try_convert_to::<Hash>()
            .map_err(VM::raise_ex)
            .unwrap();

        let mut rowbuilder = section.open_row_builder();

        while let Some(column_writer) = rowbuilder.next_column_writer() {
            match column_writer {
                tracklib2::write::section::writer::ColumnWriter::I64ColumnWriter(cwi) => {
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
                tracklib2::write::section::writer::ColumnWriter::F64ColumnWriter(cwi) => {
                    let ruby_field_name =
                        RString::from(String::from(cwi.field_definition().name()));
                    let ruby_field = ruby_row.at(&ruby_field_name);

                    let write_result = if ruby_field.is_nil() {
                        cwi.write(None)
                    } else {
                        cwi.write(Some(
                            &Float::implicit_to_f(ruby_field)
                                .map_err(VM::raise_ex)
                                .unwrap()
                                .to_f64(),
                        ))
                    };

                    if let Err(e) = write_result {
                        VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                    }
                }
                tracklib2::write::section::writer::ColumnWriter::U64ColumnWriter(cwi) => {
                    let ruby_field_name =
                        RString::from(String::from(cwi.field_definition().name()));
                    let ruby_field = ruby_row.at(&ruby_field_name);

                    let write_result = if ruby_field.is_nil() {
                        cwi.write(None)
                    } else {
                        cwi.write(Some(&match ruby_field.try_convert_to::<Integer>() {
                            Ok(i) => i.to_u64(),
                            Err(int_e) => ruby_field
                                .try_convert_to::<Float>()
                                .map_err(|_| VM::raise_ex(int_e))
                                .unwrap()
                                .to_f64()
                                .round() as u64,
                        }))
                    };

                    if let Err(e) = write_result {
                        VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                    }
                }
                tracklib2::write::section::writer::ColumnWriter::BoolColumnWriter(cwi) => {
                    let ruby_field_name =
                        RString::from(String::from(cwi.field_definition().name()));
                    let ruby_field = ruby_row.at(&ruby_field_name);

                    let write_result = if ruby_field.is_nil() {
                        cwi.write(None)
                    } else {
                        cwi.write(Some(
                            &ruby_field
                                .try_convert_to::<Boolean>()
                                .map_err(VM::raise_ex)
                                .unwrap()
                                .to_bool(),
                        ))
                    };

                    if let Err(e) = write_result {
                        VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                    }
                }
                tracklib2::write::section::writer::ColumnWriter::StringColumnWriter(cwi) => {
                    let ruby_field_name =
                        RString::from(String::from(cwi.field_definition().name()));
                    let ruby_field = ruby_row.at(&ruby_field_name);

                    let write_result = if ruby_field.is_nil() {
                        cwi.write(None)
                    } else {
                        cwi.write(Some(
                            ruby_field
                                .try_convert_to::<RString>()
                                .map_err(VM::raise_ex)
                                .unwrap()
                                .to_str(),
                        ))
                    };

                    if let Err(e) = write_result {
                        VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                    }
                }
                tracklib2::write::section::writer::ColumnWriter::BoolArrayColumnWriter(cwi) => {
                    let ruby_field_name =
                        RString::from(String::from(cwi.field_definition().name()));
                    let ruby_field = ruby_row.at(&ruby_field_name);

                    let write_result = if ruby_field.is_nil() {
                        cwi.write(None)
                    } else {
                        let array = ruby_field
                            .try_convert_to::<Array>()
                            .map_err(VM::raise_ex)
                            .unwrap();

                        let bool_vec = array
                            .into_iter()
                            .map(|ele| {
                                ele.try_convert_to::<Boolean>()
                                    .map_err(VM::raise_ex)
                                    .unwrap()
                                    .to_bool()
                            })
                            .collect::<Vec<_>>();

                        cwi.write(Some(bool_vec.as_slice()))
                    };

                    if let Err(e) = write_result {
                        VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                    }
                }
                tracklib2::write::section::writer::ColumnWriter::U64ArrayColumnWriter(cwi) => {
                    let ruby_field_name =
                        RString::from(String::from(cwi.field_definition().name()));
                    let ruby_field = ruby_row.at(&ruby_field_name);

                    let write_result = if ruby_field.is_nil() {
                        cwi.write(None)
                    } else {
                        let array = ruby_field
                            .try_convert_to::<Array>()
                            .map_err(VM::raise_ex)
                            .unwrap();

                        let u64_vec = array
                            .into_iter()
                            .map(|ele| match ele.try_convert_to::<Integer>() {
                                Ok(i) => i.to_u64(),
                                Err(int_e) => ele
                                    .try_convert_to::<Float>()
                                    .map_err(|_| VM::raise_ex(int_e))
                                    .unwrap()
                                    .to_f64()
                                    .round() as u64,
                            })
                            .collect::<Vec<_>>();

                        cwi.write(Some(u64_vec.as_slice()))
                    };

                    if let Err(e) = write_result {
                        VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                    }
                }
                tracklib2::write::section::writer::ColumnWriter::ByteArrayColumnWriter(cwi) => {
                    let ruby_field_name =
                        RString::from(String::from(cwi.field_definition().name()));
                    let ruby_field = ruby_row.at(&ruby_field_name);

                    let write_result = if ruby_field.is_nil() {
                        cwi.write(None)
                    } else {
                        cwi.write(Some(
                            ruby_field
                                .try_convert_to::<RString>()
                                .map_err(VM::raise_ex)
                                .unwrap()
                                .to_bytes_unchecked(),
                        ))
                    };

                    if let Err(e) = write_result {
                        VM::raise(Class::from_existing("Exception"), &format!("{:?}", e));
                    }
                }
            }
        }
    }
}

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
        let metadata_array = metadata.map_err(VM::raise_ex).unwrap();

        let metadata_entries = metadata_array
            .into_iter()
            .map(|metadata_ele| {
                let metadata_ele_array = metadata_ele
                    .try_convert_to::<Array>()
                    .map_err(VM::raise_ex)
                    .unwrap();
                if metadata_ele_array.length() >= 1 {
                    let metadata_type = metadata_ele_array
                        .at(0)
                        .try_convert_to::<Symbol>()
                        .map_err(VM::raise_ex)
                        .unwrap();
                    match metadata_type.to_str() {
                        "track_type" => {
                            if metadata_ele_array.length() == 3 {
                                let track_type_symbol = metadata_ele_array
                                    .at(1)
                                    .try_convert_to::<Symbol>()
                                    .map_err(VM::raise_ex)
                                    .unwrap();
                                let track_id = metadata_ele_array
                                    .at(2)
                                    .try_convert_to::<Integer>()
                                    .map_err(VM::raise_ex)
                                    .unwrap()
                                    .to_u64();

                                let track_type = match track_type_symbol.to_str() {
                                    "route" => tracklib2::types::TrackType::Route(track_id),
                                    "trip" => tracklib2::types::TrackType::Trip(track_id),
                                    "segment" => tracklib2::types::TrackType::Segment(track_id),
                                    val => {
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
                                    .map_err(VM::raise_ex)
                                    .unwrap();
                                let created_at_val = created_at_time_obj
                                    .protect_send("utc", &[])
                                    .map_err(VM::raise_ex)
                                    .unwrap()
                                    .protect_send("to_i", &[])
                                    .map_err(VM::raise_ex)
                                    .unwrap()
                                    .try_convert_to::<Integer>()
                                    .map_err(VM::raise_ex)
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
                        val => {
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

        let sections_array = sections.map_err(VM::raise_ex).unwrap();

        let section_wrappers = sections_array
            .into_iter()
            .map(|ruby_section| {
                ruby_section
                    .try_convert_to::<Section>()
                    .map_err(VM::raise_ex)
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let tracklib_sections = section_wrappers
            .iter()
            .map(|section_wrapper| &section_wrapper.get_data(&*SECTION_WRAPPER_INSTANCE).section)
            .collect::<Vec<_>>();

        let mut buf = vec![];
        tracklib2::write::track::write_track(&mut buf, &metadata_entries, &tracklib_sections)
            .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{:?}", e)))
            .unwrap();

        let encoding = Encoding::find("ASCII-8BIT").map_err(VM::raise_ex).unwrap();

        RString::from_bytes(&buf, &encoding)
    }
);
