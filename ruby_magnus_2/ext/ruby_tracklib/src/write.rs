use crate::schema::Schema;
use magnus::{typed_data::Obj, value::ReprValue, wrap, Error, RArray, RHash, RString, Ruby, TryConvert};
use tracklib::write::section::SectionWrite;

#[wrap(class = "Tracklib::Section", size)]
pub struct Section {
    section: tracklib::write::section::Section,
}

impl Section {
    pub fn create_standard(handle: &Ruby, base_schema: &Schema, data: RArray) -> Result<Self, Error> {
        let trimmed_schema = base_schema.trim(handle, data)?;
        let mut section = tracklib::write::section::standard::Section::new(trimmed_schema);
        write_ruby_array_into_section(handle, &mut section, data)?;

        Ok(Self {
            section: tracklib::write::section::Section::Standard(section),
        })
    }

    pub fn create_encrypted(
        handle: &Ruby,
        base_schema: &Schema,
        data: RArray,
        key_material: RString,
    ) -> Result<Self, Error> {
        let trimmed_schema = base_schema.trim(handle, data)?;
        let key_bytes = unsafe { key_material.as_slice().to_vec() };
        let mut section = tracklib::write::section::encrypted::Section::new(&key_bytes, trimmed_schema)
            .map_err(|e| Error::new(handle.exception_io_error(), format!("Error creating section: {e:?}")))?;
        write_ruby_array_into_section(handle, &mut section, data)?;

        Ok(Self {
            section: tracklib::write::section::Section::Encrypted(section),
        })
    }
}

fn write_ruby_array_into_section<SW: SectionWrite>(handle: &Ruby, section: &mut SW, data: RArray) -> Result<(), Error> {
    let checked_data = data.typecheck::<RHash>()?;
    for row in checked_data {
        let mut rowbuilder = section.open_row_builder();

        while let Some(column_writer) = rowbuilder.next_column_writer() {
            match column_writer {
                tracklib::write::section::writer::ColumnWriter::I64ColumnWriter(cwi) => {
                    let v = row
                        .get(cwi.field_definition().name())
                        .map(|value| {
                            if value.is_kind_of(handle.class_integer()) {
                                i64::try_convert(value)
                            } else if value.is_kind_of(handle.class_float()) {
                                Ok(f64::try_convert(value)?.round() as i64)
                            } else {
                                Err(Error::new(
                                    handle.exception_type_error(),
                                    "Unable to convert unknown ruby type into i64",
                                ))
                            }
                        })
                        .transpose()?;
                    cwi.write(v.as_ref())
                        .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing field: {e:?}")))?;
                }

                tracklib::write::section::writer::ColumnWriter::F64ColumnWriter(cwi) => {
                    let v = row
                        .get(cwi.field_definition().name())
                        .map(|value| f64::try_convert(value))
                        .transpose()?;
                    cwi.write(v.as_ref())
                        .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing field: {e:?}")))?;
                }

                tracklib::write::section::writer::ColumnWriter::U64ColumnWriter(cwi) => {
                    let v = row
                        .get(cwi.field_definition().name())
                        .map(|value| {
                            if value.is_kind_of(handle.class_integer()) {
                                u64::try_convert(value)
                            } else if value.is_kind_of(handle.class_float()) {
                                Ok(f64::try_convert(value)?.round() as u64)
                            } else {
                                Err(Error::new(
                                    handle.exception_type_error(),
                                    "Unable to convert unknown ruby type into u64",
                                ))
                            }
                        })
                        .transpose()?;
                    cwi.write(v.as_ref())
                        .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing field: {e:?}")))?;
                }

                tracklib::write::section::writer::ColumnWriter::BoolColumnWriter(cwi) => {
                    let v = row
                        .get(cwi.field_definition().name())
                        .map(|value| bool::try_convert(value))
                        .transpose()?;
                    cwi.write(v.as_ref())
                        .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing field: {e:?}")))?;
                }

                tracklib::write::section::writer::ColumnWriter::StringColumnWriter(cwi) => {
                    let v = row
                        .get(cwi.field_definition().name())
                        .map(|value| String::try_convert(value))
                        .transpose()?;
                    cwi.write(v.as_deref())
                        .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing field: {e:?}")))?;
                }

                tracklib::write::section::writer::ColumnWriter::BoolArrayColumnWriter(cwi) => {
                    let v = row
                        .get(cwi.field_definition().name())
                        .map(|value| Vec::<bool>::try_convert(value))
                        .transpose()?;
                    cwi.write(v.as_deref())
                        .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing field: {e:?}")))?;
                }

                tracklib::write::section::writer::ColumnWriter::U64ArrayColumnWriter(cwi) => {
                    let v = row
                        .get(cwi.field_definition().name())
                        .map(|value| {
                            RArray::try_convert(value)?
                                .into_iter()
                                .map(|value| {
                                    if value.is_kind_of(handle.class_integer()) {
                                        u64::try_convert(value)
                                    } else if value.is_kind_of(handle.class_float()) {
                                        Ok(f64::try_convert(value)?.round() as u64)
                                    } else {
                                        Err(Error::new(
                                            handle.exception_type_error(),
                                            "Unable to convert unknown ruby type into u64",
                                        ))
                                    }
                                })
                                .collect::<Result<Vec<u64>, Error>>()
                        })
                        .transpose()?;
                    cwi.write(v.as_deref())
                        .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing field: {e:?}")))?;
                }

                tracklib::write::section::writer::ColumnWriter::ByteArrayColumnWriter(cwi) => {
                    let v = row
                        .get(cwi.field_definition().name())
                        .map(|value| {
                            let s = RString::try_convert(value)?;
                            Ok::<_, Error>(unsafe { s.as_slice().to_vec() })
                        })
                        .transpose()?;
                    cwi.write(v.as_deref())
                        .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing field: {e:?}")))?;
                }
            }
        }
    }

    Ok(())
}

pub fn write_track(handle: &Ruby, metadata: RArray, sections: RArray) -> Result<RString, Error> {
    let metadata_entries = metadata
        .into_iter()
        .map(|entry| {
            let entry_array: RArray = TryConvert::try_convert(entry)?;

            let entry_type: magnus::symbol::Symbol = entry_array.entry(0)?;

            match core::ops::Deref::deref(&entry_type.name()?) {
                "track_type" => {
                    let track_type_symbol: magnus::symbol::Symbol = entry_array.entry(1)?;
                    let track_id: u64 = entry_array.entry(2)?;

                    match core::ops::Deref::deref(&track_type_symbol.name()?) {
                        "route" => Ok(tracklib::types::MetadataEntry::TrackType(
                            tracklib::types::TrackType::Route(track_id),
                        )),
                        "trip" => Ok(tracklib::types::MetadataEntry::TrackType(
                            tracklib::types::TrackType::Trip(track_id),
                        )),
                        "segment" => Ok(tracklib::types::MetadataEntry::TrackType(
                            tracklib::types::TrackType::Segment(track_id),
                        )),
                        val => Err(Error::new(
                            handle.exception_arg_error(),
                            format!("Metadata Entry Track Type '{val}' unknown"),
                        )),
                    }
                }
                "created_at" => {
                    let created_at_time_obj: magnus::Time = entry_array.entry(1)?;
                    let created_at_val = created_at_time_obj
                        .funcall_public::<_, _, magnus::Time>("utc", ())?
                        .funcall_public::<_, _, u64>("to_i", ())?;
                    Ok(tracklib::types::MetadataEntry::CreatedAt(created_at_val))
                }
                val => Err(Error::new(
                    handle.exception_arg_error(),
                    format!("Metadata Type '{val}' unknown"),
                )),
            }
        })
        .collect::<Result<Vec<_>, Error>>()?;

    let typed_sections = sections.typecheck::<Obj<Section>>()?;

    let mut writer = tracklib::write::track::TrackWriter::new();
    for entry in metadata_entries {
        writer
            .write_metadata(&entry)
            .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing metadata: {e:?}")))?;
    }
    for section in typed_sections {
        writer
            .write_section(&section.section)
            .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing section: {e:?}")))?;
    }
    let mut buf = vec![];
    writer
        .finish(&mut buf)
        .map_err(|e| Error::new(handle.exception_io_error(), format!("Error writing: {e:?}")))?;

    Ok(handle.enc_str_new(&buf, handle.ascii8bit_encoding()))
}
