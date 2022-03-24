use super::data_table::write_data_table;
use super::metadata::write_metadata;
use super::section::Section;
use crate::consts::RWTF_HEADER_SIZE;
use crate::error::Result;
use crate::types::MetadataEntry;
use std::convert::TryFrom;
use std::io::{self, Write};

pub fn write_track<W: Write>(
    out: &mut W,
    metadata_entries: &[MetadataEntry],
    sections: &[Section],
) -> Result<()> {
    // write metadata to a buffer so we can measure its size to use in the file header
    let mut metadata_buf = Vec::new();
    write_metadata(&mut metadata_buf, metadata_entries)?;

    // write header
    super::header::write_header(
        out,
        RWTF_HEADER_SIZE,
        RWTF_HEADER_SIZE + u16::try_from(metadata_buf.len())?,
    )?;

    // copy metadata buffer to out
    io::copy(&mut io::Cursor::new(metadata_buf), out)?;

    // write the data table
    write_data_table(out, &sections)?;

    // now write out all the data sections
    for section in sections {
        section.write(out)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;
    use crate::types::{SectionType, TrackType};
    use crate::write::section::ColumnWriter;
    use assert_matches::assert_matches;
    use std::collections::HashMap;

    #[test]
    fn test_empty_track() {
        let mut buf = Vec::new();
        assert_matches!(write_track(&mut buf, &[], &[]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf, &[
                // Header
                0x89, // rwtfmagic
                0x52,
                0x57,
                0x54,
                0x46,
                0x0A,
                0x1A,
                0x0A,
                0x01, // file version
                0x00, // fv reserve
                0x00,
                0x00,
                0x00, // creator version
                0x00, // cv reserve
                0x00,
                0x00,
                0x18, // metadata table offset
                0x00,
                0x1B, // data offset
                0x00,
                0x00, // e reserve
                0x00,
                0x84, // header crc
                0xF8,

                // Metadata Table
                0x00, // zero metadata entries
                0x40, // crc
                0xBF,

                // Data Table
                0x00, // zero sections
                0x40, // crc
                0xBF]);
        });
    }

    #[test]
    fn test_write_a_track() {
        let mut section1 = Section::new(
            SectionType::CoursePoints,
            Schema::with_fields(vec![
                FieldDefinition::new("m", DataType::I64),
                FieldDefinition::new("k", DataType::Bool),
                FieldDefinition::new("j", DataType::String),
            ]),
        );

        for _ in 0..5 {
            let mut rowbuilder = section1.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                match cw {
                    ColumnWriter::I64ColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&42)).is_ok());
                    }
                    ColumnWriter::BoolColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&true)).is_ok());
                    }
                    ColumnWriter::StringColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&"hey".to_string())).is_ok());
                    }
                    ColumnWriter::F64ColumnWriter(_) => {}
                }
            }
        }

        enum V {
            I64(i64),
            Bool(bool),
            String(String),
        }

        let mut v = Vec::new();
        let mut h = HashMap::new();
        h.insert("a", V::I64(1));
        h.insert("b", V::Bool(false));
        h.insert("c", V::String("Ride".to_string()));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", V::I64(2));
        h.insert("c", V::String("with".to_string()));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", V::I64(4));
        h.insert("b", V::Bool(true));
        h.insert("c", V::String("GPS".to_string()));
        v.push(h);

        let mut section2 = Section::new(
            SectionType::TrackPoints,
            Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
            ]),
        );

        let fields = section2.schema().fields().to_vec();

        for entry in v {
            let mut rowbuilder = section2.open_row_builder();

            for field_def in fields.iter() {
                assert_matches!(rowbuilder.next_column_writer(), Some(cw) => {
                    match cw {
                        ColumnWriter::I64ColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        V::I64(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::BoolColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        V::Bool(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::StringColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        V::String(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::F64ColumnWriter(_) => {}
                    }
                });
            }
        }

        let mut buf = Vec::new();
        assert_matches!(write_track(&mut buf,
                                    &[MetadataEntry::TrackType(TrackType::Segment(5))],
                                    &[section1, section2]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf, &[
                // Header
                0x89, // rwtfmagic
                0x52,
                0x57,
                0x54,
                0x46,
                0x0A,
                0x1A,
                0x0A,
                0x01, // file version
                0x00, // fv reserve
                0x00,
                0x00,
                0x00, // creator version
                0x00, // cv reserve
                0x00,
                0x00,
                0x18, // metadata table offset
                0x00,
                0x23, // data offset
                0x00,
                0x00, // e reserve
                0x00,
                0x89, // header crc
                0x98,

                // Metadata Table
                0x01, // one entry
                0x00, // entry type: track_type = 0x00
                0x05, // two byte entry size = 5
                0x00,
                0x02, // track type: segment = 0x02
                0x05, // four byte segment ID
                0x00,
                0x00,
                0x00,
                0xD4, // crc
                0x93,

                // Data Table
                0x02, // two sections

                // Data Table Section 1
                0x01, // type of section = course points
                0x05, // leb128 point count
                0x33, // leb128 data size

                // Types Table for Section 1
                0x03, // field count
                0x00, // first field type = I64
                0x01, // name len
                b'm', // name
                0x09, // leb128 data size
                0x05, // second field type = Bool
                0x01, // name len
                b'k', // name
                0x09, // leb128 data size
                0x04, // third field type = String
                0x01, // name len
                b'j', // name
                0x18, // leb128 data size

                // Data Table Section 2
                0x00, // type of section = track points
                0x03, // leb128 point count
                0x26, // leb128 data size

                // Types Table for Section 2
                0x03, // field count
                0x00, // first field type = I64
                0x01, // name length
                b'a', // name
                0x07, // leb128 data size
                0x05, // second field type = Bool
                0x01, // name length
                b'b', // name
                0x06, // leb128 data size
                0x04, // third field type = String
                0x01, // name length
                b'c', // name
                0x12, // leb128 data size

                // Data Table CRC
                0x49,
                0xEC,

                // Data Section 1

                // Presence Column
                0b00000111,
                0b00000111,
                0b00000111,
                0b00000111,
                0b00000111,
                0xF6, // crc
                0xF8,
                0x0D,
                0x73,

                // Data Column 1 = I64
                0x2A, // 42
                0x00, // no change
                0x00, // no change
                0x00, // no change
                0x00, // no change
                0xD0, // crc
                0x8D,
                0x79,
                0x68,

                // Data Column 2 = Bool
                0x01, // true
                0x01, // true
                0x01, // true
                0x01, // true
                0x01, // true
                0xB5, // crc
                0xC9,
                0x8F,
                0xFA,

                // Data Column 3 = String
                0x03, // length 3
                b'h',
                b'e',
                b'y',
                0x03, // length 3
                b'h',
                b'e',
                b'y',
                0x03, // length 3
                b'h',
                b'e',
                b'y',
                0x03, // length 3
                b'h',
                b'e',
                b'y',
                0x03, // length 3
                b'h',
                b'e',
                b'y',
                0x36, // crc
                0x71,
                0x24,
                0x0B,

                // Data Section 2

                // Presence Column
                0b00000111,
                0b00000101,
                0b00000111,
                0x1A, // crc
                0x75,
                0xEA,
                0xC4,

                // Data Column 1 = I64
                0x01, // 1
                0x01, // 2
                0x02, // 4
                0xCA, // crc
                0xD4,
                0xD8,
                0x92,

                // Data Column 2 = Bool
                0x00, // false
                // None
                0x01, // true
                0x35, // crc
                0x86,
                0x89,
                0xFB,

                // Data Column 3 = String
                0x04, // length 4
                b'R',
                b'i',
                b'd',
                b'e',
                0x04, // length 4
                b'w',
                b'i',
                b't',
                b'h',
                0x03, // length 3
                b'G',
                b'P',
                b'S',
                0xA3, // crc
                0x02,
                0xEC,
                0x48]);
        });
    }
}
