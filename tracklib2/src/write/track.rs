use super::data_table::write_data_table;
use super::metadata::write_metadata;
use super::section::{Section, SectionInternal};
use crate::consts::RWTF_HEADER_SIZE;
use crate::error::Result;
use crate::types::MetadataEntry;
use std::io::{self, Write};

pub fn write_track<W: Write>(
    out: &mut W,
    metadata_entries: &[MetadataEntry],
    sections: &[&Section],
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
    write_data_table(out, sections)?;

    // now write out all the data sections
    for section in sections {
        match section {
            Section::Standard(section) => section.write(out)?,
            Section::Encrypted(section) => section.write(out)?,
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;
    use crate::types::{FieldValue, TrackType};
    use crate::write::section::writer::ColumnWriter;
    use crate::write::section::{encrypted, standard, SectionWrite};
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
        let mut section1 = standard::Section::new(Schema::with_fields(vec![
            FieldDefinition::new("i64", DataType::I64),
            FieldDefinition::new("f64:2", DataType::F64 { scale: 2 }),
            FieldDefinition::new("u64", DataType::U64),
            FieldDefinition::new("bool", DataType::Bool),
            FieldDefinition::new("string", DataType::String),
            FieldDefinition::new("bool array", DataType::BoolArray),
            FieldDefinition::new("u64 array", DataType::U64Array),
            FieldDefinition::new("byte array", DataType::ByteArray),
        ]));

        for _ in 0..5 {
            let mut rowbuilder = section1.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                match cw {
                    ColumnWriter::I64ColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&42)).is_ok());
                    }
                    ColumnWriter::U64ColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&25)).is_ok());
                    }
                    ColumnWriter::BoolColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&true)).is_ok());
                    }
                    ColumnWriter::StringColumnWriter(cwi) => {
                        assert!(cwi.write(Some("hey")).is_ok());
                    }
                    ColumnWriter::F64ColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&0.7890123)).is_ok());
                    }
                    ColumnWriter::BoolArrayColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&[true])).is_ok());
                    }
                    ColumnWriter::U64ArrayColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&[12, 10, 13])).is_ok());
                    }
                    ColumnWriter::ByteArrayColumnWriter(cwi) => {
                        assert!(cwi.write(Some(&[12, 10, 13])).is_ok());
                    }
                }
            }
        }

        let mut v = Vec::new();
        let mut h = HashMap::new();
        h.insert("a", FieldValue::I64(1));
        h.insert("b", FieldValue::Bool(false));
        h.insert("c", FieldValue::String("Ride".to_string()));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", FieldValue::I64(2));
        h.insert("c", FieldValue::String("with".to_string()));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", FieldValue::I64(4));
        h.insert("b", FieldValue::Bool(true));
        h.insert("c", FieldValue::String("GPS".to_string()));
        v.push(h);

        let orion_key = orion::aead::SecretKey::default();
        let key_material = orion_key.unprotected_as_bytes();
        let mut section2 = encrypted::Section::new(
            key_material,
            Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
            ]),
        )
        .unwrap();

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
                                        FieldValue::I64(v) => Some(v),
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
                                        FieldValue::Bool(v) => Some(v),
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
                                        FieldValue::String(v) => Some(v.as_str()),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::U64ColumnWriter(_) => {}
                        ColumnWriter::F64ColumnWriter(_) => {}
                        ColumnWriter::BoolArrayColumnWriter(_) => {}
                        ColumnWriter::U64ArrayColumnWriter(_) => {}
                        ColumnWriter::ByteArrayColumnWriter(_) => {}
                    }
                });
            }
        }

        let mut buf = Vec::new();
        assert_matches!(write_track(&mut buf,
                                    &[MetadataEntry::TrackType(TrackType::Segment(5)),
                                      MetadataEntry::CreatedAt(25)],
                                    &[&Section::Standard(section1), &Section::Encrypted(section2)]), Ok(()) => {
            // std::fs::write("example.rwtf", &buf).unwrap();

            #[rustfmt::skip]
            let header_bytes = &[
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
                0x22, // data offset
                0x00,
                0x00, // e reserve
                0x00,
                0x88, // header crc
                0x64,
            ];

            #[rustfmt::skip]
            let metadata_bytes = &[
                0x02, // one entry
                0x00, // entry type: track_type
                0x02, // entry size
                0x02, // track type: segment
                0x05, // segment id
                0x01, // entry type: created_at
                0x01, // entry size
                0x19, // timestamp
                0xD7, // crc
                0x59,
            ];

            #[rustfmt::skip]
            let data_table_bytes = &[
                0x02, // two sections

                // Data Table Section 1
                0x00, // section encoding = standard
                0x05, // leb128 point count
                0x84, // leb128 data size
                0x01,

                // Schema for Section 1
                0x00, // schema version
                0x08, // field count
                0x00, // first field type = I64
                0x03, // name len
                b'i', // name
                b'6',
                b'4',
                0x09, // data size
                0x01, // second field type = F64
                0x02, // scale
                0x05, // name len
                b'f', // name
                b'6',
                b'4',
                b':',
                b'2',
                0x0A, // data len
                0x02, // third field type = U64
                0x03, // name len
                b'u', // name
                b'6',
                b'4',
                0x09, // data len
                0x10, // fourth field type = Bool
                0x04, // name len
                b'b', // name
                b'o',
                b'o',
                b'l',
                0x09, // data len
                0x20, // fifth field type = String
                0x06, // name len
                b's', // name
                b't',
                b'r',
                b'i',
                b'n',
                b'g',
                0x18, // data len
                0x21, // sixth field type = Bool Array
                0x0A, // name len
                b'b', // name
                b'o',
                b'o',
                b'l',
                b' ',
                b'a',
                b'r',
                b'r',
                b'a',
                b'y',
                0x0E, // data len
                0x22, // seventh field type = U64 Array
                0x09, // name len
                b'u', // name
                b'6',
                b'4',
                b' ',
                b'a',
                b'r',
                b'r',
                b'a',
                b'y',
                0x18, // data len
                0x23, // eigth field type = Byte Array
                0x0A, // name len
                b'b', // name
                b'y',
                b't',
                b'e',
                b' ',
                b'a',
                b'r',
                b'r',
                b'a',
                b'y',
                0x18, // data len

                // Data Table Section 2
                0x01, // section encoding = encrypted
                0x03, // leb128 point count
                0x4E, // leb128 data size

                // Schema for Section 2
                0x00, // schema version
                0x03, // field count
                0x00, // first field type = I64
                0x01, // name length
                b'a', // name
                0x07, // leb128 data size
                0x10, // second field type = Bool
                0x01, // name length
                b'b', // name
                0x06, // leb128 data size
                0x20, // third field type = String
                0x01, // name length
                b'c', // name
                0x12, // leb128 data size

                // Data Table CRC
                0xF4,
                0x07,
            ];

            #[rustfmt::skip]
            let data_section_1_bytes = &[
                // Presence Column
                0b11111111,
                0b11111111,
                0b11111111,
                0b11111111,
                0b11111111,
                0x4B, // crc
                0xBF,
                0x08,
                0x4E,

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

                // Data Column 2 = F64
                0xCE, // 0.78
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x3C, // crc
                0x2E,
                0x7B,
                0x33,

                // Data Column 3 = U64
                0x19, // 25
                0x00,
                0x00,
                0x00,
                0x00,
                0xE4, // crc
                0x2A,
                0xD9,
                0x33,

                // Data Column 4 = Bool
                0x01, // true
                0x01, // true
                0x01, // true
                0x01, // true
                0x01, // true
                0xB5, // crc
                0xC9,
                0x8F,
                0xFA,

                // Data Column 5 = String
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

                // Data Column 6 = Bool Array
                0x01, // array len
                0x01, // true
                0x01, // array len
                0x01, // true
                0x01, // array len
                0x01, // true
                0x01, // array len
                0x01, // true
                0x01, // array len
                0x01, // true
                0xB3, // crc
                0x6F,
                0x38,
                0x51,

                // Data Column 7 = U64 Array
                0x03, // array len
                0x0C, // 12
                0x7E, // -2
                0x03, // +3
                0x03, // array len
                0x0C, // 12
                0x7E, // -2
                0x03, // +3
                0x03, // array len
                0x0C, // 12
                0x7E, // -2
                0x03, // +3
                0x03, // array len
                0x0C, // 12
                0x7E, // -2
                0x03, // +3
                0x03, // array len
                0x0C, // 12
                0x7E, // -2
                0x03, // +3
                0xD1, // crc
                0xB4,
                0x14,
                0x37,

                // Data Column 8 = Byte Array
                0x03, // array len
                0x0C, // 12
                0x0A, // 10
                0x0D, // 13
                0x03, // array len
                0x0C, // 12
                0x0A, // 10
                0x0D, // 13
                0x03, // array len
                0x0C, // 12
                0x0A, // 10
                0x0D, // 13
                0x03, // array len
                0x0C, // 12
                0x0A, // 10
                0x0D, // 13
                0x03, // array len
                0x0C, // 12
                0x0A, // 10
                0x0D, // 13
                0x94, // crc
                0x1D,
                0x88,
                0xAB,
            ];

            #[rustfmt::skip]
            let data_section_2_bytes = &[
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
                0x48,
            ];

            // most of the track can be compared as-is
            let mut expected = vec![];
            expected.extend_from_slice(header_bytes);
            expected.extend_from_slice(metadata_bytes);
            expected.extend_from_slice(data_table_bytes);
            expected.extend_from_slice(data_section_1_bytes);
            assert_eq!(buf[..expected.len()], expected);

            // section 2 has to be decrypted first, and then compared with the reference
            let decrypted_section_2_bytes = crate::util::decrypt(key_material, &buf[expected.len()..]).unwrap();
            assert_eq!(decrypted_section_2_bytes, data_section_2_bytes);
        });
    }
}
