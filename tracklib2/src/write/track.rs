use super::metadata::write_metadata;
use super::section::Section;
use crate::consts::{CRC16, RWTF_HEADER_SIZE};
use crate::error::Result;
use crate::types::MetadataEntry;
use std::convert::TryFrom;
use std::io::{self, Write};

#[rustfmt::skip]
fn write_data_table<W: Write>(out: &mut W, section_bufs: &[Vec<u8>]) -> Result<usize> {
    let mut buf = Vec::new();

    buf.write_all(&u8::try_from(section_bufs.len())?.to_le_bytes())?;      // 1 byte  - number of sections

    for section in section_bufs.iter() {
        leb128::write::unsigned(&mut buf, u64::try_from(section.len())?)?; // ? bytes - leb128 section size
    }

    buf.write_all(&CRC16.checksum(&buf).to_le_bytes())?;                   // 2 bytes - crc

    out.write_all(&buf)?;
    Ok(buf.len())
}

pub fn write_track<W: Write>(
    out: &mut W,
    metadata_entries: &[MetadataEntry],
    sections: &[Section],
) -> Result<usize> {
    let mut bytes_written = 0;

    // write metadata to a buffer so we can measure its size to use in the file header
    let mut metadata_buf = Vec::new();
    write_metadata(&mut metadata_buf, metadata_entries)?;

    // write header
    bytes_written += super::header::write_header(
        out,
        RWTF_HEADER_SIZE,
        RWTF_HEADER_SIZE + u16::try_from(metadata_buf.len())?,
    )?;

    // copy metadata buffer to out
    bytes_written += usize::try_from(io::copy(&mut io::Cursor::new(metadata_buf), out)?)?;

    // create bufs for all sections
    let section_bufs: Vec<Vec<u8>> = sections
        .iter()
        .map(|section| {
            let mut buf = Vec::new();
            section.write(&mut buf)?;
            Ok(buf)
        })
        .collect::<Result<_>>()?;

    // write the data table
    bytes_written += write_data_table(out, &section_bufs)?;

    // now write out all the data sections
    for section in section_bufs {
        bytes_written += usize::try_from(io::copy(&mut io::Cursor::new(section), out)?)?;
    }

    Ok(bytes_written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FieldType;
    use crate::write::section::{ColumnWriter, SectionType};
    use assert_matches::assert_matches;
    use std::collections::HashMap;

    #[test]
    fn test_write_empty_data_table() {
        let mut buf = Vec::new();
        let bytes_written = write_data_table(&mut buf, &[]);
        assert!(bytes_written.is_ok());
        assert_eq!(bytes_written.unwrap(), buf.len());
        #[rustfmt::skip]
        assert_eq!(buf, &[0x00, // zero entries
                          0x40, // crc
                          0xBF]);
    }

    #[test]
    fn test_empty_track() {
        let mut buf = Vec::new();
        let bytes_written = write_track(&mut buf, &[], &[]);
        assert!(bytes_written.is_ok());
        assert_eq!(bytes_written.unwrap(), buf.len());
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
    }

    #[test]
    fn test_write_a_track() {
        let mut section1 = Section::new(
            SectionType::CoursePoints,
            vec![
                ("m".to_string(), FieldType::I64),
                ("k".to_string(), FieldType::Bool),
                ("j".to_string(), FieldType::String),
            ],
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
            vec![
                ("a".to_string(), FieldType::I64),
                ("b".to_string(), FieldType::Bool),
                ("c".to_string(), FieldType::String),
            ],
        );

        let mapping = section2.fields().to_vec();

        for entry in v {
            let mut rowbuilder = section2.open_row_builder();

            for field_desc in mapping.iter() {
                assert_matches!(rowbuilder.next_column_writer(), Some(cw) => {
                    match cw {
                        ColumnWriter::I64ColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_desc.name())
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
                                    .get(field_desc.name())
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
                                    .get(field_desc.name())
                                    .map(|v| match v {
                                        V::String(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                    }
                });
            }
        }

        let mut buf = Vec::new();
        let bytes_written = write_track(&mut buf, &[], &[section1, section2]);
        assert!(bytes_written.is_ok());
        assert_eq!(bytes_written.unwrap(), buf.len());
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
            0x00, // zero entries
            0x40, // crc
            0xBF,

            // Data Table
            0x02, // two entries
            0x39, // size of first entry
            0x2D, // size of second entry
            0xFD, // crc
            0xB2,


            // Section 1
            0x01, // section type = course points
            0x05, // point count
            0x00,
            0x00,
            0x00,

            // Types Table
            0x03, // field count
            0x00, // first field type = I64
            0x01, // name len
            b'm', // name
            0x05, // leb128 data size
            0x05, // second field type = Bool
            0x01, // name len
            b'k', // name
            0x05, // leb128 data size
            0x04, // third field type = String
            0x01, // name len
            b'j', // name
            0x14, // leb128 data size

            // Presence Column
            0b00000111,
            0b00000111,
            0b00000111,
            0b00000111,
            0b00000111,

            // Data Column 1 = I64
            0x2A, // 42
            0x00, // no change
            0x00, // no change
            0x00, // no change
            0x00, // no change

            // Data Column 2 = Bool
            0x01, // true
            0x01, // true
            0x01, // true
            0x01, // true
            0x01, // true

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

            // CRC
            0x5B,
            0xC3,
            0x0F,
            0x6E,


            // Section 2
            0x00, // section type = track points
            0x03, // point count
            0x00,
            0x00,
            0x00,

            // Types Table
            0x03, // field count
            0x00, // first field type = I64
            0x01, // name length
            b'a', // name
            0x03, // leb128 data size
            0x05, // second field type = Bool
            0x01, // name length
            b'b', // name
            0x03, // leb128 data size
            0x04, // third field type = String
            0x01, // name length
            b'c', // name
            0x0E, // leb128 data size

            // Presence Column
            0b00000111,
            0b00000101,
            0b00000111,

            // Data Column 1 = I64
            0x01, // 1
            0x01, // 2
            0x02, // 4

            // Data Column 2 = Bool
            0x00, // false
            0x00, // missing
            0x01, // true

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

            // CRC
            0xAF,
            0xEC,
            0x7D,
            0x70]);
    }
}
