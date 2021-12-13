use super::crcwriter::CrcWriter;
use super::section::Section;
use crate::error::Result;
use std::convert::TryFrom;
use std::io::Write;

#[rustfmt::skip]
pub(crate) fn write_data_table<W: Write>(out: &mut W, sections: &[Section]) -> Result<()> {
    let mut crcwriter = CrcWriter::new16(out);

    crcwriter.write_all(&u8::try_from(sections.len())?.to_le_bytes())?;                // 1 byte  - number of sections
    for section in sections.iter() {
        crcwriter.write_all(&section.type_tag().to_le_bytes())?;                       // 1 byte  - section type
        leb128::write::unsigned(&mut crcwriter, u64::try_from(section.rows())?)?;      // ? bytes - number of points in this section
        leb128::write::unsigned(&mut crcwriter, u64::try_from(section.data_size())?)?; // ? bytes - leb128 section size
        section.write_types_table(&mut crcwriter)?;                                    // ? bytes - types table
    }
    crcwriter.append_crc()?;                                                           // 2 bytes - crc

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FieldType;
    use crate::write::section::SectionType;
    use assert_matches::assert_matches;

    #[test]
    fn test_write_empty_data_table() {
        let mut buf = Vec::new();
        assert_matches!(write_data_table(&mut buf, &[]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf, &[0x00, // zero entries
                              0x40, // crc
                              0xBF]);
        });
    }

    #[test]
    fn test_data_table() {
        let section1 = Section::new(
            SectionType::TrackPoints,
            vec![
                ("a".to_string(), FieldType::I64),
                ("b".to_string(), FieldType::Bool),
                ("c".to_string(), FieldType::String),
            ],
        );

        let section2 = Section::new(
            SectionType::CoursePoints,
            vec![
                ("Ride".to_string(), FieldType::I64),
                ("with".to_string(), FieldType::Bool),
                ("GPS".to_string(), FieldType::String),
            ],
        );

        let mut buf = Vec::new();
        assert_matches!(write_data_table(&mut buf, &[section1, section2]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x02, // number of sections

                         // Section 1
                         0x00, // section type = track points
                         0x00, // leb128 section point count
                         0x00, // leb128 section data size
                         // Types Table
                         0x03, // field count
                         0x00, // first field type = I64
                         0x01, // name length
                         b'a', // name
                         0x00, // leb128 data size
                         0x05, // second field type = Bool
                         0x01, // name length
                         b'b', // name
                         0x00, // leb128 data size
                         0x04, // third field type = String
                         0x01, // name length
                         b'c', // name
                         0x00, // leb128 data size


                         // Section 2
                         0x01, // section type = course points
                         0x00, // leb128 section point count
                         0x00, // leb128 section data size

                         // Types Table
                         0x03, // field count
                         0x00, // first field type = I64
                         0x04, // name length
                         b'R', // name
                         b'i', // name
                         b'd', // name
                         b'e', // name
                         0x00, // leb128 data size
                         0x05, // second field type = Bool
                         0x04, // name length
                         b'w', // name
                         b'i', // name
                         b't', // name
                         b'h', // name
                         0x00, // leb128 data size
                         0x04, // third field type = String
                         0x03, // name length
                         b'G', // name
                         b'P', // name
                         b'S', // name
                         0x00, // leb128 data size


                         0x4E, // crc
                         0x88], "{:#04X?}", buf);
        });
    }
}
