use super::crcwriter::CrcWriter;
use super::section::Section;
use crate::error::Result;
use crate::types::SectionEncoding;
use std::io::Write;

impl SectionEncoding {
    fn type_tag(&self) -> u8 {
        match self {
            Self::Standard => 0x00,
        }
    }
}

#[rustfmt::skip]
pub(crate) fn write_data_table<W: Write>(out: &mut W, sections: &[&Section]) -> Result<()> {
    let mut crcwriter = CrcWriter::new16(out);

    crcwriter.write_all(&u8::try_from(sections.len())?.to_le_bytes())?;                // 1 byte  - number of sections
    for section in sections.iter() {
        crcwriter.write_all(&section.encoding().type_tag().to_le_bytes())?;            // 1 byte  - section encoding
        leb128::write::unsigned(&mut crcwriter, u64::try_from(section.rows())?)?;      // ? bytes - number of points in this section
        leb128::write::unsigned(&mut crcwriter, u64::try_from(section.data_size())?)?; // ? bytes - leb128 section size
        section.write_schema(&mut crcwriter)?;                                         // ? bytes - schema
    }
    crcwriter.append_crc()?;                                                           // 2 bytes - crc

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;
    use crate::types::SectionEncoding;
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
            SectionEncoding::Standard,
            Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
            ]),
        );

        let section2 = Section::new(
            SectionEncoding::Standard,
            Schema::with_fields(vec![
                FieldDefinition::new("Ride", DataType::I64),
                FieldDefinition::new("with", DataType::Bool),
                FieldDefinition::new("GPS", DataType::String),
            ]),
        );

        let mut buf = Vec::new();
        assert_matches!(write_data_table(&mut buf, &[&section1, &section2]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x02, // number of sections

                         // Section 1
                         0x00, // section encoding = standard
                         0x00, // leb128 section point count
                         0x10, // leb128 section data size
                         // Schema
                         0x00, // schema version
                         0x03, // field count
                         0x00, // first field type = I64
                         0x01, // name length
                         b'a', // name
                         0x04, // leb128 data size
                         0x05, // second field type = Bool
                         0x01, // name length
                         b'b', // name
                         0x04, // leb128 data size
                         0x04, // third field type = String
                         0x01, // name length
                         b'c', // name
                         0x04, // leb128 data size


                         // Section 2
                         0x00, // section encoding = standard
                         0x00, // leb128 section point count
                         0x10, // leb128 section data size

                         // Schema
                         0x00, // schema version
                         0x03, // field count
                         0x00, // first field type = I64
                         0x04, // name length
                         b'R', // name
                         b'i', // name
                         b'd', // name
                         b'e', // name
                         0x04, // leb128 data size
                         0x05, // second field type = Bool
                         0x04, // name length
                         b'w', // name
                         b'i', // name
                         b't', // name
                         b'h', // name
                         0x04, // leb128 data size
                         0x04, // third field type = String
                         0x03, // name length
                         b'G', // name
                         b'P', // name
                         b'S', // name
                         0x04, // leb128 data size

                         0x74, // crc
                         0xA4]);
        });
    }

    #[test]
    fn test_data_table_with_multibyte_character() {
        let section = Section::new(
            SectionEncoding::Standard,
            Schema::with_fields(vec![FieldDefinition::new("I ♥ NY", DataType::F64)]),
        );

        let mut buf = Vec::new();
        assert_matches!(write_data_table(&mut buf, &[&section]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x01, // number of sections

                         // Section 1
                         0x00, // section encoding = standard
                         0x00, // leb128 section point count
                         0x08, // leb128 section data size
                         // Schema
                         0x00, // schema version
                         0x01, // field count
                         0x01, // first field type = F64
                         0x08, // name length
                         b'I', // name
                         b' ',
                         0xE2, // heart
                         0x99,
                         0xA5,
                         b' ',
                         b'N',
                         b'Y',
                         0x04, // leb128 data size

                         0x35, // crc
                         0x13]);
        });
    }

    #[test]
    fn test_array_types() {
        let section = Section::new(
            SectionEncoding::Standard,
            Schema::with_fields(vec![FieldDefinition::new("a", DataType::BoolArray)]),
        );

        let mut buf = Vec::new();
        assert_matches!(write_data_table(&mut buf, &[&section]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x01, // number of sections

                         // Section 1
                         0x00, // section encoding = standard
                         0x00, // leb128 section point count
                         0x08, // leb128 section data size
                         // Schema
                         0x00, // schema version
                         0x01, // field count
                         0x06, // first field type = Array
                         0x05, // array subtype = Bool
                         0x01, // name length
                         b'a', // name
                         0x04, // leb128 data size

                         0x0C, // crc
                         0xCF]);
        });
    }
}
