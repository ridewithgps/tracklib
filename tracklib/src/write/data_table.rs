use super::crcwriter::CrcWriter;
use super::section::{Section, SectionInternal};
use crate::error::{Result, TracklibError};
use std::io::Write;

#[derive(Default)]
pub(crate) struct DataTableWriter {
    buf: Vec<u8>,
    count: u8,
}

impl DataTableWriter {
    #[rustfmt::skip]
    pub(crate) fn write_section(&mut self, section: &Section) -> Result<()> {
        match section {
            Section::Standard(section) => {
                section.write_encoding(&mut self.buf)?;                 // 1 byte  - section encoding
                section.write_rows(&mut self.buf)?;                     // ? bytes - number of points in this section
                section.write_data_size(&mut self.buf)?;                // ? bytes - leb128 section size
                section.write_schema(&mut self.buf)?;                   // ? bytes - schema
            }
            Section::Encrypted(section) => {
                section.write_encoding(&mut self.buf)?;                 // ? bytes - section encoding
                section.write_rows(&mut self.buf)?;                     // ? bytes - number of points in this section
                section.write_data_size(&mut self.buf)?;                // ? bytes - leb128 section size
                section.write_schema(&mut self.buf)?;                   // ? bytes - schema
            }
        }
        self.count = self.count.checked_add(1).ok_or(TracklibError::TooManyEntriesError)?;

        Ok(())
    }

    #[rustfmt::skip]
    pub(crate) fn finish<W: Write>(self, out: &mut W) -> Result<()> {
        let mut crcwriter = CrcWriter::new16(out);

        crcwriter.write_all(&self.count.to_le_bytes())?;                // 1 byte  - entry count
        crcwriter.write_all(&self.buf)?;                                // ? bytes - all sections' contents
        crcwriter.append_crc()?;                                        // 2 bytes - crc

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;
    use crate::write::section::{encrypted, standard};
    use assert_matches::assert_matches;

    #[test]
    fn test_write_empty_data_table() {
        let mut buf = Vec::new();
        let w = DataTableWriter::default();
        assert_matches!(w.finish(&mut buf), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf,
                   &[0x00, // zero entries
                     0x40, // crc
                     0xBF]);
    }

    #[test]
    fn test_data_table() {
        let section1 = standard::Section::new(Schema::with_fields(vec![
            FieldDefinition::new("a", DataType::I64),
            FieldDefinition::new("b", DataType::Bool),
            FieldDefinition::new("c", DataType::String),
        ]));

        let section2 = encrypted::Section::new(
            &crate::util::random_key_material(),
            Schema::with_fields(vec![
                FieldDefinition::new("Ride", DataType::I64),
                FieldDefinition::new("with", DataType::Bool),
                FieldDefinition::new("GPS", DataType::String),
            ]),
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut w = DataTableWriter::default();
        assert_matches!(w.write_section(&Section::Standard(section1)), Ok(()));
        assert_matches!(w.write_section(&Section::Encrypted(section2)), Ok(()));
        assert_matches!(w.finish(&mut buf), Ok(()));
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
                     0x10, // second field type = Bool
                     0x01, // name length
                     b'b', // name
                     0x04, // leb128 data size
                     0x20, // third field type = String
                     0x01, // name length
                     b'c', // name
                     0x04, // leb128 data size


                     // Section 2
                     0x01, // section encoding = encrypted
                     0x00, // leb128 section point count
                     0x38, // leb128 section data size

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
                     0x10, // second field type = Bool
                     0x04, // name length
                     b'w', // name
                     b'i', // name
                     b't', // name
                     b'h', // name
                     0x04, // leb128 data size
                     0x20, // third field type = String
                     0x03, // name length
                     b'G', // name
                     b'P', // name
                     b'S', // name
                     0x04, // leb128 data size

                     0xF4, // crc
                     0x6B]);
    }

    #[test]
    fn test_data_table_with_multibyte_character() {
        let section = standard::Section::new(Schema::with_fields(vec![FieldDefinition::new(
            "I ♥ NY",
            DataType::F64 { scale: 7 },
        )]));

        let mut buf = Vec::new();
        let mut w = DataTableWriter::default();
        assert_matches!(w.write_section(&Section::Standard(section)), Ok(()));
        assert_matches!(w.finish(&mut buf), Ok(()));
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
                     0x07, // scale
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

                     0x29, // crc
                     0x2C]);
    }

    #[test]
    fn test_too_many_sections() {
        let mut w = DataTableWriter::default();
        for _ in 0..255 {
            assert_matches!(
                w.write_section(&Section::Standard(standard::Section::new(Schema::with_fields(vec![])))),
                Ok(())
            );
        }
        assert_matches!(
            w.write_section(&Section::Standard(standard::Section::new(Schema::with_fields(vec![])))),
            Err(TracklibError::TooManyEntriesError)
        );
    }
}
