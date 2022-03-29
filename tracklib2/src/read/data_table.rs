use super::crc::CRC;
use super::schema::{parse_schema, SchemaEntry};
use crate::error::TracklibError;
use crate::types::SectionEncoding;
use nom::{number::complete::le_u8, IResult};
use nom_leb128::leb128_u64;

#[cfg_attr(test, derive(Debug, PartialEq))]
pub(crate) struct DataTableEntry {
    section_encoding: SectionEncoding,
    offset: usize,
    size: usize,
    rows: usize,
    schema_entries: Vec<SchemaEntry>,
}

impl DataTableEntry {
    pub(crate) fn section_encoding(&self) -> SectionEncoding {
        self.section_encoding
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset
    }

    #[cfg(feature = "inspect")]
    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    pub(crate) fn schema_entries(&self) -> &[SchemaEntry] {
        self.schema_entries.as_slice()
    }
}

fn parse_data_table_entry(
    offset: usize,
) -> impl Fn(&[u8]) -> IResult<&[u8], DataTableEntry, TracklibError> {
    move |input: &[u8]| {
        let (input, type_tag) = le_u8(input)?;
        let (input, rows) = leb128_u64(input)?;
        let (input, size) = leb128_u64(input)?;
        let (input, schema_entries) = parse_schema(input)?;

        let section_encoding = match type_tag {
            0x00 => SectionEncoding::Standard,
            _ => {
                return Err(nom::Err::Error(TracklibError::ParseError {
                    error_kind: nom::error::ErrorKind::Tag,
                }))
            }
        };

        Ok((
            input,
            DataTableEntry {
                section_encoding,
                offset,
                size: usize::try_from(size).expect("usize != u64"),
                rows: usize::try_from(rows).expect("usize != u64"),
                schema_entries,
            },
        ))
    }
}

pub(crate) fn parse_data_table(input: &[u8]) -> IResult<&[u8], Vec<DataTableEntry>, TracklibError> {
    let input_start = input;
    let (mut input, entry_count) = le_u8(input)?;
    let mut entries = Vec::with_capacity(usize::from(entry_count));
    let mut offset = 0;
    for _ in 0..entry_count {
        let (rest, entry) = parse_data_table_entry(offset)(input)?;
        input = rest;
        offset += entry.size;
        entries.push(entry);
    }
    let (input, checksum) = CRC::<u16>::parser(input_start)(input)?;

    match checksum {
        CRC::Valid(_) => Ok((input, entries)),
        CRC::Invalid { expected, computed } => Err(nom::Err::Error(TracklibError::CRC16Error {
            expected,
            computed,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DataType;
    use assert_matches::assert_matches;

    #[test]
    fn test_parse_data_table() {
        #[rustfmt::skip]
        let buf = &[0x02, // number of sections

                    // Section 1
                    0x00, // section encoding = standard
                    0x00, // leb128 section point count
                    0x00, // leb128 section data size

                    // Schema
                    0x00, // schema version
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
                    0x00, // section encoding = standard
                    0x00, // leb128 section point count
                    0x00, // leb128 section data size

                    // Schema
                    0x00, // schema version
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


                    0xDA, // crc
                    0x8E];

        assert_matches!(parse_data_table(buf), Ok((&[], entries)) => {
            assert_eq!(
                entries,
                vec![
                    DataTableEntry {
                        section_encoding: SectionEncoding::Standard,
                        offset: 0,
                        size: 0,
                        rows: 0,
                        schema_entries: vec![
                            SchemaEntry::new_for_tests("a", DataType::I64, 0, 0),
                            SchemaEntry::new_for_tests("b", DataType::Bool, 0, 0),
                            SchemaEntry::new_for_tests("c", DataType::String, 0, 0)
                        ]
                    },
                    DataTableEntry {
                        section_encoding: SectionEncoding::Standard,
                        offset: 0,
                        size: 0,
                        rows: 0,
                        schema_entries: vec![
                            SchemaEntry::new_for_tests("Ride", DataType::I64, 0, 0),
                            SchemaEntry::new_for_tests("with", DataType::Bool, 0, 0),
                            SchemaEntry::new_for_tests("GPS", DataType::String, 0, 0)
                        ]
                    }
                ]
            );
        });
    }

    #[test]
    fn test_invalid_utf8_fieldname() {
        #[rustfmt::skip]
        let buf = &[0x01, // number of sections

                    // Section 1
                    0x00, // section encoding = standard
                    0x00, // leb128 section point count
                    0x00, // leb128 section data size

                    // Schema
                    0x00, // schema version
                    0x01, // field count
                    0x00, // first field type = I64
                    0x05, // name length
                    b'a', // name with invalid utf8
                    0xF0,
                    0x90,
                    0x80,
                    b'b',
                    0x00, // leb128 data size

                    0x41, // crc
                    0x43];

        assert_matches!(parse_data_table(buf), Ok((&[], entries)) => {
            assert_eq!(
                entries,
                vec![
                    DataTableEntry {
                        section_encoding: SectionEncoding::Standard,
                        offset: 0,
                        size: 0,
                        rows: 0,
                        schema_entries: vec![
                            SchemaEntry::new_for_tests("a�b", DataType::I64, 0, 0),
                        ]
                    }
                ]
            );
        });
    }
}
