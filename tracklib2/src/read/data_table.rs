use super::crc::CRC;
use super::types_table::{parse_types_table, TypesTableEntry};
use crate::error::TracklibError;
use crate::types::SectionType;
use nom::{number::complete::le_u8, IResult};
use nom_leb128::leb128_u64;
use std::convert::TryFrom;

#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct DataTableEntry {
    section_type: SectionType,
    offset: usize,
    size: usize,
    rows: usize,
    types: Vec<TypesTableEntry>,
}

impl DataTableEntry {
    pub fn section_type(&self) -> &SectionType {
        &self.section_type
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn types(&self) -> &[TypesTableEntry] {
        self.types.as_slice()
    }
}

fn parse_data_table_entry(
    offset: usize,
) -> impl Fn(&[u8]) -> IResult<&[u8], DataTableEntry, TracklibError> {
    move |input: &[u8]| {
        let (input, type_tag) = le_u8(input)?;
        let (input, rows) = leb128_u64(input)?;
        let (input, size) = leb128_u64(input)?;
        let (input, types) = parse_types_table(input)?;

        let section_type = match type_tag {
            0x00 => SectionType::TrackPoints,
            0x01 => SectionType::CoursePoints,
            _ => {
                return Err(nom::Err::Error(TracklibError::ParseError {
                    error_kind: nom::error::ErrorKind::Tag,
                }))
            }
        };

        Ok((
            input,
            DataTableEntry {
                section_type,
                offset,
                size: usize::try_from(size).expect("usize != u64"),
                rows: usize::try_from(rows).expect("usize != u64"),
                types,
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
    use crate::types::FieldType;
    use assert_matches::assert_matches;

    #[test]
    fn test_parse_data_table() {
        #[rustfmt::skip]
        let buf = &[0x02, // number of sections

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
                    0x88];

        assert_matches!(parse_data_table(buf), Ok((&[], entries)) => {
            assert_eq!(
                entries,
                vec![
                    DataTableEntry {
                        section_type: SectionType::TrackPoints,
                        offset: 0,
                        size: 0,
                        rows: 0,
                        types: vec![
                            TypesTableEntry::new_for_tests(FieldType::I64, "a", 0, 0),
                            TypesTableEntry::new_for_tests(FieldType::Bool, "b", 0, 0),
                            TypesTableEntry::new_for_tests(FieldType::String, "c", 0, 0)
                        ]
                    },
                    DataTableEntry {
                        section_type: SectionType::CoursePoints,
                        offset: 0,
                        size: 0,
                        rows: 0,
                        types: vec![
                            TypesTableEntry::new_for_tests(FieldType::I64, "Ride", 0, 0),
                            TypesTableEntry::new_for_tests(FieldType::Bool, "with", 0, 0),
                            TypesTableEntry::new_for_tests(FieldType::String, "GPS", 0, 0)
                        ]
                    }
                ]
            );
        });
    }
}
