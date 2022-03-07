use crate::error::TracklibError;
use crate::types::{FieldDescription, FieldType};
use nom::{multi::length_data, number::complete::le_u8, IResult};
use nom_leb128::leb128_u64;
use std::convert::TryFrom;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct TypesTableEntry {
    field_description: FieldDescription,
    size: usize,
    offset: usize,
}

#[cfg(test)]
impl TypesTableEntry {
    pub(crate) fn new_for_tests(
        fieldtype: FieldType,
        name: &str,
        size: usize,
        offset: usize,
    ) -> Self {
        Self {
            field_description: FieldDescription::new(name.to_string(), fieldtype),
            size,
            offset,
        }
    }
}

impl TypesTableEntry {
    pub(crate) fn field_description(&self) -> &FieldDescription {
        &self.field_description
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset
    }
}

fn parse_types_table_entry<'a>(
    offset: usize,
) -> impl Fn(&[u8]) -> IResult<&[u8], TypesTableEntry, TracklibError> {
    move |input: &[u8]| {
        let (input, type_tag) = le_u8(input)?;
        let (input, field_name) = length_data(le_u8)(input)?;
        let (input, data_size) = leb128_u64(input)?;

        let fieldtype = match type_tag {
            0x00 => FieldType::I64,
            0x04 => FieldType::String,
            0x05 => FieldType::Bool,
            _ => {
                return Err(nom::Err::Error(TracklibError::ParseError {
                    error_kind: nom::error::ErrorKind::Tag,
                }))
            }
        };

        let name = match String::from_utf8(field_name.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return Err(nom::Err::Error(TracklibError::ParseError {
                    error_kind: nom::error::ErrorKind::Tag,
                }))
            }
        };

        Ok((
            input,
            TypesTableEntry {
                field_description: FieldDescription::new(name, fieldtype),
                size: usize::try_from(data_size).expect("usize != u64"),
                offset,
            },
        ))
    }
}

pub(crate) fn parse_types_table(
    input: &[u8],
) -> IResult<&[u8], Vec<TypesTableEntry>, TracklibError> {
    let (mut input, entry_count) = le_u8(input)?;
    let mut entries = Vec::with_capacity(usize::from(entry_count));
    let mut offset = 0;
    for _ in 0..entry_count {
        let (rest, entry) = parse_types_table_entry(offset)(input)?;
        input = rest;
        offset += entry.size;
        entries.push(entry);
    }
    Ok((input, entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_test_parse_types_table() {
        #[rustfmt::skip]
        let buf = &[0x04, // entry count = 4
                    0x00, // first entry type: i64 = 0
                    0x01, // name len = 1
                    b'm', // name = "m"
                    0x02, // data size = 2
                    0x05, // second entry type: bool = 5
                    0x01, // name len = 1
                    b'k', // name = "k"
                    0x01, // data size = 1
                    0x04, // third entry type: string = 4
                    0x0A, // name len = 10
                    b'l', // name = "long name!"
                    b'o',
                    b'n',
                    b'g',
                    b' ',
                    b'n',
                    b'a',
                    b'm',
                    b'e',
                    b'!',
                    0x07, // data size = 7 ("Hello!" + leb128 length prefix)
                    0x00, // fourth entry type: i64 = 0
                    0x01, // name len = 1
                    b'i', // name = "i"
                    0x02]; // data size = 2
        assert_matches!(parse_types_table(buf), Ok((&[], entries)) => {
            assert_eq!(entries, vec![TypesTableEntry{field_description: FieldDescription::new("m".to_string(), FieldType::I64),
                                                     size: 2,
                                                     offset: 0},
                                     TypesTableEntry{field_description: FieldDescription::new("k".to_string(), FieldType::Bool),
                                                     size: 1,
                                                     offset: 2},
                                     TypesTableEntry{field_description: FieldDescription::new("long name!".to_string(), FieldType::String),
                                                     size: 7,
                                                     offset: 3},
                                     TypesTableEntry{field_description: FieldDescription::new("i".to_string(), FieldType::I64),
                                                     size: 2,
                                                     offset: 10}]);
        });
    }

    #[test]
    fn test_types_table_invalid_field_tag() {
        #[rustfmt::skip]
        let buf = &[0x01, // entry count
                    0xEF, // first entry type: invalid
                    0x01, // name len = 1
                    b'm', // name = "m"
                    0x02]; // data size = 2
        assert_matches!(parse_types_table(buf), Err(nom::Err::Error(TracklibError::ParseError{error_kind})) => {
            assert_eq!(error_kind, nom::error::ErrorKind::Tag);
        });
    }

    #[test]
    fn test_types_table_invalid_utf8() {
        #[rustfmt::skip]
        let buf = &[0x01, // entry count
                    0x00, // first entry type: I64
                    0x01, // name len = 1
                    0xC0, // name: invalid utf-8
                    0x02]; // data size = 2
        assert_matches!(parse_types_table(buf), Err(nom::Err::Error(TracklibError::ParseError{error_kind})) => {
            assert_eq!(error_kind, nom::error::ErrorKind::Tag);
        });
    }
}
