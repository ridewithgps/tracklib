use crate::error::TracklibError;
use crate::types::FieldType;
use nom::{
    multi::{length_count, length_data},
    number::complete::le_u8,
    IResult,
};
use nom_leb128::leb128_u64;
use std::str;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
struct TypesTableEntry<'a> {
    fieldtype: FieldType,
    name: &'a str,
    size: u64,
}

fn parse_types_table_entry<'a>(
    input: &'a [u8],
) -> IResult<&'a [u8], TypesTableEntry, TracklibError> {
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

    let name = match str::from_utf8(field_name) {
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
            fieldtype,
            name,
            size: data_size,
        },
    ))
}

fn parse_types_table(input: &[u8]) -> IResult<&[u8], Vec<TypesTableEntry>, TracklibError> {
    length_count(le_u8, parse_types_table_entry)(input)
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
            assert_eq!(entries, vec![TypesTableEntry{fieldtype: FieldType::I64,
                                                     name: "m",
                                                     size: 2},
                                     TypesTableEntry{fieldtype: FieldType::Bool,
                                                     name: "k",
                                                     size: 1},
                                     TypesTableEntry{fieldtype: FieldType::String,
                                                     name: "long name!",
                                                     size: 7},
                                     TypesTableEntry{fieldtype: FieldType::I64,
                                                     name: "i",
                                                     size: 2}]);
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
