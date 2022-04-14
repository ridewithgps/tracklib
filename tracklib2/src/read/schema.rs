use crate::consts::SCHEMA_VERSION;
use crate::error::TracklibError;
use crate::schema::*;
use nom::{bytes::complete::tag, multi::length_data, number::complete::le_u8, IResult};
use nom_leb128::leb128_u64;

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct SchemaEntry {
    field_definition: FieldDefinition,
    size: usize,
    offset: usize,
}

#[cfg(test)]
impl SchemaEntry {
    pub(crate) fn new_for_tests(
        name: &str,
        data_type: DataType,
        size: usize,
        offset: usize,
    ) -> Self {
        Self {
            field_definition: FieldDefinition::new(name, data_type),
            size,
            offset,
        }
    }
}

impl SchemaEntry {
    pub(crate) fn field_definition(&self) -> &FieldDefinition {
        &self.field_definition
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset
    }
}

fn parse_schema_entry<'a>(
    offset: usize,
) -> impl Fn(&[u8]) -> IResult<&[u8], SchemaEntry, TracklibError> {
    move |input: &[u8]| {
        let (mut input, type_tag) = le_u8(input)?;

        let data_type = match type_tag {
            0x00 => DataType::I64,
            0x01 => {
                let (rest, scale) = le_u8(input)?;
                input = rest;
                DataType::F64 { scale }
            }
            0x04 => DataType::String,
            0x05 => DataType::Bool,
            0x06 => {
                let (rest, subtype) = le_u8(input)?;
                input = rest;
                match subtype {
                    0x05 => DataType::BoolArray,
                    0x07 => DataType::U64Array,
                    _ => {
                        return Err(nom::Err::Error(TracklibError::ParseError {
                            error_kind: nom::error::ErrorKind::Tag,
                        }))
                    }
                }
            }
            0x07 => DataType::U64,
            _ => {
                return Err(nom::Err::Error(TracklibError::ParseError {
                    error_kind: nom::error::ErrorKind::Tag,
                }))
            }
        };

        let (input, field_name) = length_data(le_u8)(input)?;
        let (input, data_size) = leb128_u64(input)?;
        let name = String::from_utf8_lossy(field_name);

        Ok((
            input,
            SchemaEntry {
                field_definition: FieldDefinition::new(name, data_type),
                size: usize::try_from(data_size).expect("usize != u64"),
                offset,
            },
        ))
    }
}

pub(crate) fn parse_schema(input: &[u8]) -> IResult<&[u8], Vec<SchemaEntry>, TracklibError> {
    let (input, _schema_version) = tag(&SCHEMA_VERSION.to_le_bytes())(input)?;
    let (mut input, entry_count) = le_u8(input)?;
    let mut entries = Vec::with_capacity(usize::from(entry_count));
    let mut offset = 0;
    for _ in 0..entry_count {
        let (rest, entry) = parse_schema_entry(offset)(input)?;
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
    fn test_test_parse_schema() {
        #[rustfmt::skip]
        let buf = &[0x00, // schema version
                    0x07, // entry count
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
                    0x01, // fourth entry type: f64 = 1
                    0x07, // scale
                    0x01, // name len = 1
                    b'i', // name = "i"
                    0x02, // data size = 2
                    0x06, // fifth entry type: array = 6
                    0x05, // array subtype = bool
                    0x02, // name len = 2
                    b'a', // name
                    b'b',
                    0x11, // data size
                    0x07, // sixth field type = U64
                    0x01, // name length
                    b'u', // name
                    0x10, // data size
                    0x06, // seventh field type = Array
                    0x07, // subtype U64
                    0x02, // name length
                    b'u', // name
                    b'b',
                    0x00, // data size
        ];
        assert_matches!(parse_schema(buf), Ok((&[], entries)) => {
            assert_eq!(entries, vec![SchemaEntry::new_for_tests("m", DataType::I64, 2, 0),
                                     SchemaEntry::new_for_tests("k", DataType::Bool, 1, 2),
                                     SchemaEntry::new_for_tests("long name!", DataType::String, 7, 3),
                                     SchemaEntry::new_for_tests("i", DataType::F64{scale: 7}, 2, 10),
                                     SchemaEntry::new_for_tests("ab", DataType::BoolArray, 17, 12),
                                     SchemaEntry::new_for_tests("u", DataType::U64, 16, 29),
                                     SchemaEntry::new_for_tests("ub", DataType::U64Array, 0, 45),
            ]);
        });
    }

    #[test]
    fn test_schema_invalid_field_tag() {
        #[rustfmt::skip]
        let buf = &[0x00, // schema version
                    0x01, // entry count
                    0xEF, // first entry type: invalid
                    0x01, // name len = 1
                    b'm', // name = "m"
                    0x02]; // data size = 2
        assert_matches!(parse_schema(buf), Err(nom::Err::Error(TracklibError::ParseError{error_kind})) => {
            assert_eq!(error_kind, nom::error::ErrorKind::Tag);
        });
    }

    #[test]
    fn test_schema_invalid_utf8() {
        #[rustfmt::skip]
        let buf = &[0x00, // schema version
                    0x01, // entry count
                    0x00, // first entry type: I64
                    0x01, // name len = 1
                    0xC0, // name: invalid utf-8
                    0x02]; // data size = 2
        assert_matches!(parse_schema(buf), Ok((&[], entries)) => {
            assert_eq!(entries, vec![SchemaEntry::new_for_tests("�", DataType::I64, 2, 0)])
        });
    }

    #[test]
    fn test_schema_invalid_array_subtype() {
        #[rustfmt::skip]
        let buf = &[0x00, // schema version
                    0x01, // entry count
                    0x06, // first entry type: Array
                    0xFF, // invalid array subtype!
                    0x01, // name len = 1
                    b'a', // name
                    0x00, // data size
        ];
        assert_matches!(parse_schema(buf), Err(nom::Err::Error(TracklibError::ParseError{error_kind})) => {
            assert_eq!(error_kind, nom::error::ErrorKind::Tag);
        });
    }
}
