use super::data_table::DataTableEntry;
use super::decoders::*;
use super::presence_column::parse_presence_column;
use crate::error::{Result, TracklibError};
use crate::types::{FieldDescription, FieldType, FieldValue};

#[cfg_attr(test, derive(Debug))]
enum ColumnDecoder<'a> {
    I64 {
        field_description: &'a FieldDescription,
        decoder: I64Decoder<'a>,
    },
    F64 {
        field_description: &'a FieldDescription,
        decoder: F64Decoder<'a>,
    },
    Bool {
        field_description: &'a FieldDescription,
        decoder: BoolDecoder<'a>,
    },
    String {
        field_description: &'a FieldDescription,
        decoder: StringDecoder<'a>,
    },
}

#[cfg_attr(test, derive(Debug))]
pub struct SectionReader<'a> {
    data_table_entry: &'a DataTableEntry,
    decoders: Vec<ColumnDecoder<'a>>,
    rows: usize,
}

impl<'a> SectionReader<'a> {
    pub(crate) fn new(input: &'a [u8], data_table_entry: &'a DataTableEntry) -> Result<Self> {
        let (column_data, presence_column) =
            parse_presence_column(data_table_entry.types().len(), data_table_entry.rows())(input)?;

        let decoders = data_table_entry
            .types()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let column_data = &column_data[field.offset()..field.offset() + field.size()];
                let presence_column_view =
                    presence_column
                        .view(i)
                        .ok_or_else(|| TracklibError::ParseIncompleteError {
                            needed: nom::Needed::Unknown,
                        })?;
                let field_description = field.field_description();
                let decoder = match field_description.fieldtype() {
                    FieldType::I64 => ColumnDecoder::I64 {
                        field_description,
                        decoder: I64Decoder::new(column_data, presence_column_view)?,
                    },
                    FieldType::F64 => ColumnDecoder::F64 {
                        field_description,
                        decoder: F64Decoder::new(column_data, presence_column_view)?,
                    },
                    FieldType::Bool => ColumnDecoder::Bool {
                        field_description,
                        decoder: BoolDecoder::new(column_data, presence_column_view)?,
                    },
                    FieldType::String => ColumnDecoder::String {
                        field_description,
                        decoder: StringDecoder::new(column_data, presence_column_view)?,
                    },
                };
                Ok(decoder)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            data_table_entry,
            decoders,
            rows: data_table_entry.rows(),
        })
    }

    pub fn data_table_entry(&self) -> &'a DataTableEntry {
        self.data_table_entry
    }

    pub fn open_column_iter<'r>(&'r mut self) -> Option<ColumnIter<'r, 'a>> {
        if self.rows > 0 {
            self.rows -= 1;
            Some(ColumnIter::new(&mut self.decoders))
        } else {
            None
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub struct ColumnIter<'a, 'b> {
    decoders: &'a mut Vec<ColumnDecoder<'b>>,
    index: usize,
}

impl<'a, 'b> ColumnIter<'a, 'b> {
    fn new(decoders: &'a mut Vec<ColumnDecoder<'b>>) -> Self {
        Self { decoders, index: 0 }
    }
}

impl<'a, 'b> Iterator for ColumnIter<'a, 'b> {
    type Item = Result<(&'a FieldDescription, Option<FieldValue>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(decoder_enum) = self.decoders.get_mut(self.index) {
            self.index += 1;
            match decoder_enum {
                ColumnDecoder::I64 {
                    ref field_description,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_description, maybe_v.map(|v| FieldValue::I64(v))))
                        .map_err(|e| e),
                ),
                ColumnDecoder::F64 {
                    ref field_description,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_description, maybe_v.map(|v| FieldValue::F64(v))))
                        .map_err(|e| e),
                ),
                ColumnDecoder::Bool {
                    ref field_description,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_description, maybe_v.map(|v| FieldValue::Bool(v))))
                        .map_err(|e| e),
                ),
                ColumnDecoder::String {
                    ref field_description,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_description, maybe_v.map(|v| FieldValue::String(v))))
                        .map_err(|e| e),
                ),
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::data_table::parse_data_table;
    use assert_matches::assert_matches;

    #[test]
    fn test_section_reader() {
        #[rustfmt::skip]
        let data_table_buf = &[0x01, // number of sections

                               // Section 1
                               0x00, // section type = track points
                               0x03, // leb128 section point count
                               0x26, // leb128 section data size
                               // Types Table
                               0x04, // field count
                               0x00, // first field type = I64
                               0x01, // name length
                               b'a', // name
                               0x07, // leb128 data size
                               0x05, // second field type = Bool
                               0x01, // name length
                               b'b', // name
                               0x06, // leb128 data size
                               0x04, // third field type = String
                               0x01, // name length
                               b'c', // name
                               0x12, // leb128 data size
                               0x01, // fourth field type = F64
                               0x01, // name length
                               b'f', // name
                               0x0C, // leb128 data size

                               0xE9, // crc
                               0x0B];

        assert_matches!(parse_data_table(data_table_buf), Ok((&[], data_table_entries)) => {
            assert_eq!(data_table_entries.len(), 1);

            #[rustfmt::skip]
            let buf = &[
                // Presence Column
                0b00000111,
                0b00001101,
                0b00001111,
                0xF0, // crc
                0xDB,
                0xAA,
                0x68,

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

                // Data Column 4 = F64
                // None
                0x80, // 1.0
                0xAD,
                0xE2,
                0x04,
                0xC0, // 2.5
                0xC3,
                0x93,
                0x07,
                0xCC, // crc
                0xEC,
                0xC5,
                0x15
            ];

            assert_matches!(SectionReader::new(buf, &data_table_entries[0]), Ok(mut section_reader) => {
                // Row 1
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 4);
                    assert_matches!(&values[0], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[0].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("a".to_string(), FieldType::I64));
                        assert_eq!(field_value, &Some(FieldValue::I64(1)));
                    });
                    assert_matches!(&values[1], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[1].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("b".to_string(), FieldType::Bool));
                        assert_eq!(field_value, &Some(FieldValue::Bool(false)));
                    });
                    assert_matches!(&values[2], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[2].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("c".to_string(), FieldType::String));
                        assert_eq!(field_value, &Some(FieldValue::String("Ride".to_string())));
                    });
                    assert_matches!(&values[3], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[3].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("f".to_string(), FieldType::F64));
                        assert_eq!(field_value, &None);
                    });
                });

                // Row 2
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 4);
                    assert_matches!(&values[0], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[0].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("a".to_string(), FieldType::I64));
                        assert_eq!(field_value, &Some(FieldValue::I64(2)));
                    });
                    assert_matches!(&values[1], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[1].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("b".to_string(), FieldType::Bool));
                        assert_eq!(field_value, &None);
                    });
                    assert_matches!(&values[2], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[2].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("c".to_string(), FieldType::String));
                        assert_eq!(field_value, &Some(FieldValue::String("with".to_string())));
                    });
                    assert_matches!(&values[3], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[3].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("f".to_string(), FieldType::F64));
                        assert_eq!(field_value, &Some(FieldValue::F64(1.0)));
                    });
                });

                // Row 3
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 4);
                    assert_matches!(&values[0], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[0].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("a".to_string(), FieldType::I64));
                        assert_eq!(field_value, &Some(FieldValue::I64(4)));
                    });
                    assert_matches!(&values[1], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[1].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("b".to_string(), FieldType::Bool));
                        assert_eq!(field_value, &Some(FieldValue::Bool(true)));
                    });
                    assert_matches!(&values[2], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[2].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("c".to_string(), FieldType::String));
                        assert_eq!(field_value, &Some(FieldValue::String("GPS".to_string())));
                    });
                    assert_matches!(&values[3], Ok((field_description, field_value)) => {
                        assert_eq!(*field_description, data_table_entries[0].types()[3].field_description());
                        assert_eq!(*field_description, &FieldDescription::new("f".to_string(), FieldType::F64));
                        assert_eq!(field_value, &Some(FieldValue::F64(2.5)));
                    });
                });

                // Trying to get another row will return nothing
                assert_matches!(section_reader.open_column_iter(), None);
            });
        });
    }
}
