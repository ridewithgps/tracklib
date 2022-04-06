use super::data_table::DataTableEntry;
use super::decoders::*;
use super::presence_column::parse_presence_column;
use super::schema::SchemaEntry;
use crate::error::{Result, TracklibError};
use crate::schema::*;
use crate::types::{FieldValue, SectionEncoding};

#[cfg_attr(test, derive(Debug))]
pub struct Section<'a> {
    input: &'a [u8],
    data_table_entry: &'a DataTableEntry,
}

impl<'a> Section<'a> {
    pub(crate) fn new(input: &'a [u8], data_table_entry: &'a DataTableEntry) -> Self {
        Self {
            input,
            data_table_entry,
        }
    }

    pub fn encoding(&self) -> SectionEncoding {
        self.data_table_entry.section_encoding()
    }

    pub fn schema(&self) -> Schema {
        Schema::with_fields(
            self.data_table_entry
                .schema_entries()
                .iter()
                .map(|schema_entry| schema_entry.field_definition().clone())
                .collect(),
        )
    }

    pub fn rows(&self) -> usize {
        self.data_table_entry.rows()
    }

    pub fn reader(&self) -> Result<SectionReader> {
        SectionReader::new(
            self.input,
            self.data_table_entry
                .schema_entries()
                .iter()
                .enumerate()
                .collect(),
            self.data_table_entry.schema_entries().len(),
            self.data_table_entry.rows(),
        )
    }

    pub fn reader_for_schema(&self, schema: &Schema) -> Result<SectionReader> {
        let schema_entries = schema
            .fields()
            .into_iter()
            .filter_map(|field| {
                self.data_table_entry
                    .schema_entries()
                    .iter()
                    .enumerate()
                    .find(|(_, schema_entry)| schema_entry.field_definition() == field)
            })
            .collect::<Vec<_>>();

        SectionReader::new(
            self.input,
            schema_entries,
            self.data_table_entry.schema_entries().len(),
            self.data_table_entry.rows(),
        )
    }
}

#[cfg_attr(test, derive(Debug))]
enum ColumnDecoder<'a> {
    I64 {
        field_definition: &'a FieldDefinition,
        decoder: I64Decoder<'a>,
    },
    F64 {
        field_definition: &'a FieldDefinition,
        decoder: F64Decoder<'a>,
    },
    Bool {
        field_definition: &'a FieldDefinition,
        decoder: BoolDecoder<'a>,
    },
    String {
        field_definition: &'a FieldDefinition,
        decoder: StringDecoder<'a>,
    },
}

#[cfg_attr(test, derive(Debug))]
pub struct SectionReader<'a> {
    decoders: Vec<ColumnDecoder<'a>>,
    rows: usize,
}

impl<'a> SectionReader<'a> {
    pub(crate) fn new(
        input: &'a [u8],
        schema_entries: Vec<(usize, &'a SchemaEntry)>,
        columns: usize,
        rows: usize,
    ) -> Result<Self> {
        let (column_data, presence_column) = parse_presence_column(columns, rows)(input)?;

        let decoders = schema_entries
            .into_iter()
            .map(|(presence_column_index, schema_entry)| {
                let column_data = &column_data
                    [schema_entry.offset()..schema_entry.offset() + schema_entry.size()];
                let presence_column_view =
                    presence_column.view(presence_column_index).ok_or_else(|| {
                        TracklibError::ParseIncompleteError {
                            needed: nom::Needed::Unknown,
                        }
                    })?;
                let field_definition = schema_entry.field_definition();
                let decoder = match field_definition.data_type() {
                    DataType::I64 => ColumnDecoder::I64 {
                        field_definition,
                        decoder: I64Decoder::new(column_data, presence_column_view)?,
                    },
                    DataType::F64 => ColumnDecoder::F64 {
                        field_definition,
                        decoder: F64Decoder::new(column_data, presence_column_view)?,
                    },
                    DataType::Bool => ColumnDecoder::Bool {
                        field_definition,
                        decoder: BoolDecoder::new(column_data, presence_column_view)?,
                    },
                    DataType::String => ColumnDecoder::String {
                        field_definition,
                        decoder: StringDecoder::new(column_data, presence_column_view)?,
                    },
                };
                Ok(decoder)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { decoders, rows })
    }

    pub fn rows_remaining(&self) -> usize {
        self.rows
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
    type Item = Result<(&'b FieldDefinition, Option<FieldValue>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(decoder_enum) = self.decoders.get_mut(self.index) {
            self.index += 1;
            match decoder_enum {
                ColumnDecoder::I64 {
                    ref field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(|v| FieldValue::I64(v))))
                        .map_err(|e| e),
                ),
                ColumnDecoder::F64 {
                    ref field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(|v| FieldValue::F64(v))))
                        .map_err(|e| e),
                ),
                ColumnDecoder::Bool {
                    ref field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(|v| FieldValue::Bool(v))))
                        .map_err(|e| e),
                ),
                ColumnDecoder::String {
                    ref field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(|v| FieldValue::String(v))))
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
                               0x00, // section encoding = standard
                               0x03, // leb128 section point count
                               0x26, // leb128 section data size
                               // Schema
                               0x00, // schema version
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

                               0x81, // crc
                               0x7D];

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

            let section = Section::new(buf, &data_table_entries[0]);

            assert_eq!(section.encoding(), SectionEncoding::Standard);
            assert_eq!(section.rows(), 3);
            assert_eq!(section.schema(), Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
                FieldDefinition::new("f", DataType::F64),
            ]));

            assert_matches!(section.reader(), Ok(mut section_reader) => {
                // Row 1
                assert_eq!(section_reader.rows_remaining(), 3);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 4);
                    assert_matches!(&values[0], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[0].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("a", DataType::I64));
                        assert_eq!(field_value, &Some(FieldValue::I64(1)));
                    });
                    assert_matches!(&values[1], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[1].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("b", DataType::Bool));
                        assert_eq!(field_value, &Some(FieldValue::Bool(false)));
                    });
                    assert_matches!(&values[2], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[2].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("c", DataType::String));
                        assert_eq!(field_value, &Some(FieldValue::String("Ride".to_string())));
                    });
                    assert_matches!(&values[3], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[3].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64));
                        assert_eq!(field_value, &None);
                    });
                });

                // Row 2
                assert_eq!(section_reader.rows_remaining(), 2);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 4);
                    assert_matches!(&values[0], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[0].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("a", DataType::I64));
                        assert_eq!(field_value, &Some(FieldValue::I64(2)));
                    });
                    assert_matches!(&values[1], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[1].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("b", DataType::Bool));
                        assert_eq!(field_value, &None);
                    });
                    assert_matches!(&values[2], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[2].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("c", DataType::String));
                        assert_eq!(field_value, &Some(FieldValue::String("with".to_string())));
                    });
                    assert_matches!(&values[3], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[3].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64));
                        assert_eq!(field_value, &Some(FieldValue::F64(1.0)));
                    });
                });

                // Row 3
                assert_eq!(section_reader.rows_remaining(), 1);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 4);
                    assert_matches!(&values[0], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[0].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("a", DataType::I64));
                        assert_eq!(field_value, &Some(FieldValue::I64(4)));
                    });
                    assert_matches!(&values[1], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[1].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("b", DataType::Bool));
                        assert_eq!(field_value, &Some(FieldValue::Bool(true)));
                    });
                    assert_matches!(&values[2], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[2].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("c", DataType::String));
                        assert_eq!(field_value, &Some(FieldValue::String("GPS".to_string())));
                    });
                    assert_matches!(&values[3], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[3].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64));
                        assert_eq!(field_value, &Some(FieldValue::F64(2.5)));
                    });
                });

                // Trying to get another row will return nothing
                assert_eq!(section_reader.rows_remaining(), 0);
                assert_matches!(section_reader.open_column_iter(), None);
            });
        });
    }

    #[test]
    fn test_section_reader_for_schema() {
        #[rustfmt::skip]
        let data_table_buf = &[0x01, // number of sections

                               // Section 1
                               0x00, // section encoding = standard
                               0x03, // leb128 section point count
                               0x26, // leb128 section data size
                               // Schema
                               0x00, // schema version
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

                               0x81, // crc
                               0x7D];

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

            let section = Section::new(buf, &data_table_entries[0]);

            assert_eq!(section.encoding(), SectionEncoding::Standard);
            assert_eq!(section.rows(), 3);
            assert_eq!(section.schema(), Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
                FieldDefinition::new("f", DataType::F64),
            ]));

            // Missing field
            assert_matches!(section.reader_for_schema(&Schema::with_fields(vec![
                FieldDefinition::new("z", DataType::Bool),
            ])), Ok(mut section_reader) => {
                // Row 1
                assert_eq!(section_reader.rows_remaining(), 3);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    assert_eq!(column_iter.count(), 0);
                });

                // Row 2
                assert_eq!(section_reader.rows_remaining(), 2);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    assert_eq!(column_iter.count(), 0);
                });

                // Row 3
                assert_eq!(section_reader.rows_remaining(), 1);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    assert_eq!(column_iter.count(), 0);
                });

                // Trying to get another row will return nothing
                assert_eq!(section_reader.rows_remaining(), 0);
                assert_matches!(section_reader.open_column_iter(), None);
            });

            // Field exists but we're asking for the wrong type
            assert_matches!(section.reader_for_schema(&Schema::with_fields(vec![
                FieldDefinition::new("b", DataType::I64),
            ])), Ok(mut section_reader) => {
                // Row 1
                assert_eq!(section_reader.rows_remaining(), 3);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    assert_eq!(column_iter.count(), 0);
                });

                // Row 2
                assert_eq!(section_reader.rows_remaining(), 2);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    assert_eq!(column_iter.count(), 0);
                });

                // Row 3
                assert_eq!(section_reader.rows_remaining(), 1);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    assert_eq!(column_iter.count(), 0);
                });

                // Trying to get another row will return nothing
                assert_eq!(section_reader.rows_remaining(), 0);
                assert_matches!(section_reader.open_column_iter(), None);
            });


            // Only one of these fields exists
            assert_matches!(section.reader_for_schema(&Schema::with_fields(vec![
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("z", DataType::Bool),
            ])), Ok(mut section_reader) => {
                // Row 1
                assert_eq!(section_reader.rows_remaining(), 3);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 1);
                    assert_matches!(&values[0], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[1].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("b", DataType::Bool));
                        assert_eq!(field_value, &Some(FieldValue::Bool(false)));
                    });
                });

                // Row 2
                assert_eq!(section_reader.rows_remaining(), 2);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 1);
                    assert_matches!(&values[0], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[1].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("b", DataType::Bool));
                        assert_eq!(field_value, &None);
                    });
                });

                // Row 3
                assert_eq!(section_reader.rows_remaining(), 1);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 1);
                    assert_matches!(&values[0], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[1].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("b", DataType::Bool));
                        assert_eq!(field_value, &Some(FieldValue::Bool(true)));
                    });
                });

                // Trying to get another row will return nothing
                assert_eq!(section_reader.rows_remaining(), 0);
                assert_matches!(section_reader.open_column_iter(), None);
            });

            // Both of these fields exist
            assert_matches!(section.reader_for_schema(&Schema::with_fields(vec![
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("f", DataType::F64),
            ])), Ok(mut section_reader) => {
                // Row 1
                assert_eq!(section_reader.rows_remaining(), 3);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 2);
                    assert_matches!(&values[0], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[1].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("b", DataType::Bool));
                        assert_eq!(field_value, &Some(FieldValue::Bool(false)));
                    });
                    assert_matches!(&values[1], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[3].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64));
                        assert_eq!(field_value, &None);
                    });
                });

                // Row 2
                assert_eq!(section_reader.rows_remaining(), 2);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 2);
                    assert_matches!(&values[0], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[1].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("b", DataType::Bool));
                        assert_eq!(field_value, &None);
                    });
                    assert_matches!(&values[1], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[3].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64));
                        assert_eq!(field_value, &Some(FieldValue::F64(1.0)));
                    });
                });

                // Row 3
                assert_eq!(section_reader.rows_remaining(), 1);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 2);
                    assert_matches!(&values[0], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[1].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("b", DataType::Bool));
                        assert_eq!(field_value, &Some(FieldValue::Bool(true)));
                    });
                    assert_matches!(&values[1], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[3].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64));
                        assert_eq!(field_value, &Some(FieldValue::F64(2.5)));
                    });
                });

                // Trying to get another row will return nothing
                assert_eq!(section_reader.rows_remaining(), 0);
                assert_matches!(section_reader.open_column_iter(), None);
            });
        });
    }
}