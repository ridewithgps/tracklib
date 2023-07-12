use super::reader::SectionReader;
use crate::error::Result;
use crate::read::data_table::DataTableEntry;
use crate::schema::*;
use crate::types::SectionEncoding;

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

    pub fn reader(&self) -> Result<SectionReader> {
        SectionReader::new(
            self.input,
            self.data_table_entry.schema_entries().iter().enumerate().collect(),
            self.data_table_entry.schema_entries().len(),
            self.data_table_entry.rows(),
        )
    }

    pub fn reader_for_schema(&self, schema: &Schema) -> Result<SectionReader> {
        let schema_entries = schema
            .fields()
            .iter()
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

impl<'a> super::SectionRead for Section<'a> {
    fn encoding(&self) -> SectionEncoding {
        SectionEncoding::Standard
    }

    fn schema(&self) -> Schema {
        Schema::with_fields(
            self.data_table_entry
                .schema_entries()
                .iter()
                .map(|schema_entry| schema_entry.field_definition().clone())
                .collect(),
        )
    }

    fn rows(&self) -> usize {
        self.data_table_entry.rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::data_table::parse_data_table;
    use crate::read::section::SectionRead;
    use crate::types::{FieldValue, SectionEncoding};
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
                               0x08, // field count
                               0x00, // first field type = I64
                               0x01, // name length
                               b'a', // name
                               0x07, // leb128 data size
                               0x10, // second field type = Bool
                               0x01, // name length
                               b'b', // name
                               0x06, // leb128 data size
                               0x20, // third field type = String
                               0x01, // name length
                               b'c', // name
                               0x12, // leb128 data size
                               0x01, // fourth field type = F64
                               0x07, // scale
                               0x01, // name length
                               b'f', // name
                               0x0C, // leb128 data size
                               0x21, // fifth field type = BoolArray
                               0x02, // name length 2
                               b'b', // name
                               b'a',
                               0x0C, // data size
                               0x02, // sixth field type = U64
                               0x01, // name length
                               b'u', // name
                               0x06, // data size
                               0x22, // seventh field type = U64Array
                               0x02, // name length
                               b'b', // name
                               b'u',
                               0x08, // data size
                               0x23, // eight field type = ByteArray
                               0x02, // name length
                               b'b', // name
                               b'B',
                               0x08, // data size

                               0x04, // crc
                               0x13,
        ];

        assert_matches!(parse_data_table(data_table_buf), Ok((&[], data_table_entries)) => {
            assert_eq!(data_table_entries.len(), 1);

            #[rustfmt::skip]
            let buf = &[
                // Presence Column
                0b01010111,
                0b01111101,
                0b10111111,
                0x47, // crc
                0x7E,
                0x81,
                0xCF,

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
                0x15,

                // Data Column 5 = Bool Array
                0x01, // array length
                0x01, // true
                0x04, // array length
                0x00, // false
                0x00, // false
                0x00, // false
                0x01, // true
                0x00, // array length
                0x41, // crc
                0x04,
                0xB4,
                0x3C,

                // Data Column 6 = U64
                0x16, // 22
                0x7E, // -2
                0xDA, // crc
                0x02,
                0xF5,
                0xD6,

                // Data Column 7 = U64Array
                0x00, // array len
                0x02, // array len
                0x04,
                0x01,
                0x94, //crc
                0x47,
                0xAF,
                0x7A,

                // Data Column 8 = ByteArray
                0x03,
                0xDE,
                0xAD,
                0xFF,
                0xDD, // crc
                0xB8,
                0xC0,
                0xDB,
            ];

            let section = Section::new(buf, &data_table_entries[0]);

            assert_eq!(section.encoding(), SectionEncoding::Standard);
            assert_eq!(section.rows(), 3);
            assert_eq!(section.schema(), Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
                FieldDefinition::new("f", DataType::F64{scale: 7}),
                FieldDefinition::new("ba", DataType::BoolArray),
                FieldDefinition::new("u", DataType::U64),
                FieldDefinition::new("bu", DataType::U64Array),
                FieldDefinition::new("bB", DataType::ByteArray),
            ]));

            assert_matches!(section.reader(), Ok(mut section_reader) => {
                // Reader has the full schema
                assert_eq!(section_reader.schema(), Schema::with_fields(vec![
                    FieldDefinition::new("a", DataType::I64),
                    FieldDefinition::new("b", DataType::Bool),
                    FieldDefinition::new("c", DataType::String),
                    FieldDefinition::new("f", DataType::F64{scale: 7}),
                    FieldDefinition::new("ba", DataType::BoolArray),
                    FieldDefinition::new("u", DataType::U64),
                    FieldDefinition::new("bu", DataType::U64Array),
                    FieldDefinition::new("bB", DataType::ByteArray),
                ]));

                // Row 1
                assert_eq!(section_reader.rows_remaining(), 3);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 8);
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
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64{scale: 7}));
                        assert_eq!(field_value, &None);
                    });
                    assert_matches!(&values[4], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[4].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("ba", DataType::BoolArray));
                        assert_eq!(field_value, &Some(FieldValue::BoolArray(vec![true])));
                    });
                    assert_matches!(&values[5], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[5].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("u", DataType::U64));
                        assert_eq!(field_value, &None);
                    });
                    assert_matches!(&values[6], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[6].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("bu", DataType::U64Array));
                        assert_eq!(field_value, &Some(FieldValue::U64Array(vec![])));
                    });
                    assert_matches!(&values[7], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[7].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("bB", DataType::ByteArray));
                        assert_eq!(field_value, &None);
                    });
                });

                // Row 2
                assert_eq!(section_reader.rows_remaining(), 2);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 8);
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
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64{scale: 7}));
                        assert_eq!(field_value, &Some(FieldValue::F64(1.0)));
                    });
                    assert_matches!(&values[4], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[4].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("ba", DataType::BoolArray));
                        assert_eq!(field_value, &Some(FieldValue::BoolArray(vec![false, false, false, true])));
                    });
                    assert_matches!(&values[5], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[5].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("u", DataType::U64));
                        assert_eq!(field_value, &Some(FieldValue::U64(22)));
                    });
                    assert_matches!(&values[6], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[6].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("bu", DataType::U64Array));
                        assert_eq!(field_value, &Some(FieldValue::U64Array(vec![4, 5])));
                    });
                    assert_matches!(&values[7], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[7].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("bB", DataType::ByteArray));
                        assert_eq!(field_value, &None);
                    });
                });

                // Row 3
                assert_eq!(section_reader.rows_remaining(), 1);
                assert_matches!(section_reader.open_column_iter(), Some(column_iter) => {
                    let values = column_iter.collect::<Vec<_>>();
                    assert_eq!(values.len(), 8);
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
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64{scale: 7}));
                        assert_eq!(field_value, &Some(FieldValue::F64(2.5)));
                    });
                    assert_matches!(&values[4], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[4].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("ba", DataType::BoolArray));
                        assert_eq!(field_value, &Some(FieldValue::BoolArray(vec![])));
                    });
                    assert_matches!(&values[5], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[5].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("u", DataType::U64));
                        assert_eq!(field_value, &Some(FieldValue::U64(20)));
                    });
                    assert_matches!(&values[6], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[6].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("bu", DataType::U64Array));
                        assert_eq!(field_value, &None);
                    });
                    assert_matches!(&values[7], Ok((field_definition, field_value)) => {
                        assert_eq!(*field_definition, data_table_entries[0].schema_entries()[7].field_definition());
                        assert_eq!(*field_definition, &FieldDefinition::new("bB", DataType::ByteArray));
                        assert_eq!(field_value, &Some(FieldValue::ByteArray(vec![0xDE, 0xAD, 0xFF])));
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
                               0x10, // second field type = Bool
                               0x01, // name length
                               b'b', // name
                               0x06, // leb128 data size
                               0x20, // third field type = String
                               0x01, // name length
                               b'c', // name
                               0x12, // leb128 data size
                               0x01, // fourth field type = F64
                               0x07, // scale
                               0x01, // name length
                               b'f', // name
                               0x0C, // leb128 data size

                               0xFB, // crc
                               0xB9];

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
                FieldDefinition::new("f", DataType::F64{scale: 7}),
            ]));

            // Missing field
            assert_matches!(section.reader_for_schema(&Schema::with_fields(vec![
                FieldDefinition::new("z", DataType::Bool),
            ])), Ok(mut section_reader) => {
                // Reader has an empty schema
                assert_eq!(section_reader.schema(), Schema::with_fields(vec![]));

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
                // Reader has an empty schema
                assert_eq!(section_reader.schema(), Schema::with_fields(vec![]));

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
                // Reader has only the one field in its schema
                assert_eq!(section_reader.schema(), Schema::with_fields(vec![
                    FieldDefinition::new("b", DataType::Bool),
                ]));

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
                FieldDefinition::new("f", DataType::F64{scale: 7}),
            ])), Ok(mut section_reader) => {
                // Reader has both fields
                assert_eq!(section_reader.schema(), Schema::with_fields(vec![
                    FieldDefinition::new("b", DataType::Bool),
                    FieldDefinition::new("f", DataType::F64{scale: 7}),
                ]));

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
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64{scale: 7}));
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
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64{scale: 7}));
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
                        assert_eq!(*field_definition, &FieldDefinition::new("f", DataType::F64{scale: 7}));
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
