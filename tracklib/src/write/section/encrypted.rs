use super::writer::{Buffer, RowBuilder};
use crate::error::Result;
use crate::schema::*;
use crate::types::SectionEncoding;
use std::io::{self, Write};

pub struct Section {
    key: orion::aead::SecretKey,
    rows_written: usize,
    schema: Schema,
    column_data: Vec<Buffer>,
}

impl Section {
    pub fn new(key_material: &[u8], schema: Schema) -> Result<Self> {
        let column_data = schema
            .fields()
            .iter()
            .map(|field_def| Buffer::new(field_def.data_type()))
            .collect();
        let key = orion::aead::SecretKey::from_slice(key_material)?;

        Ok(Self {
            key,
            rows_written: 0,
            schema,
            column_data,
        })
    }
}

impl super::SectionWrite for Section {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn encoding(&self) -> SectionEncoding {
        SectionEncoding::Encrypted
    }

    fn rows_written(&self) -> usize {
        self.rows_written
    }

    fn open_row_builder(&mut self) -> RowBuilder {
        self.rows_written += 1;
        RowBuilder::new(&self.schema, &mut self.column_data)
    }
}

impl super::SectionInternal for Section {
    fn section_encoding_tag(&self) -> u8 {
        0x01
    }

    fn data_size_overhead(&self) -> usize {
        orion::hazardous::stream::xchacha20::XCHACHA_NONCESIZE + orion::hazardous::mac::poly1305::POLY1305_OUTSIZE
    }

    fn buffers(&self) -> &[Buffer] {
        &self.column_data
    }

    fn write<W: Write>(&self, out: &mut W) -> Result<()> {
        // write to a local buffer, encrypt, then copy the buffer to out
        let mut buf = Vec::new();
        self.write_data(&mut buf)?;
        let ciphertext = orion::aead::seal(&self.key, &buf)?;
        io::copy(&mut io::Cursor::new(&ciphertext), out)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FieldValue;
    use crate::write::section::writer::ColumnWriter;
    use crate::write::section::{SectionInternal, SectionWrite};
    use assert_matches::assert_matches;
    use std::collections::HashMap;

    #[test]
    fn test_write_presence_column() {
        let mut section = Section::new(
            &crate::util::random_key_material(),
            Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
            ]),
        )
        .unwrap();
        let mut buf = Vec::new();
        assert_matches!(section.write_presence_column(&mut buf), Ok(()) => {
            assert_eq!(buf,
                       &[0x00, // crc
                         0x00,
                         0x00,
                         0x00]);
        });

        let a_vals = vec![Some(&42), Some(&0), None, Some(&-20)];
        let b_vals = vec![Some(&true), None, Some(&false), Some(&false)];
        let c_vals = vec![None, Some("hi"), Some("tracklib"), Some("!")];

        for i in 0..4 {
            let mut rowbuilder = section.open_row_builder();
            while let Some(cw) = rowbuilder.next_column_writer() {
                match cw {
                    ColumnWriter::I64ColumnWriter(cwi) => {
                        assert!(cwi.write(a_vals[i]).is_ok());
                    }
                    ColumnWriter::BoolColumnWriter(cwi) => {
                        assert!(cwi.write(b_vals[i]).is_ok());
                    }
                    ColumnWriter::StringColumnWriter(cwi) => {
                        assert!(cwi.write(c_vals[i]).is_ok());
                    }
                    ColumnWriter::U64ColumnWriter(_) => {}
                    ColumnWriter::F64ColumnWriter(_) => {}
                    ColumnWriter::BoolArrayColumnWriter(_) => {}
                    ColumnWriter::U64ArrayColumnWriter(_) => {}
                    ColumnWriter::ByteArrayColumnWriter(_) => {}
                }
            }
        }

        let mut buf = Vec::new();
        assert_matches!(section.write_presence_column(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0b00000011,
                         0b00000101,
                         0b00000110,
                         0b00000111,
                         0xD2, // crc
                         0x61,
                         0xA7,
                         0xA5]);
        });
    }

    #[test]
    fn test_multibyte_presence_column() {
        let mut section = Section::new(
            &crate::util::random_key_material(),
            Schema::with_fields(
                (0..20)
                    .map(|i| FieldDefinition::new(i.to_string(), DataType::Bool))
                    .collect(),
            ),
        )
        .unwrap();

        for _ in 0..2 {
            let mut rowbuilder = section.open_row_builder();
            while let Some(cw) = rowbuilder.next_column_writer() {
                match cw {
                    ColumnWriter::BoolColumnWriter(cwi) => assert!(cwi.write(Some(&true)).is_ok()),
                    _ => assert!(false, "unexpected column writer type here"),
                }
            }
        }

        let mut buf = Vec::new();
        assert_matches!(section.write_presence_column(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0b00001111, 0b11111111, 0b11111111,
                         0b00001111, 0b11111111, 0b11111111,
                         0xDD, // crc
                         0xCB,
                         0x18,
                         0x17]);
        });
    }

    #[test]
    fn test_write_huge_presence_column() {
        let mut section = Section::new(
            &crate::util::random_key_material(),
            Schema::with_fields(
                (0..80)
                    .map(|i| FieldDefinition::new(i.to_string(), DataType::Bool))
                    .collect(),
            ),
        )
        .unwrap();

        #[rustfmt::skip]
        let vals = &[
            Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 1
            Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 2
            None,        None,        Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 3
            Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), None,        None,        None,        // 4
            Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 5
            None,        Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 6
            Some(&true), Some(&true), Some(&true), Some(&true), None,        None,        Some(&true), Some(&true), // 7
            None,        None,        None,        None,        None,        None,        None,        None,        // 8
            Some(&true), Some(&true), Some(&true), Some(&true), None,        Some(&true), None,        None,        // 9
            None,        None,        None,        Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 10
        ];

        let mut rowbuilder = section.open_row_builder();
        let mut i = 0;
        while let Some(cw) = rowbuilder.next_column_writer() {
            match cw {
                ColumnWriter::BoolColumnWriter(cwi) => assert!(cwi.write(vals[i]).is_ok()),
                _ => assert!(false, "unexpected column writer type here"),
            }
            i += 1;
        }

        let mut buf = Vec::new();
        assert_matches!(section.write_presence_column(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0b11111000, // 10
                         0b00101111, // 9
                         0b00000000, // 8
                         0b11001111, // 7
                         0b11111110, // 6
                         0b11111111, // 5
                         0b00011111, // 4
                         0b11111100, // 3
                         0b11111111, // 2
                         0b11111111, // 1
                         0x92, // crc
                         0x0E,
                         0x6F,
                         0xC2]);
        });
    }

    #[test]
    fn test_schema() {
        let mut section = Section::new(
            &crate::util::random_key_material(),
            Schema::with_fields(vec![
                FieldDefinition::new("m", DataType::I64),
                FieldDefinition::new("k", DataType::Bool),
                FieldDefinition::new("long name!", DataType::String),
                FieldDefinition::new("f", DataType::F64 { scale: 7 }),
                FieldDefinition::new("ab", DataType::BoolArray),
                FieldDefinition::new("u", DataType::U64),
                FieldDefinition::new("au", DataType::U64Array),
                FieldDefinition::new("abyte", DataType::ByteArray),
            ]),
        )
        .unwrap();

        let mut rowbuilder = section.open_row_builder();
        while let Some(cw) = rowbuilder.next_column_writer() {
            match cw {
                ColumnWriter::I64ColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&500)).is_ok());
                }
                ColumnWriter::U64ColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&2112)).is_ok());
                }
                ColumnWriter::BoolColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&false)).is_ok());
                }
                ColumnWriter::StringColumnWriter(cwi) => {
                    assert!(cwi.write(Some("Hello!")).is_ok());
                }
                ColumnWriter::F64ColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&0.0042)).is_ok());
                }
                ColumnWriter::BoolArrayColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&[true, false, true])).is_ok());
                }
                ColumnWriter::U64ArrayColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&[1, 30, 12])).is_ok());
                }
                ColumnWriter::ByteArrayColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&[0xDE, 0xAD, 0xBE, 0xEF])).is_ok());
                }
            }
        }

        let mut buf = Vec::new();
        assert_matches!(section.write_schema(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x00, // schema version
                         0x08, // entry count
                         0x00, // first entry type: I64
                         0x01, // name len = 1
                         b'm', // name = "m"
                         0x06, // data size = 6
                         0x10, // second entry type: Bool
                         0x01, // name len = 1
                         b'k', // name = "k"
                         0x05, // data size = 5
                         0x20, // third entry type: String
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
                         0x0B, // data size = 11
                         0x01, // fourth entry type: F64
                         0x07, // scale
                         0x01, // name len = 1
                         b'f', // name = "f"
                         0x07, // data size = 7
                         0x21, // fifth entry type: BoolArray
                         0x02, // name len = 2
                         b'a', // name = "ids"
                         b'b',
                         0x08, // data size = 8
                         0x02, // sixth entry type: U64
                         0x01, // name len
                         b'u', // name
                         0x06, // data size
                         0x22, // seventh entry type: U64Array
                         0x02, // name len
                         b'a', // name
                         b'u',
                         0x08, // data size
                         0x23, // eigth entry type: ByteArray
                         0x05, // name len
                         b'a', // name
                         b'b',
                         b'y',
                         b't',
                         b'e',
                         0x09, // data size
                       ]);
        });
    }

    #[test]
    fn test_writing_a_section() {
        let key_material = crate::util::random_key_material();

        let mut v = Vec::new();
        let mut h = HashMap::new();
        h.insert("a", FieldValue::I64(1));
        h.insert("b", FieldValue::Bool(false));
        h.insert("c", FieldValue::String("Ride".to_string()));
        h.insert("d", FieldValue::F64(0.0));
        h.insert("g", FieldValue::U64Array(vec![50, 49]));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", FieldValue::I64(2));
        h.insert("c", FieldValue::String("with".to_string()));
        h.insert("e", FieldValue::BoolArray(vec![true, false]));
        h.insert("f", FieldValue::U64(20));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", FieldValue::I64(4));
        h.insert("b", FieldValue::Bool(true));
        h.insert("c", FieldValue::String("GPS".to_string()));
        h.insert("d", FieldValue::F64(2112.90125));
        h.insert("f", FieldValue::U64(18));
        h.insert("g", FieldValue::U64Array(vec![1, 2, 3]));
        h.insert("h", FieldValue::ByteArray(vec![0, 1]));
        v.push(h);

        let mut section = Section::new(
            &key_material,
            Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
                FieldDefinition::new("d", DataType::F64 { scale: 7 }),
                FieldDefinition::new("e", DataType::BoolArray),
                FieldDefinition::new("f", DataType::U64),
                FieldDefinition::new("g", DataType::U64Array),
                FieldDefinition::new("h", DataType::ByteArray),
            ]),
        )
        .unwrap();

        let fields = section.schema().fields().to_vec();

        for entry in v {
            let mut rowbuilder = section.open_row_builder();

            for field_def in fields.iter() {
                assert_matches!(rowbuilder.next_column_writer(), Some(cw) => {
                    match cw {
                        ColumnWriter::I64ColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        FieldValue::I64(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::U64ColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        FieldValue::U64(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::BoolColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        FieldValue::Bool(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::StringColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        FieldValue::String(v) => Some(v.as_str()),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::F64ColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        FieldValue::F64(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::BoolArrayColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        FieldValue::BoolArray(v) => Some(v.as_slice()),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::U64ArrayColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        FieldValue::U64Array(v) => Some(v.as_slice()),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::ByteArrayColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        FieldValue::ByteArray(v) => Some(v.as_slice()),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                    }
                });
            }
        }

        let mut buf = Vec::new();
        assert_matches!(section.write(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            let expected_buf = &[
                // Presence Column
                0b01001111,
                0b00110101,
                0b11101111,
                0x16, // crc
                0x56,
                0x57,
                0x6F,

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
                0x0, // 0.0
                // None
                0x94, // 2112.90125
                0xCA,
                0x8C,
                0xDB,
                0xCE,
                0x00,
                0xF0, // crc
                0xA4,
                0x8A,
                0xDD,

                // Data Column 5 = Bool Array
                0x02, // two entries
                0x01, // true
                0x00, // false
                0x2D, // crc
                0x1A,
                0x33,
                0x99,

                // Data Column 6 = U64
                0x14, // 20
                0x7E, // -2
                0xD5, // crc
                0x9C,
                0x07,
                0x76,

                // Data Column 7 = U64 Array
                0x02, // two entries
                0x32, // 50
                0x7F, // -1
                0x03,
                0x01, // 1
                0x01, // +1
                0x01, // +1 again
                0xE8, // crc
                0x5D,
                0x06,
                0x83,

                // Data Column 8 = Byte Array
                0x02, // array len 2
                0x00,
                0x01,
                0x46, // crc
                0xC6,
                0xEB,
                0x4F,
            ];

            let decrypted_buf = crate::util::decrypt(&key_material, &buf).unwrap();
            assert_eq!(decrypted_buf, expected_buf);
        });
    }
}
