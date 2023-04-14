// Only run these tests in a release build because they rely on unchecked math
#[cfg(not(debug_assertions))]
mod tests {
    use std::collections::HashMap;
    use tracklib::read::track::TrackReader;
    use tracklib::schema::*;
    use tracklib::types::{FieldValue, MetadataEntry, TrackType};
    use tracklib::write::section::writer::ColumnWriter;
    use tracklib::write::section::{encrypted, standard, Section, SectionWrite};
    use tracklib::write::track::write_track;

    #[test]
    fn roundtrip_i64() {
        let mut buf = vec![];
        #[rustfmt::skip]
        let write_values = &[
            0,
            20,
            -20,
            5000,
            -5000,
            i64::MIN,
            -10,
            0,
            i64::MAX,
            i64::MIN,
        ];

        // Write
        let mut section = standard::Section::new(Schema::with_fields(vec![FieldDefinition::new("v", DataType::I64)]));
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::I64ColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<i64> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Standard(section) = section {
                let mut section_reader = section.reader().unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let vals = columniter.collect::<Vec<_>>();
                    assert_eq!(vals.len(), 1);
                    let (_field_desc, field_value) = vals[0].as_ref().unwrap();
                    if let Some(FieldValue::I64(v)) = field_value {
                        read_values.push(*v);
                    }
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_u64() {
        let mut buf = vec![];
        #[rustfmt::skip]
        let write_values = &[
            0,
            20,
            5000,
            10,
            0,
            u64::MAX,
            u64::MIN,
            u64::MAX,
            u64::MAX - 100,
            0,
            i64::MAX as u64 + 1,
        ];

        // Write
        let mut section = standard::Section::new(Schema::with_fields(vec![FieldDefinition::new("v", DataType::U64)]));
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::U64ColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<u64> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Standard(section) = section {
                let mut section_reader = section.reader().unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let vals = columniter.collect::<Vec<_>>();
                    assert_eq!(vals.len(), 1);
                    let (_field_desc, field_value) = vals[0].as_ref().unwrap();
                    if let Some(FieldValue::U64(v)) = field_value {
                        read_values.push(*v);
                    }
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_f64() {
        let mut buf = vec![];
        #[rustfmt::skip]
        let write_values = &[
            -200.101,
            0.0,
            0.1,
        ];

        // Write
        let mut section = standard::Section::new(Schema::with_fields(vec![FieldDefinition::new(
            "v",
            DataType::F64 { scale: 7 },
        )]));
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::F64ColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<f64> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Standard(section) = section {
                let mut section_reader = section.reader().unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let vals = columniter.collect::<Vec<_>>();
                    assert_eq!(vals.len(), 1);
                    let (_field_desc, field_value) = vals[0].as_ref().unwrap();
                    if let Some(FieldValue::F64(v)) = field_value {
                        read_values.push(*v);
                    }
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_bool() {
        let mut buf = vec![];
        #[rustfmt::skip]
        let write_values = &[
            false,
            true,
            true,
            true,
            false,
            false,
            true,
        ];

        // Write
        let mut section = standard::Section::new(Schema::with_fields(vec![FieldDefinition::new("v", DataType::Bool)]));
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::BoolColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<bool> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Standard(section) = section {
                let mut section_reader = section.reader().unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let vals = columniter.collect::<Vec<_>>();
                    assert_eq!(vals.len(), 1);
                    let (_field_desc, field_value) = vals[0].as_ref().unwrap();
                    if let Some(FieldValue::Bool(v)) = field_value {
                        read_values.push(*v);
                    }
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_string() {
        let mut buf = vec![];
        #[rustfmt::skip]
        let write_values = &[
            "",
            "longer string",
            r"reallllllllllllllllllllllllllllllllllllllyyyyyyyyyyyyyyyyyyyyyyy
              longggggggggggggggggggggggggggggggggggggggggggggggggggggggggg
              stringgggggggggggggggggggggggggggggggggg",
        ];

        // Write
        let mut section =
            standard::Section::new(Schema::with_fields(vec![FieldDefinition::new("v", DataType::String)]));
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::StringColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<String> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Standard(section) = section {
                let mut section_reader = section.reader().unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let vals = columniter.collect::<Vec<_>>();
                    assert_eq!(vals.len(), 1);
                    let (_field_desc, field_value) = vals[0].as_ref().unwrap();
                    if let Some(FieldValue::String(v)) = field_value {
                        read_values.push(v.clone());
                    }
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_bool_array() {
        let mut buf = vec![];
        #[rustfmt::skip]
        let write_values = &[
            vec![true, true, true, false],
            vec![],
            vec![false],
            vec![true],
            vec![true; 1_000_000],
            vec![false; 1_000_000],
        ];

        // Write
        let mut section = standard::Section::new(Schema::with_fields(vec![FieldDefinition::new(
            "v",
            DataType::BoolArray,
        )]));
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::BoolArrayColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<Vec<bool>> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Standard(section) = section {
                let mut section_reader = section.reader().unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let vals = columniter.collect::<Vec<_>>();
                    assert_eq!(vals.len(), 1);
                    let (_field_desc, field_value) = vals[0].as_ref().unwrap();
                    if let Some(FieldValue::BoolArray(v)) = field_value {
                        read_values.push(v.clone());
                    }
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_u64_array() {
        let mut buf = vec![];
        #[rustfmt::skip]
        let write_values = &[
            vec![1, 2, 3, 4, 1_000],
            vec![1000, 5, 2000, 0, 9000, 8000, 2],
            vec![128; 1_000_000],
        ];

        // Write
        let mut section =
            standard::Section::new(Schema::with_fields(vec![FieldDefinition::new("v", DataType::U64Array)]));
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::U64ArrayColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<Vec<u64>> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Standard(section) = section {
                let mut section_reader = section.reader().unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let vals = columniter.collect::<Vec<_>>();
                    assert_eq!(vals.len(), 1);
                    let (_field_desc, field_value) = vals[0].as_ref().unwrap();
                    if let Some(FieldValue::U64Array(v)) = field_value {
                        read_values.push(v.clone());
                    }
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_byte_array() {
        let mut buf = vec![];
        #[rustfmt::skip]
        let write_values = &[
            vec![1, 2, 3, 4, 100, 5],
            vec![],
            vec![0, 255, 0, 127],
            vec![128; 1_000_000],
        ];

        // Write
        let mut section = standard::Section::new(Schema::with_fields(vec![FieldDefinition::new(
            "v",
            DataType::ByteArray,
        )]));
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::ByteArrayColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<Vec<u8>> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Standard(section) = section {
                let mut section_reader = section.reader().unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let vals = columniter.collect::<Vec<_>>();
                    assert_eq!(vals.len(), 1);
                    let (_field_desc, field_value) = vals[0].as_ref().unwrap();
                    if let Some(FieldValue::ByteArray(v)) = field_value {
                        read_values.push(v.clone());
                    }
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_metadata() {
        let mut buf = vec![];
        let metadata_entries = vec![
            MetadataEntry::TrackType(TrackType::Trip(u64::MAX)),
            MetadataEntry::CreatedAt(u64::MAX),
        ];

        // Write
        let section = standard::Section::new(Schema::with_fields(vec![]));
        assert!(write_track(&mut buf, &metadata_entries, &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();

        // Compare
        assert_eq!(metadata_entries.as_slice(), track_reader.metadata());
    }

    #[test]
    fn roundtrip_lots_of_columns() {
        let mut buf = vec![];
        let mut write_values: Vec<HashMap<String, FieldValue>> = Vec::new();
        let mut h = HashMap::new();
        h.insert("i64".to_string(), FieldValue::I64(1));
        h.insert("f64:2".to_string(), FieldValue::F64(0.12));
        h.insert("f64:7".to_string(), FieldValue::F64(0.1234567));
        h.insert("u64".to_string(), FieldValue::U64(80_000_000_000_000));
        h.insert("bool".to_string(), FieldValue::Bool(true));
        h.insert("second i64".to_string(), FieldValue::I64(12));
        h.insert("string".to_string(), FieldValue::String("RWGPS".to_string()));
        h.insert(
            "bool array".to_string(),
            FieldValue::BoolArray(vec![true, false, false, false, true, false]),
        );
        h.insert("u64 array".to_string(), FieldValue::U64Array(vec![100, 120, 140, 160]));
        h.insert(
            "second string".to_string(),
            FieldValue::String("This is a string".to_string()),
        );
        h.insert("byte array".to_string(), FieldValue::ByteArray(vec![3, 4, 255]));
        write_values.push(h);
        write_values.push(HashMap::new());
        write_values.push(HashMap::new());
        write_values.push(HashMap::new());
        let mut h = HashMap::new();
        h.insert("i64".to_string(), FieldValue::I64(200));
        h.insert("second i64".to_string(), FieldValue::I64(-600));
        h.insert("string".to_string(), FieldValue::String("RWGPS 2".to_string()));
        h.insert("bool array".to_string(), FieldValue::BoolArray(vec![]));
        h.insert("u64 array".to_string(), FieldValue::U64Array(vec![]));
        h.insert(
            "second string".to_string(),
            FieldValue::String("This is another string".to_string()),
        );
        h.insert("byte array".to_string(), FieldValue::ByteArray(vec![]));
        write_values.push(h);

        // Write
        let mut section = standard::Section::new(Schema::with_fields(vec![
            FieldDefinition::new("i64", DataType::I64),
            FieldDefinition::new("f64:2", DataType::F64 { scale: 2 }),
            FieldDefinition::new("f64:7", DataType::F64 { scale: 7 }),
            FieldDefinition::new("u64", DataType::U64),
            FieldDefinition::new("bool", DataType::Bool),
            FieldDefinition::new("second i64", DataType::I64),
            FieldDefinition::new("string", DataType::String),
            FieldDefinition::new("bool array", DataType::BoolArray),
            FieldDefinition::new("u64 array", DataType::U64Array),
            FieldDefinition::new("second string", DataType::String),
            FieldDefinition::new("byte array", DataType::ByteArray),
        ]));

        let fields = section.schema().fields().to_vec();

        for entry in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            for field_def in fields.iter() {
                if let Some(cw) = rowbuilder.next_column_writer() {
                    match cw {
                        ColumnWriter::I64ColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::I64(v) => Some(v),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::BoolColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::Bool(v) => Some(v),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::StringColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::String(v) => Some(v.as_str()),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::U64ColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::U64(v) => Some(v),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::F64ColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::F64(v) => Some(v),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::BoolArrayColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::BoolArray(v) => Some(v.as_slice()),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::U64ArrayColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::U64Array(v) => Some(v.as_slice()),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::ByteArrayColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::ByteArray(v) => Some(v.as_slice()),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                    }
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Standard(section)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<HashMap<String, FieldValue>> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Standard(section) = section {
                let mut section_reader = section.reader().unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let row = columniter
                        .filter_map(|row_result| {
                            if let (field_desc, Some(field_value)) = row_result.unwrap() {
                                Some((field_desc.name().to_string(), field_value))
                            } else {
                                None
                            }
                        })
                        .collect::<HashMap<_, _>>();

                    read_values.push(row);
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values);
    }

    #[test]
    fn roundtrip_encrypted_section() {
        let mut buf = vec![];
        let mut write_values: Vec<HashMap<String, FieldValue>> = Vec::new();
        let mut h = HashMap::new();
        h.insert("i64".to_string(), FieldValue::I64(1));
        h.insert("f64:2".to_string(), FieldValue::F64(0.12));
        h.insert("f64:7".to_string(), FieldValue::F64(0.1234567));
        h.insert("u64".to_string(), FieldValue::U64(80_000_000_000_000));
        h.insert("bool".to_string(), FieldValue::Bool(true));
        h.insert("second i64".to_string(), FieldValue::I64(12));
        h.insert("string".to_string(), FieldValue::String("RWGPS".to_string()));
        h.insert(
            "bool array".to_string(),
            FieldValue::BoolArray(vec![true, false, false, false, true, false]),
        );
        h.insert("u64 array".to_string(), FieldValue::U64Array(vec![100, 120, 140, 160]));
        h.insert(
            "second string".to_string(),
            FieldValue::String("This is a string".to_string()),
        );
        h.insert("byte array".to_string(), FieldValue::ByteArray(vec![3, 4, 255]));
        write_values.push(h);
        write_values.push(HashMap::new());
        write_values.push(HashMap::new());
        write_values.push(HashMap::new());
        let mut h = HashMap::new();
        h.insert("i64".to_string(), FieldValue::I64(200));
        h.insert("second i64".to_string(), FieldValue::I64(-600));
        h.insert("string".to_string(), FieldValue::String("RWGPS 2".to_string()));
        h.insert("bool array".to_string(), FieldValue::BoolArray(vec![]));
        h.insert("u64 array".to_string(), FieldValue::U64Array(vec![]));
        h.insert(
            "second string".to_string(),
            FieldValue::String("This is another string".to_string()),
        );
        h.insert("byte array".to_string(), FieldValue::ByteArray(vec![]));
        write_values.push(h);

        // Write
        let key_material = orion::aead::SecretKey::default().unprotected_as_bytes().to_vec();
        let mut section0 = encrypted::Section::new(
            &key_material,
            Schema::with_fields(vec![
                FieldDefinition::new("i64", DataType::I64),
                FieldDefinition::new("f64:2", DataType::F64 { scale: 2 }),
                FieldDefinition::new("f64:7", DataType::F64 { scale: 7 }),
                FieldDefinition::new("u64", DataType::U64),
                FieldDefinition::new("bool", DataType::Bool),
                FieldDefinition::new("second i64", DataType::I64),
                FieldDefinition::new("string", DataType::String),
                FieldDefinition::new("bool array", DataType::BoolArray),
                FieldDefinition::new("u64 array", DataType::U64Array),
                FieldDefinition::new("second string", DataType::String),
                FieldDefinition::new("byte array", DataType::ByteArray),
            ]),
        )
        .unwrap();

        let fields = section0.schema().fields().to_vec();

        for entry in write_values.iter() {
            let mut rowbuilder = section0.open_row_builder();

            for field_def in fields.iter() {
                if let Some(cw) = rowbuilder.next_column_writer() {
                    match cw {
                        ColumnWriter::I64ColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::I64(v) => Some(v),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::BoolColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::Bool(v) => Some(v),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::StringColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::String(v) => Some(v.as_str()),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::U64ColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::U64(v) => Some(v),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::F64ColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::F64(v) => Some(v),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::BoolArrayColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::BoolArray(v) => Some(v.as_slice()),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::U64ArrayColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::U64Array(v) => Some(v.as_slice()),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                        ColumnWriter::ByteArrayColumnWriter(cwi) => {
                            assert!(cwi
                                .write(
                                    entry
                                        .get(field_def.name())
                                        .map(|v| match v {
                                            FieldValue::ByteArray(v) => Some(v.as_slice()),
                                            _ => None,
                                        })
                                        .flatten(),
                                )
                                .is_ok());
                        }
                    }
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&Section::Encrypted(section0)]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<HashMap<String, FieldValue>> = vec![];
        for section in track_reader.sections() {
            if let tracklib::read::section::Section::Encrypted(mut section) = section {
                let mut section_reader = section.reader(&key_material).unwrap();
                while let Some(columniter) = section_reader.open_column_iter() {
                    let row = columniter
                        .filter_map(|row_result| {
                            if let (field_desc, Some(field_value)) = row_result.unwrap() {
                                Some((field_desc.name().to_string(), field_value))
                            } else {
                                None
                            }
                        })
                        .collect::<HashMap<_, _>>();

                    read_values.push(row);
                }
            }
        }

        // Compare
        assert_eq!(write_values, read_values);
    }
}
