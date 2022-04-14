// Only run these tests in a release build because they rely on unchecked math
#[cfg(not(debug_assertions))]
mod tests {
    use tracklib2::read::track::TrackReader;
    use tracklib2::schema::*;
    use tracklib2::types::{FieldValue, SectionEncoding};
    use tracklib2::write::section::{ColumnWriter, Section};
    use tracklib2::write::track::write_track;

    #[test]
    fn roundtrip_i64() {
        let mut buf = vec![];
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
        let mut section = Section::new(
            SectionEncoding::Standard,
            Schema::with_fields(vec![FieldDefinition::new("v", DataType::I64)]),
        );
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::I64ColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&section]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<i64> = vec![];
        for section in track_reader.sections() {
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

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_f64() {
        let mut buf = vec![];
        let write_values = &[-200.101, 0.0, 0.1];

        // Write
        let mut section = Section::new(
            SectionEncoding::Standard,
            Schema::with_fields(vec![FieldDefinition::new("v", DataType::F64 { scale: 7 })]),
        );
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::F64ColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&section]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<f64> = vec![];
        for section in track_reader.sections() {
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

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_bool() {
        let mut buf = vec![];
        let write_values = &[false, true, true, true, false, false, true];

        // Write
        let mut section = Section::new(
            SectionEncoding::Standard,
            Schema::with_fields(vec![FieldDefinition::new("v", DataType::Bool)]),
        );
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::BoolColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&section]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<bool> = vec![];
        for section in track_reader.sections() {
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

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }

    #[test]
    fn roundtrip_string() {
        let mut buf = vec![];
        let write_values = &[
            "",
            "longer string",
            r"reallllllllllllllllllllllllllllllllllllllyyyyyyyyyyyyyyyyyyyyyyy
              longggggggggggggggggggggggggggggggggggggggggggggggggggggggggg
              stringgggggggggggggggggggggggggggggggggg",
        ];

        // Write
        let mut section = Section::new(
            SectionEncoding::Standard,
            Schema::with_fields(vec![FieldDefinition::new("v", DataType::String)]),
        );
        for v in write_values.iter() {
            let mut rowbuilder = section.open_row_builder();

            while let Some(cw) = rowbuilder.next_column_writer() {
                if let ColumnWriter::StringColumnWriter(cwi) = cw {
                    assert!(cwi.write(Some(v)).is_ok());
                }
            }
        }
        assert!(write_track(&mut buf, &[], &[&section]).is_ok());

        // Read
        let track_reader = TrackReader::new(&buf).unwrap();
        let mut read_values: Vec<String> = vec![];
        for section in track_reader.sections() {
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

        // Compare
        assert_eq!(write_values, read_values.as_slice());
    }
}
