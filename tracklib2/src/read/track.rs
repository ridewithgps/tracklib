use super::data_table::{parse_data_table, DataTableEntry};
use super::header::{parse_header, Header};
use super::metadata::parse_metadata;
use super::section::Section;
use crate::error::Result;
use crate::types::MetadataEntry;

#[cfg_attr(test, derive(Debug))]
pub struct TrackReader<'a> {
    header: Header,
    metadata_entries: Vec<MetadataEntry>,
    data_table: Vec<DataTableEntry>,
    data_start: &'a [u8],
}

impl<'a> TrackReader<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self> {
        let (_, header) = parse_header(data)?;
        let (_, metadata_entries) = parse_metadata(&data[usize::from(header.metadata_offset())..])?;
        let (data_start, data_table) =
            parse_data_table(&data[usize::from(header.data_offset())..])?;

        Ok(Self {
            header,
            metadata_entries,
            data_table,
            data_start,
        })
    }

    pub fn file_version(&self) -> u8 {
        self.header.file_version()
    }

    pub fn creator_version(&self) -> u8 {
        self.header.creator_version()
    }

    pub fn metadata(&self) -> &[MetadataEntry] {
        &self.metadata_entries
    }

    pub fn section(&self, index: usize) -> Option<Section> {
        let section = self.data_table.get(index)?;
        let data = &self.data_start[usize::try_from(section.offset()).expect("usize != u64")..];
        Some(Section::new(data, &section))
    }

    pub fn sections(&self) -> SectionIter {
        SectionIter {
            data: &self.data_start,
            entries: &self.data_table,
        }
    }

    pub fn section_count(&self) -> usize {
        self.data_table.len()
    }
}

pub struct SectionIter<'a> {
    data: &'a [u8],
    entries: &'a [DataTableEntry],
}

impl<'a> Iterator for SectionIter<'a> {
    type Item = Section<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((section, rest)) = self.entries.split_first() {
            self.entries = rest;

            let data = &self.data[usize::try_from(section.offset()).expect("usize != u64")..];
            Some(Section::new(data, &section))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;
    use crate::types::{FieldValue, SectionEncoding, TrackType};
    use assert_matches::assert_matches;

    #[test]
    fn test_read_a_track() {
        #[rustfmt::skip]
        let buf = &[
            // Header
            0x89, // rwtfmagic
            0x52,
            0x57,
            0x54,
            0x46,
            0x0A,
            0x1A,
            0x0A,
            0x01, // file version
            0x00, // fv reserve
            0x00,
            0x00,
            0x00, // creator version
            0x00, // cv reserve
            0x00,
            0x00,
            0x18, // metadata table offset
            0x00,
            0x1F, // data offset
            0x00,
            0x00, // e reserve
            0x00,
            0x85, // header crc
            0xC8,

            // Metadata Table
            0x01, // one entry
            0x00, // entry type: track_type
            0x02, // entry size
            0x02, // track type: segment
            0x05, // segment id
            0x86, // crc
            0x9C,

            // Data Table
            0x02, // two sections

            // Data Table Section 1
            0x00, // section encoding = standard
            0x05, // leb128 point count
            0x33, // leb128 data size

            // Schema for Section 1
            0x00, // schema version
            0x03, // field count
            0x00, // first field type = I64
            0x01, // name len
            b'm', // name
            0x09, // leb128 data size
            0x10, // second field type = Bool
            0x01, // name len
            b'k', // name
            0x09, // leb128 data size
            0x20, // third field type = String
            0x01, // name len
            b'j', // name
            0x18, // leb128 data size

            // Data Table Section 2
            0x00, // section encoding = standard
            0x03, // leb128 point count
            0x26, // leb128 data size

            // Schema for Section 2
            0x00, // schema version
            0x03, // field count
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

            // Data Table CRC
            0x34,
            0x2E,

            // Data Section 1

            // Presence Column
            0b00000111,
            0b00000111,
            0b00000111,
            0b00000111,
            0b00000111,
            0xF6, // crc
            0xF8,
            0x0D,
            0x73,

            // Data Column 1 = I64
            0x2A, // 42
            0x00, // no change
            0x00, // no change
            0x00, // no change
            0x00, // no change
            0xD0, // crc
            0x8D,
            0x79,
            0x68,

            // Data Column 2 = Bool
            0x01, // true
            0x01, // true
            0x01, // true
            0x01, // true
            0x01, // true
            0xB5, // crc
            0xC9,
            0x8F,
            0xFA,

            // Data Column 3 = String
            0x03, // length 3
            b'h',
            b'e',
            b'y',
            0x03, // length 3
            b'h',
            b'e',
            b'y',
            0x03, // length 3
            b'h',
            b'e',
            b'y',
            0x03, // length 3
            b'h',
            b'e',
            b'y',
            0x03, // length 3
            b'h',
            b'e',
            b'y',
            0x36, // crc
            0x71,
            0x24,
            0x0B,

            // Data Section 2

            // Presence Column
            0b00000111,
            0b00000101,
            0b00000111,
            0x1A, // crc
            0x75,
            0xEA,
            0xC4,

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
            0x48];

        let track = assert_matches!(TrackReader::new(buf), Ok(track) => track);

        assert_eq!(track.file_version(), 1);
        assert_eq!(track.creator_version(), 0);
        assert_eq!(track.section_count(), 2);
        assert_eq!(track.metadata().len(), 1);

        assert_matches!(
            track.metadata()[0],
            MetadataEntry::TrackType(TrackType::Segment(5))
        );

        let expected_section_0 = vec![
            vec![
                Some(FieldValue::I64(42)),
                Some(FieldValue::Bool(true)),
                Some(FieldValue::String("hey".to_string())),
            ],
            vec![
                Some(FieldValue::I64(42)),
                Some(FieldValue::Bool(true)),
                Some(FieldValue::String("hey".to_string())),
            ],
            vec![
                Some(FieldValue::I64(42)),
                Some(FieldValue::Bool(true)),
                Some(FieldValue::String("hey".to_string())),
            ],
            vec![
                Some(FieldValue::I64(42)),
                Some(FieldValue::Bool(true)),
                Some(FieldValue::String("hey".to_string())),
            ],
            vec![
                Some(FieldValue::I64(42)),
                Some(FieldValue::Bool(true)),
                Some(FieldValue::String("hey".to_string())),
            ],
        ];

        let expected_section_1 = vec![
            vec![
                Some(FieldValue::I64(1)),
                Some(FieldValue::Bool(false)),
                Some(FieldValue::String("Ride".to_string())),
            ],
            vec![
                Some(FieldValue::I64(2)),
                None,
                Some(FieldValue::String("with".to_string())),
            ],
            vec![
                Some(FieldValue::I64(4)),
                Some(FieldValue::Bool(true)),
                Some(FieldValue::String("GPS".to_string())),
            ],
        ];

        let sections = track
            .sections()
            .map(|section| {
                let mut section_reader = section.reader()?;
                let mut v = vec![];
                while let Some(columniter) = section_reader.open_column_iter() {
                    v.push(
                        columniter
                            .map(|column_result| {
                                let (_field_def, field_value) = column_result.unwrap();
                                field_value
                            })
                            .collect::<Vec<_>>(),
                    );
                }
                Ok(v)
            })
            .collect::<Result<Vec<_>>>();

        assert_matches!(sections , Ok(sections) => {
            assert_eq!(sections.len(), 2);
            assert_eq!(sections[0], expected_section_0);
            assert_eq!(sections[1], expected_section_1);
        });

        assert_matches!(track.section(0), Some(section) => {
            assert_eq!(section.encoding(), SectionEncoding::Standard);
            assert_eq!(section.rows(), 5);
            assert_eq!(section.schema(), Schema::with_fields(vec![
                FieldDefinition::new("m", DataType::I64),
                FieldDefinition::new("k", DataType::Bool),
                FieldDefinition::new("j", DataType::String),
            ]));
            assert_matches!(section.reader(), Ok(mut section_reader) => {
                let mut v = vec![];
                while let Some(columniter) = section_reader.open_column_iter() {
                    v.push(columniter
                           .map(|column_result| {
                               let (_field_def, field_value) = column_result.unwrap();
                               field_value
                           })
                           .collect::<Vec<_>>());
                }
                assert_eq!(v, expected_section_0);
            });
        });

        assert_matches!(track.section(1), Some(section) => {
            assert_eq!(section.encoding(), SectionEncoding::Standard);
            assert_eq!(section.rows(), 3);
            assert_eq!(section.schema(), Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
            ]));
            assert_matches!(section.reader(), Ok(mut section_reader) => {
                let mut v = vec![];
                while let Some(columniter) = section_reader.open_column_iter() {
                    v.push(columniter
                           .map(|column_result| {
                               let (_field_def, field_value) = column_result.unwrap();
                               field_value
                           })
                           .collect::<Vec<_>>());
                }
                assert_eq!(v, expected_section_1);
            });
        });

        assert!(track.section(2).is_none());
    }
}
