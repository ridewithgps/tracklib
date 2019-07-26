use snafu::{Snafu, ResultExt};
use std::io::{Write};
use std::convert::{TryFrom};
use crate::section::{Section, SectionType, Error as SectionError};
use crate::metadata::{RWTFMetadata, TrackType, Error as MetadataError};
use crate::utils::{write};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Couldn't write header: {}", source))]
    WriteHeader{source: std::io::Error},
    #[snafu(display("Couldn't write metadata table: {}", source))]
    WriteMetadataTable{source: MetadataError},
    #[snafu(display("Couldn't write: {}", source))]
    WriteBytes{source: std::io::Error},
    #[snafu(display("Number truncation error: {}", source))]
    NumberTruncation{source: std::num::TryFromIntError},
    #[snafu(display("Couldn't add track point: {}", source))]
    AddTrackPoint{source: SectionError},
    #[snafu(display("Couldn't add course point: {}", source))]
    AddCoursePoint{source: SectionError},
    #[snafu(display("Couldn't write section data: {}", source))]
    WriteSection{source: SectionError},
    #[snafu(display("Couldn't write file trailer: {}", source))]
    WriteTrailer{source: std::io::Error},
    #[snafu(display("Couldn't decode base64: {}", source))]
    DecodeBase64{source: base64::DecodeError},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum DataField {
    Number(i64),
    LongFloat(f64),
    ShortFloat(f64),
    Base64(String),
    String(String),
    Bool(bool),
    IDs(Vec<u64>),
}

impl From<i64> for DataField {
    fn from(v: i64) -> Self {
        DataField::Number(v)
    }
}

impl From<String> for DataField {
    fn from(v: String) -> Self {
        DataField::String(v)
    }
}

impl From<bool> for DataField {
    fn from(v: bool) -> Self {
        DataField::Bool(v)
    }
}

impl From<Vec<u64>> for DataField {
    fn from(v: Vec<u64>) -> Self {
        DataField::IDs(v)
    }
}

use serde::ser::{Serialize, Serializer, SerializeSeq, SerializeMap};

impl Serialize for DataField {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DataField::Number(v) => serializer.serialize_i64(*v),
            DataField::LongFloat(v) => serializer.serialize_f64(*v),
            DataField::ShortFloat(v) => serializer.serialize_f64(*v),
            DataField::Base64(v) => serializer.serialize_str(v),
            DataField::String(v) => serializer.serialize_str(v),
            DataField::Bool(v) => serializer.serialize_bool(*v),
            DataField::IDs(v) => {
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for e in v {
                    seq.serialize_element(e)?;
                }
                seq.end()
            }
        }
    }
}

pub const RWTFMAGIC: [u8; 8] = [0x89,  // non-ascii
                                0x52,  // R
                                0x57,  // W
                                0x54,  // T
                                0x46,  // F
                                0x0A,  // newline
                                0x1A,  // ctrl-z
                                0x0A]; // newline

pub(crate) const RWTFTRAILER: [u8; 5] = [0xff,  // SectionType
                                         0x46,  // F
                                         0x54,  // T
                                         0x57,  // W
                                         0x52]; // R

#[derive(Debug)]
pub struct RWTFHeader {
    pub(crate) file_version: u8,
    pub(crate) creator_version: u8,
}

impl RWTFHeader {
    fn new() -> Self {
        RWTFHeader{file_version: 0,
                   creator_version: 0}
    }

    pub fn file_version(&self) -> u8 {
        self.file_version
    }

    pub fn creator_version(&self) -> u8 {
        self.creator_version
    }

    fn write<W: Write>(&self, out: &mut W, metadata_table_offset: u16, data_offset: u16) -> Result<usize> {
        let mut buf = Vec::with_capacity(24);

        // Write 8 bytes - Magic Number
        write(&mut buf, &RWTFMAGIC).context(WriteHeader{})?;

        // Write 1 byte - File Version
        write(&mut buf, &self.file_version.to_le_bytes()).context(WriteHeader{})?;

        // Write 3 bytes - File Version Reserve
        write(&mut buf, &[0x00, 0x00, 0x00]).context(WriteHeader{})?;

        // Write 1 byte - Creator Version
        write(&mut buf, &self.creator_version.to_le_bytes()).context(WriteHeader{})?;

        // Write 3 bytes - Creator Version Reserve
        write(&mut buf, &[0x00, 0x00, 0x00]).context(WriteHeader{})?;

        // Write 2 bytes - Offset to Metadata Table
        write(&mut buf, &metadata_table_offset.to_le_bytes()).context(WriteHeader{})?;

        // Write 2 bytes - Offset to Data
        write(&mut buf, &data_offset.to_le_bytes()).context(WriteHeader{})?;

        // Write 2 bytes - E Reserve
        write(&mut buf, &[0x00, 0x00]).context(WriteHeader{})?;

        // Write 2 bytes - Header CRC
        let crc = crc::crc16::checksum_usb(&buf).to_le_bytes();
        write(&mut buf, &crc).context(WriteHeader{})?;

        // Write buf -> out
        let written = write(out, &buf).context(WriteHeader{})?;

        Ok(written)
    }
}

#[derive(Debug)]
pub struct RWTFile {
    pub(crate) header: RWTFHeader,
    pub(crate) metadata: RWTFMetadata,
    pub track_points: Section,
    pub course_points: Section,
}

impl RWTFile {
    pub fn new() -> Self {
        Self{header: RWTFHeader::new(),
             metadata: RWTFMetadata::new(None, None),
             track_points: Section::new(SectionType::TrackPoints),
             course_points: Section::new(SectionType::CoursePoints)}
    }

    pub fn with_track_type(track_type: TrackType) -> Self {
        Self{header: RWTFHeader::new(),
             metadata: RWTFMetadata::new(None, Some(track_type)),
             track_points: Section::new(SectionType::TrackPoints),
             course_points: Section::new(SectionType::CoursePoints)}
    }

    pub fn header(&self) -> &RWTFHeader {
        &self.header
    }

    fn add_point<V: Into<DataField>>(section: &mut Section, index: usize, k: &str, v: V) -> Result<()>{
        match v.into() {
            DataField::Number(v) => section.add_number(index, k, v).eager_context(AddTrackPoint),
            DataField::LongFloat(v) => section.add_long_float(index, k, v).eager_context(AddTrackPoint),
            DataField::ShortFloat(v) => section.add_short_float(index, k, v).eager_context(AddTrackPoint),
            DataField::Base64(v) => section.add_base64(index, k, base64::decode(&v).context(DecodeBase64)?).eager_context(AddTrackPoint),
            DataField::String(v) => section.add_string(index, k, v).eager_context(AddTrackPoint),
            DataField::Bool(v) => section.add_bool(index, k, v).eager_context(AddTrackPoint),
            DataField::IDs(v) => section.add_ids(index, k, v).eager_context(AddTrackPoint),
        }
    }

    pub fn add_track_point<V: Into<DataField>>(&mut self, index: usize, k: &str, v: V) -> Result<()>{
        Self::add_point(&mut self.track_points, index, k, v)
    }

    pub fn add_course_point<V: Into<DataField>>(&mut self, index: usize, k: &str, v: V) -> Result<()>{
        Self::add_point(&mut self.course_points, index, k, v)
    }

    pub fn metadata(&self) -> &RWTFMetadata {
        &self.metadata
    }

    pub fn write<W: Write>(&self, out: &mut W) -> Result<usize> {
        // Prepare all the data
        let mut metadata_table_buf = vec![];
        self.metadata.write(&mut metadata_table_buf).context(WriteMetadataTable)?;

        let mut track_points_buf = vec![];
        if self.track_points.len() > 0 {
            self.track_points.write(&mut track_points_buf).context(WriteSection)?;
        }

        let mut course_points_buf = vec![];
        if self.course_points.len() > 0 {
            self.course_points.write(&mut course_points_buf).context(WriteSection)?;
        }

        let header_size: u16 = 24;
        let metadata_table_offset: u16 = header_size;
        let data_offset: u16 = metadata_table_offset + u16::try_from(metadata_table_buf.len()).context(NumberTruncation{})?;

        // Write all the data
        let mut written = self.header.write(out, metadata_table_offset, data_offset)?;
        written += write(out, &metadata_table_buf).context(WriteBytes)?;
        written += write(out, &track_points_buf).context(WriteBytes)?;
        written += write(out, &course_points_buf).context(WriteBytes)?;
        written += write(out, &RWTFTRAILER).context(WriteTrailer)?;

        Ok(written)
    }
}

impl Serialize for RWTFile {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;

        if self.track_points.len() > 0 {
            map.serialize_entry("track_points", &self.track_points)?;
        }

        if self.course_points.len() > 0 {
            map.serialize_entry("course_points", &self.course_points)?;
        }

        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn testfoo() {
        let mut f = RWTFile::new();
        assert!(f.add_track_point(1, "foo", 5).is_ok());
        assert!(f.add_track_point(1, "bar", DataField::String("hey".into())).is_ok());
        assert!(f.add_track_point(2, "foo", 0).is_ok());
        assert!(f.add_track_point(2, "bar", 7).is_err());
        assert!(f.add_track_point(2, "bar", DataField::String("5,6,7".into())).is_ok());
        assert!(f.add_track_point(2, "bar", DataField::String("10,11,12".into())).is_err());
        assert!(f.add_track_point(1, "baz", DataField::LongFloat(0.3)).is_ok());
        assert!(f.add_track_point(2, "baz", DataField::ShortFloat(0.3)).is_err());
        assert!(f.add_track_point(2, "baz", DataField::LongFloat(0.3)).is_ok());
        assert!(f.add_track_point(1, "bam", DataField::ShortFloat(0.3)).is_ok());
    }

    #[test]
    fn test_base64() {
        let mut f = RWTFile::new();
        assert!(f.add_track_point(1, "foo", DataField::Base64("SGVsbG8sIFdvcmxkIQ==".into())).is_ok());
        assert!(f.add_track_point(1, "foo", DataField::Base64("invalid base64".into())).is_err());
    }

    #[test]
    fn test_write_header() {
        let f = RWTFHeader::new();
        let mut buf = vec![];
        let written = f.write(&mut buf, 0x0A, 0x1A);
        assert!(written.is_ok());
        let expected = &[0x89, // magic number
                         0x52,
                         0x57,
                         0x54,
                         0x46,
                         0x0A,
                         0x1A,
                         0x0A,
                         0x00, // file version
                         0x00, // file version reserved space
                         0x00,
                         0x00,
                         0x00, // creator version
                         0x00, // creator version reserved space
                         0x00,
                         0x00,
                         0x0A, // metadata table offset
                         0x00,
                         0x1A, // data offset
                         0x00,
                         0x00, // e reserved space
                         0x00,
                         0x86, // header crc
                         0xB7];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }
}
