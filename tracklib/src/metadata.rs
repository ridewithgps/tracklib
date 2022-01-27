use std::io::{Write};
use snafu::{Snafu, ResultExt};
use std::time::{UNIX_EPOCH, SystemTime, SystemTimeError};
use serde::ser::{Error as SerError, Serialize, Serializer, SerializeMap};
use crate::utils::{write};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Couldn't write metadata table: {}", source))]
    WriteMetadataTable{source: std::io::Error},
    #[snafu(display("Couldn't compute the system time: {}", source))]
    GetTime{source: SystemTimeError},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TrackType {
    Trip(u32),
    Route(u32),
    Segment(u32)
}

impl TrackType {
    pub fn id(&self) -> u32 {
        match self {
            TrackType::Trip(id)    => *id,
            TrackType::Route(id)   => *id,
            TrackType::Segment(id) => *id,
        }
    }

    fn type_tag(&self) -> u8 {
        match self {
            TrackType::Trip(_)    => 0x00,
            TrackType::Route(_)   => 0x01,
            TrackType::Segment(_) => 0x02,
        }
    }

    pub(crate) fn from_tag(tag: u8, id: u32) -> Option<Self> {
        match tag {
            0x00 => Some(TrackType::Trip(id)),
            0x01 => Some(TrackType::Route(id)),
            0x02 => Some(TrackType::Segment(id)),
            _ => None
        }
    }

    fn id_to_le_bytes(&self) -> [u8; 4] {
        match self {
            TrackType::Trip(id)    => id.to_le_bytes(),
            TrackType::Route(id)   => id.to_le_bytes(),
            TrackType::Segment(id) => id.to_le_bytes(),
        }
    }
}

impl Serialize for TrackType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (type_name, id) = match self {
            TrackType::Trip(id)    => ("trip", id),
            TrackType::Route(id)   => ("route", id),
            TrackType::Segment(id) => ("segment", id),
        };
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("type", type_name)?;
        map.serialize_entry("id", id)?;
        map.end()
    }
}

#[derive(Debug)]
pub struct RWTFMetadata {
    created_at: Option<SystemTime>,
    track_type: Option<TrackType>,
}

impl RWTFMetadata {
    pub(crate) fn new(created_at: Option<SystemTime>, track_type: Option<TrackType>) -> Self {
        RWTFMetadata{created_at: created_at,
                     track_type: track_type}
    }

    pub fn created_at(&self) -> Option<SystemTime> {
        self.created_at
    }

    pub fn track_type(&self) -> Option<TrackType> {
        self.track_type
    }

    fn write_created_at<W: Write>(&self, out: &mut W) -> Result<usize> {
        let mut written = 0;
        let now_buf = SystemTime::now().duration_since(UNIX_EPOCH).context(GetTime)?.as_secs().to_le_bytes();

        // write the type of the entry: created_at = 0x01
        written += write(out, &[0x01]).context(WriteMetadataTable{})?;

        // write size-prefixed entry data
        const ENTRY_SIZE: u16 = 8;
        let entry_size_buf: [u8; 2] = ENTRY_SIZE.to_le_bytes();
        written += write(out, &entry_size_buf).context(WriteMetadataTable{})?;
        written += write(out, &now_buf).context(WriteMetadataTable{})?;

        Ok(written)
    }

    fn write_track_type<W: Write>(&self, out: &mut W, track_type: &TrackType) -> Result<usize> {
        let mut written = 0;

        // write the type of the entry: track_type = 0x00
        written += write(out, &[0x00]).context(WriteMetadataTable{})?;

        // write size-prefixed entry data
        const ENTRY_SIZE: u16 = 5;
        let entry_size_buf: [u8; 2] = ENTRY_SIZE.to_le_bytes();
        written += write(out, &entry_size_buf).context(WriteMetadataTable{})?;
        written += write(out, &track_type.type_tag().to_le_bytes()).context(WriteMetadataTable{})?;
        written += write(out, &track_type.id_to_le_bytes()).context(WriteMetadataTable{})?;

        Ok(written)
    }

    pub(crate) fn write<W: Write>(&self, out: &mut W) -> Result<usize> {
        let mut buf = Vec::new();

        if let Some(track_type) = self.track_type {
            // there are two entries - the track type and created_at
            write(&mut buf, &[0x02]).context(WriteMetadataTable{})?;

            self.write_created_at(&mut buf)?;
            self.write_track_type(&mut buf, &track_type)?;
        } else {
            // self.track_type isn't set so there is just one entry: created_at
            write(&mut buf, &[0x01]).context(WriteMetadataTable{})?;

            self.write_created_at(&mut buf)?;
        }

        // Write 2 bytes - CRC
        let crc = crc::crc16::checksum_usb(&buf).to_le_bytes();
        write(&mut buf, &crc).context(WriteMetadataTable{})?;

        // Write buf -> out
        let written = write(out, &buf).context(WriteMetadataTable{})?;

        Ok(written)
    }
}

impl Serialize for RWTFMetadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if let Some(created_at) = self.created_at {
            let unix_time = created_at.duration_since(UNIX_EPOCH).map_err(SerError::custom)?.as_secs();
            map.serialize_entry("created_at", &unix_time)?;
        }
        if let Some(track_type) = self.track_type {
            map.serialize_entry("track_type", &track_type)?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::{TryInto};
    use std::time::{UNIX_EPOCH};

    fn test_buf(buf: &[u8], expected_head: &[u8], expected_tail: &[u8]) {
        const CREATED_AT_LEN: usize = 8;
        const CRC_LEN: usize = 2;

        // is the length correct
        assert_eq!(buf.len(), expected_head.len() + CREATED_AT_LEN + expected_tail.len() + CRC_LEN);

        // is the beginning of the buffer correct
        assert!(buf.starts_with(expected_head));

        // is the timestamp ~now
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let created_at = u64::from_le_bytes(buf[expected_head.len()..expected_head.len() + CREATED_AT_LEN].try_into().unwrap());
        assert!(now - created_at < 3);

        // is the end of the buffer correct
        assert_eq!(&buf[expected_head.len() + CREATED_AT_LEN..buf.len() - CRC_LEN], expected_tail);

        // is the crc correct
        assert!(buf.ends_with(&crc::crc16::checksum_usb(&buf[..buf.len() - 2]).to_le_bytes()));
    }

    #[test]
    fn test_write_metadata_table_without_track_type() {
        let m = RWTFMetadata::new(None, None);

        let mut buf = vec![];
        let written = m.write(&mut buf);
        assert!(written.is_ok());
        let expected_head = &[0x01, // 1 entry in the table
                              0x01, // entry is of type created_at
                              0x08, // entry data is 8 bytes
                              0x00];
        test_buf(&buf, expected_head, &[]);
    }

    #[test]
    fn test_write_metadata_table_with_segment() {
        let m = RWTFMetadata::new(None, Some(TrackType::Segment(0x42)));

        let mut buf = vec![];
        let written = m.write(&mut buf);
        assert!(written.is_ok());
        let expected_head = &[0x02, // 2 table entries
                              0x01, // entry #1 is of type created_at
                              0x08, // entry data is 8 bytes
                              0x00];
        let expected_tail = &[0x00, // entry #2 is of type track_type
                              0x05, // entry data is 5 bytes
                              0x00,
                              0x02, // TrackType::Segment
                              0x42, // the segment id
                              0x00,
                              0x00,
                              0x00];
        test_buf(&buf, expected_head, expected_tail);
    }

    #[test]
    fn test_write_metadata_table_with_route() {
        let m = RWTFMetadata::new(None, Some(TrackType::Route(2u32.pow(16)-1)));

        let mut buf = vec![];
        let written = m.write(&mut buf);
        assert!(written.is_ok());
        let expected_head = &[0x02, // 2 table entries
                              0x01, // entry #1 is of type created_at
                              0x08, // entry data is 8 bytes
                              0x00];
        let expected_tail = &[0x00, // entry #2 is of type track_type
                              0x05, // entry data is 5 bytes
                              0x00,
                              0x01, // TrackType::Route
                              0xff, // the segment id
                              0xff,
                              0x00,
                              0x00];
        test_buf(&buf, expected_head, expected_tail);
    }

    #[test]
    fn test_write_metadata_table_with_trip() {
        let m = RWTFMetadata::new(None, Some(TrackType::Trip(std::u32::MAX)));

        let mut buf = vec![];
        let written = m.write(&mut buf);
        assert!(written.is_ok());
        let expected_head = &[0x02, // 2 table entries
                              0x01, // entry #1 is of type created_at
                              0x08, // entry data is 8 bytes
                              0x00];
        let expected_tail = &[0x00, // entry #2 is of type track_type
                              0x05, // entry data is 5 bytes
                              0x00,
                              0x00, // TrackType::Trip
                              0xff, // the segment id
                              0xff,
                              0xff,
                              0xff];
        test_buf(&buf, expected_head, expected_tail);
    }

    #[test]
    fn test_roundtrip_metadata() {
        let created_at = Some(SystemTime::now());
        let tt = Some(TrackType::Trip(42));
        let m = RWTFMetadata::new(created_at, tt);

        assert_eq!(m.created_at(), created_at);
        assert_eq!(m.track_type(), tt);
        assert_eq!(m.track_type().map(|tt| tt.id()), Some(42));
    }
}
