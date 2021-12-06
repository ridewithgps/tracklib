use crate::consts::CRC16;
use crate::error::{Result, TracklibError};
use crate::types::{MetadataEntry, TrackType};
use std::convert::TryFrom;
use std::io::Write;

impl MetadataEntry {
    fn write(&self) -> Result<(u8, Vec<u8>)> {
        match self {
            Self::TrackType(track_type) => {
                // TrackType metadata is 5 bytes
                // 1 byte for the type of track
                // 4 bytes for the id of the track
                let (type_tag, id): (u8, &u32) = match track_type {
                    TrackType::Trip(id) => (0x00, id),
                    TrackType::Route(id) => (0x01, id),
                    TrackType::Segment(id) => (0x02, id),
                };
                let mut buf = Vec::with_capacity(5);
                buf.write_all(&type_tag.to_le_bytes())?;
                buf.write_all(&id.to_le_bytes())?;
                Ok((0x00, buf))
            }
            Self::CreatedAt(seconds_since_epoch) => {
                // CreatedAt metadata is 8 bytes: seconds since epoch
                Ok((0x01, seconds_since_epoch.to_le_bytes().to_vec()))
            }
        }
    }
}

#[rustfmt::skip]
pub(crate) fn write_metadata<W: Write>(out: &mut W, entries: Vec<MetadataEntry>) -> Result<usize> {
    let entry_count = u8::try_from(entries.len())?;

    let mut buf = Vec::new();

    buf.write_all(&entry_count.to_le_bytes())?;                    // 1 byte  - entry count

    for entry in entries {
        let (entry_type, entry_bytes) = entry.write()?;
        let entry_size = u16::try_from(entry_bytes.len())?;

        buf.write_all(&entry_type.to_le_bytes())?;                 // 1 byte  - entry type
        buf.write_all(&entry_size.to_le_bytes())?;                 // 2 bytes - entry size
        buf.write_all(&entry_bytes)?;                              // ? bytes - entry value
    }

    buf.write_all(&CRC16.checksum(&buf).to_le_bytes())?;           // 2 bytes - crc

    out.write_all(&buf)?;
    Ok(buf.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_write_empty_metadata() {
        let mut buf = vec![];
        let written = write_metadata(&mut buf, vec![]);
        assert!(written.is_ok());
        #[rustfmt::skip]
        let expected = &[0x00, // zero metadata entries
                         0x40, // crc
                         0xBF];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_only_track_type_trip() {
        let mut buf = vec![];
        let written = write_metadata(
            &mut buf,
            vec![MetadataEntry::TrackType(TrackType::Trip(400))],
        );
        assert!(written.is_ok());
        #[rustfmt::skip]
        let expected = &[0x01, // one metadata entry
                         0x00, // entry type: track_type = 0x00
                         0x05, // two byte entry size = 5
                         0x00,
                         0x00, // track type: trip = 0x00
                         0x90, // four byte trip ID = 400
                         0x01,
                         0x00,
                         0x00,
                         0xD1, // crc
                         0x5F];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_only_track_type_route() {
        let mut buf = vec![];
        let written = write_metadata(
            &mut buf,
            vec![MetadataEntry::TrackType(TrackType::Route(64))],
        );
        assert!(written.is_ok());
        #[rustfmt::skip]
        let expected = &[0x01, // one metadata entry
                         0x00, // entry type: track_type = 0x00
                         0x05, // two byte entry size = 5
                         0x00,
                         0x01, // track type: route = 0x01
                         0x40, // four byte route ID = 64
                         0x00,
                         0x00,
                         0x00,
                         0x85, // crc
                         0x9F];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_only_track_type_segment() {
        let mut buf = vec![];
        let written = write_metadata(
            &mut buf,
            vec![MetadataEntry::TrackType(TrackType::Segment(u32::MAX))],
        );
        assert!(written.is_ok());
        #[rustfmt::skip]
        let expected = &[0x01, // one metadata entry
                         0x00, // entry type: track_type = 0x00
                         0x05, // two byte entry size = 5
                         0x00,
                         0x02, // track type: segment = 0x02
                         0xFF, // four byte segment ID = 4,294,967,295
                         0xFF,
                         0xFF,
                         0xFF,
                         0xD5, // crc
                         0xCB];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_only_created_at_epoch() {
        let mut buf = vec![];
        let written = write_metadata(&mut buf, vec![MetadataEntry::CreatedAt(UNIX_EPOCH)]);
        assert!(written.is_ok());
        #[rustfmt::skip]
        let expected = &[0x01, // one metadata entry
                         0x01, // entry type: created_at = 0x01
                         0x08, // two byte entry size = 8
                         0x00,
                         0x00, // eight byte timestamp: zero seconds elapsed
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0xE3, // crc
                         0x28];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_only_created_at_future() {
        let mut buf = vec![];
        let the_future = Duration::from_millis(u64::MAX).as_secs();
        let written = write_metadata(&mut buf, vec![MetadataEntry::CreatedAt(the_future)]);
        assert!(written.is_ok());
        #[rustfmt::skip]
        let expected = &[0x01, // one metadata entry
                         0x01, // entry type: created_at = 0x01
                         0x08, // two byte entry size = 8
                         0x00,
                         0xEF, // eight byte timestamp: lots of seconds elapsed
                         0xA7,
                         0xC6,
                         0x4B,
                         0x37,
                         0x89,
                         0x41,
                         0x00,
                         0x21, // crc
                         0x4C];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_both() {
        let mut buf = vec![];
        let written = write_metadata(
            &mut buf,
            vec![
                MetadataEntry::TrackType(TrackType::Trip(20)),
                MetadataEntry::CreatedAt(0),
            ],
        );
        assert!(written.is_ok());
        #[rustfmt::skip]
        let expected = &[0x02, // two metadata entries
                         0x00, // entry type: track_type = 0x00
                         0x05, // two byte entry size = 5
                         0x00,
                         0x00, // track type: trip = 0x00
                         0x14, // four byte trip ID = 20
                         0x00,
                         0x00,
                         0x00,
                         0x01, // entry type: created_at = 0x01
                         0x08, // two byte entry size = 8
                         0x00,
                         0x00, // eight byte timestamp: zero seconds elapsed
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x23, // crc
                         0xD2];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_duplicate_types() {
        let mut buf = vec![];
        let written = write_metadata(
            &mut buf,
            vec![
                MetadataEntry::TrackType(TrackType::Trip(20)),
                MetadataEntry::TrackType(TrackType::Trip(21)),
                MetadataEntry::TrackType(TrackType::Route(22)),
            ],
        );
        assert!(written.is_ok());
        #[rustfmt::skip]
        let expected = &[0x03, // three metadata entries
                         0x00, // entry type: track_type = 0x00
                         0x05, // two byte entry size = 5
                         0x00,
                         0x00, // track type: trip = 0x00
                         0x14, // four byte trip ID = 20
                         0x00,
                         0x00,
                         0x00,
                         0x00, // entry type: track_type = 0x00
                         0x05, // two byte entry size = 5
                         0x00,
                         0x00, // track type: trip = 0x00
                         0x15, // four byte trip ID = 21
                         0x00,
                         0x00,
                         0x00,
                         0x00, // entry type: track_type = 0x00
                         0x05, // two byte entry size = 5
                         0x00,
                         0x01, // track type: route = 0x01
                         0x16, // four byte route ID = 22
                         0x00,
                         0x00,
                         0x00,
                         0xDE, // crc
                         0x57];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }
}
