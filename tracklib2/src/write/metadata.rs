use super::crcwriter::CrcWriter;
use crate::error::Result;
use crate::types::{MetadataEntry, TrackType};
use std::convert::TryFrom;
use std::io::Write;

impl MetadataEntry {
    #[rustfmt::skip]
    fn write<W: Write>(&self, out: &mut W) -> Result<()> {
        match self {
            Self::TrackType(track_type) => {
                let (type_tag, id): (u8, &u32) = match track_type {
                    TrackType::Trip(id) => (0x00, id),
                    TrackType::Route(id) => (0x01, id),
                    TrackType::Segment(id) => (0x02, id),
                };
                out.write_all(&[0x00])?;                            // 1 byte  - entry type
                out.write_all(&[0x05, 0x00])?;                      // 2 bytes - entry size
                out.write_all(&type_tag.to_le_bytes())?;            // 1 byte  - TrackType type tag
                out.write_all(&id.to_le_bytes())?;                  // 4 bytes - TrackType id
                Ok(())
            }
            Self::CreatedAt(seconds_since_epoch) => {
                out.write_all(&[0x01])?;                            // 1 byte  - entry type
                out.write_all(&[0x08, 0x00])?;                      // 2 bytes - entry size
                out.write_all(&seconds_since_epoch.to_le_bytes())?; // 8 bytes - created_at
                Ok(())
            }
        }
    }
}

#[rustfmt::skip]
pub(crate) fn write_metadata<W: Write>(out: &mut W, entries: &[MetadataEntry]) -> Result<()> {
    let mut crcwriter = CrcWriter::new16(out);

    crcwriter.write_all(&u8::try_from(entries.len())?.to_le_bytes())?; // 1 byte  - entry count
    for entry in entries {
        entry.write(&mut crcwriter)?;                                  // ? bytes - entry contents
    }
    crcwriter.append_crc()?;                                           // 2 bytes - crc

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::time::Duration;

    #[test]
    fn test_write_empty_metadata() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x00, // zero metadata entries
                         0x40, // crc
                         0xBF]);
        });
    }

    #[test]
    fn test_only_track_type_trip() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::TrackType(TrackType::Trip(400))]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x01, // one metadata entry
                         0x00, // entry type: track_type = 0x00
                         0x05, // two byte entry size = 5
                         0x00,
                         0x00, // track type: trip = 0x00
                         0x90, // four byte trip ID = 400
                         0x01,
                         0x00,
                         0x00,
                         0xD1, // crc
                         0x5F]);
        });
    }

    #[test]
    fn test_only_track_type_route() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::TrackType(TrackType::Route(64))]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x01, // one metadata entry
                         0x00, // entry type: track_type = 0x00
                         0x05, // two byte entry size = 5
                         0x00,
                         0x01, // track type: route = 0x01
                         0x40, // four byte route ID = 64
                         0x00,
                         0x00,
                         0x00,
                         0x85, // crc
                         0x9F]);
        });
    }

    #[test]
    fn test_only_track_type_segment() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::TrackType(TrackType::Segment(u32::MAX))]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf, &[0x01, // one metadata entry
                              0x00, // entry type: track_type = 0x00
                              0x05, // two byte entry size = 5
                              0x00,
                              0x02, // track type: segment = 0x02
                              0xFF, // four byte segment ID = 4,294,967,295
                              0xFF,
                              0xFF,
                              0xFF,
                              0xD5, // crc
                              0xCB]);
        });
    }

    #[test]
    fn test_only_created_at_epoch() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::CreatedAt(0)]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x01, // one metadata entry
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
                         0x28]);
        });
    }

    #[test]
    fn test_only_created_at_future() {
        let mut buf = vec![];
        let the_future = Duration::from_millis(u64::MAX).as_secs();
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::CreatedAt(the_future)]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x01, // one metadata entry
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
                         0x4C]);
        });
    }

    #[test]
    fn test_both() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::TrackType(TrackType::Trip(20)),
                                                   MetadataEntry::CreatedAt(0)]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf, &[0x02, // two metadata entries
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
                              0xD2]);
        });
    }

    #[test]
    fn test_duplicate_types() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::TrackType(TrackType::Trip(20)),
                                                   MetadataEntry::TrackType(TrackType::Trip(21)),
                                                   MetadataEntry::TrackType(TrackType::Route(22))]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x03, // three metadata entries
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
                         0x57]);
        });
    }
}
