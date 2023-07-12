use super::crcwriter::CrcWriter;
use crate::error::Result;
use crate::types::{MetadataEntry, TrackType};
use std::io::Write;

impl MetadataEntry {
    #[rustfmt::skip]
    fn write<W: Write>(&self, out: &mut W) -> Result<()> {
        let (entry_type, entry_buf) = match self {
            Self::TrackType(track_type) => {
                let mut entry_contents_buf = vec![];
                let (type_tag, id): (u8, &u64) = match track_type {
                    TrackType::Trip(id) => (0x00, id),
                    TrackType::Route(id) => (0x01, id),
                    TrackType::Segment(id) => (0x02, id),
                };
                entry_contents_buf.write_all(&type_tag.to_le_bytes())?;
                leb128::write::unsigned(&mut entry_contents_buf, *id)?;
                (0x00, entry_contents_buf)
            }
            Self::CreatedAt(seconds_since_epoch) => {
                let mut entry_contents_buf = vec![];
                leb128::write::unsigned(&mut entry_contents_buf, *seconds_since_epoch)?;
                (0x01, entry_contents_buf)
            }
        };

        out.write_all(&[entry_type])?;                                                             // 1 byte  - entry type
        leb128::write::unsigned(out, u64::try_from(entry_buf.len()).expect("usize != u64"))?;      // ? bytes - entry size
        out.write_all(&entry_buf)?;                                                                // ? bytes - entry data

        Ok(())
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
                         0x00, // entry type: track_type
                         0x03, // entry size
                         0x00, // track type: trip
                         0x90, // trip id
                         0x03,
                         0xD2, // crc
                         0x70]);
        });
    }

    #[test]
    fn test_only_track_type_route() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::TrackType(TrackType::Route(64))]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x01, // one metadata entry
                         0x00, // entry type: track_type
                         0x02, // entry size
                         0x01, // track type: route
                         0x40, // route id
                         0x47, // crc
                         0x9F]);
        });
    }

    #[test]
    fn test_only_track_type_segment() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::TrackType(TrackType::Segment(u64::MAX))]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf, &[0x01, // one metadata entry
                              0x00, // entry type: track_type
                              0x0B, // entry size
                              0x02, // track type: segment
                              0xFF, // segment id
                              0xFF,
                              0xFF,
                              0xFF,
                              0xFF,
                              0xFF,
                              0xFF,
                              0xFF,
                              0xFF,
                              0x01,
                              0x0A, // crc
                              0x5F]);
        });
    }

    #[test]
    fn test_only_created_at_epoch() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::CreatedAt(0)]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x01, // one metadata entry
                         0x01, // entry type: created_at
                         0x01, // entry size
                         0x00, // timestamp
                         0xAE, // crc
                         0x77]);
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
                         0x01, // entry type: created_at
                         0x08, // entry size
                         0xEF, // timestamp
                         0xCF,
                         0x9A,
                         0xDE,
                         0xF4,
                         0xA6,
                         0xE2,
                         0x20,
                         0x94, // crc
                         0x64]);
        });
    }

    #[test]
    fn test_both() {
        let mut buf = vec![];
        assert_matches!(write_metadata(&mut buf, &[MetadataEntry::TrackType(TrackType::Trip(20)),
                                                   MetadataEntry::CreatedAt(0)]), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf, &[0x02, // two metadata entries
                              0x00, // entry type: track_type
                              0x02, // entry size
                              0x00, // track type: trip
                              0x14, // trip id
                              0x01, // entry type: created_at
                              0x01, // entry size
                              0x00, // timestamp
                              0x6A, // crc
                              0x6F]);
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
                         0x00, // entry type: track_type
                         0x02, // entry size
                         0x00, // track type: trip
                         0x14, // four byte trip ID = 20
                         0x00, // entry type: track_type
                         0x02, // entry size
                         0x00, // track type: trip
                         0x15, // trip id
                         0x00, // entry type: track_type
                         0x02, // entry size
                         0x01, // track type: route
                         0x16, // route id
                         0x02, // crc
                         0xF2]);
        });
    }
}
