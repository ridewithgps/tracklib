use super::crcwriter::CrcWriter;
use crate::error::{Result, TracklibError};
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

#[derive(Default)]
pub(crate) struct MetadataWriter {
    buf: Vec<u8>,
    count: u8,
}

impl MetadataWriter {
    pub(crate) fn write_entry(&mut self, entry: &MetadataEntry) -> Result<()> {
        entry.write(&mut self.buf)?;
        self.count = self.count.checked_add(1).ok_or(TracklibError::TooManyEntriesError)?;
        Ok(())
    }

    #[rustfmt::skip]
    pub(crate) fn finish<W: Write>(self, out: &mut W) -> Result<()> {
        let mut crcwriter = CrcWriter::new16(out);

        crcwriter.write_all(&self.count.to_le_bytes())?; // 1 byte  - entry count
        crcwriter.write_all(&self.buf)?;                 // ? bytes - all entries contents
        crcwriter.append_crc()?;                         // 2 bytes - crc

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::time::Duration;

    #[test]
    fn test_write_empty_metadata() {
        let mut buf = vec![];
        let w = MetadataWriter::default();
        assert_matches!(w.finish(&mut buf), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf,
                   &[0x00, // zero metadata entries
                     0x40, // crc
                     0xBF]);
    }

    #[test]
    fn test_only_track_type_trip() {
        let mut buf = vec![];
        let mut w = MetadataWriter::default();
        assert_matches!(w.write_entry(&MetadataEntry::TrackType(TrackType::Trip(400))), Ok(()));
        assert_matches!(w.finish(&mut buf), Ok(()));
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
    }

    #[test]
    fn test_only_track_type_route() {
        let mut buf = vec![];
        let mut w = MetadataWriter::default();
        assert_matches!(w.write_entry(&MetadataEntry::TrackType(TrackType::Route(64))), Ok(()));
        assert_matches!(w.finish(&mut buf), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf,
                   &[0x01, // one metadata entry
                     0x00, // entry type: track_type
                     0x02, // entry size
                     0x01, // track type: route
                     0x40, // route id
                     0x47, // crc
                     0x9F]);
    }

    #[test]
    fn test_only_track_type_segment() {
        let mut buf = vec![];
        let mut w = MetadataWriter::default();
        assert_matches!(
            w.write_entry(&MetadataEntry::TrackType(TrackType::Segment(u64::MAX))),
            Ok(())
        );
        assert_matches!(w.finish(&mut buf), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf,
                   &[0x01, // one metadata entry
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
    }

    #[test]
    fn test_only_created_at_epoch() {
        let mut buf = vec![];
        let mut w = MetadataWriter::default();
        assert_matches!(w.write_entry(&MetadataEntry::CreatedAt(0)), Ok(()));
        assert_matches!(w.finish(&mut buf), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf,
                   &[0x01, // one metadata entry
                     0x01, // entry type: created_at
                     0x01, // entry size
                     0x00, // timestamp
                     0xAE, // crc
                     0x77]);
    }

    #[test]
    fn test_only_created_at_future() {
        let mut buf = vec![];
        let the_future = Duration::from_millis(u64::MAX).as_secs();
        let mut w = MetadataWriter::default();
        assert_matches!(w.write_entry(&MetadataEntry::CreatedAt(the_future)), Ok(()));
        assert_matches!(w.finish(&mut buf), Ok(()));
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
    }

    #[test]
    fn test_both() {
        let mut buf = vec![];
        let mut w = MetadataWriter::default();
        assert_matches!(w.write_entry(&MetadataEntry::TrackType(TrackType::Trip(20))), Ok(()));
        assert_matches!(w.write_entry(&MetadataEntry::CreatedAt(0)), Ok(()));
        assert_matches!(w.finish(&mut buf), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf,
                   &[0x02, // two metadata entries
                     0x00, // entry type: track_type
                     0x02, // entry size
                     0x00, // track type: trip
                     0x14, // trip id
                     0x01, // entry type: created_at
                     0x01, // entry size
                     0x00, // timestamp
                     0x6A, // crc
                     0x6F]);
    }

    #[test]
    fn test_duplicate_types() {
        let mut buf = vec![];
        let mut w = MetadataWriter::default();
        assert_matches!(w.write_entry(&MetadataEntry::TrackType(TrackType::Trip(20))), Ok(()));
        assert_matches!(w.write_entry(&MetadataEntry::TrackType(TrackType::Trip(21))), Ok(()));
        assert_matches!(w.write_entry(&MetadataEntry::TrackType(TrackType::Route(22))), Ok(()));
        assert_matches!(w.finish(&mut buf), Ok(()));
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
    }

    #[test]
    fn test_too_many_entries() {
        let mut w = MetadataWriter::default();
        for _ in 0..255 {
            assert_matches!(w.write_entry(&MetadataEntry::CreatedAt(0)), Ok(()));
        }
        assert_matches!(
            w.write_entry(&MetadataEntry::CreatedAt(0)),
            Err(TracklibError::TooManyEntriesError)
        );
    }
}
