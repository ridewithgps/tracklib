use super::crc::CRC;
use crate::error::TracklibError;
use crate::types::{MetadataEntry, TrackType};
use nom::{multi::length_data, number::complete::le_u8, IResult};
use nom_leb128::leb128_u64;

fn parse_metadata_entry_track_type(input: &[u8]) -> IResult<&[u8], Option<MetadataEntry>, TracklibError> {
    let (input, _size) = leb128_u64(input)?;
    let (input, type_tag) = le_u8(input)?;
    let (input, id) = leb128_u64(input)?;

    match type_tag {
        0x00 => Ok((input, Some(MetadataEntry::TrackType(TrackType::Trip(id))))),
        0x01 => Ok((input, Some(MetadataEntry::TrackType(TrackType::Route(id))))),
        0x02 => Ok((input, Some(MetadataEntry::TrackType(TrackType::Segment(id))))),
        _ => Err(nom::Err::Error(TracklibError::ParseError {
            error_kind: nom::error::ErrorKind::Tag,
        })),
    }
}

fn parse_metadata_entry_created_at(input: &[u8]) -> IResult<&[u8], Option<MetadataEntry>, TracklibError> {
    let (input, _size) = leb128_u64(input)?;
    let (input, seconds_since_epoch) = leb128_u64(input)?;

    Ok((input, Some(MetadataEntry::CreatedAt(seconds_since_epoch))))
}

fn parse_metadata_entry_unknown(input: &[u8]) -> IResult<&[u8], Option<MetadataEntry>, TracklibError> {
    let (input, _data) = length_data(leb128_u64)(input)?;
    Ok((input, None))
}

fn parse_metadata_entry(input: &[u8]) -> IResult<&[u8], Option<MetadataEntry>, TracklibError> {
    let (input, type_tag) = le_u8(input)?;

    let (input, maybe_metadata_entry) = match type_tag {
        0x00 => parse_metadata_entry_track_type(input)?,
        0x01 => parse_metadata_entry_created_at(input)?,
        _ => parse_metadata_entry_unknown(input)?,
    };

    Ok((input, maybe_metadata_entry))
}

pub(crate) fn parse_metadata(input: &[u8]) -> IResult<&[u8], Vec<MetadataEntry>, TracklibError> {
    let input_start = input;
    let (mut input, entry_count) = le_u8(input)?;

    let mut entries = Vec::with_capacity(usize::from(entry_count));
    for _ in 0..entry_count {
        let (rest, maybe_entry) = parse_metadata_entry(input)?;
        input = rest;
        if let Some(entry) = maybe_entry {
            entries.push(entry);
        }
    }

    let (input, checksum) = CRC::<u16>::parser(input_start)(input)?;

    match checksum {
        CRC::Valid(_) => Ok((input, entries)),
        CRC::Invalid { expected, computed } => Err(nom::Err::Error(TracklibError::CRC16Error { expected, computed })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_empty_metadata() {
        #[rustfmt::skip]
        let buf = &[0x00, // zero metadata entries
                    0x40, // crc
                    0xBF];
        assert_matches!(parse_metadata(buf), Ok((&[], entries)) => {
            assert_eq!(entries, vec![]);
        });
    }

    #[test]
    fn test_metadata_both() {
        #[rustfmt::skip]
        let buf = &[0x02, // two metadata entries
                    0x00, // entry type: track_type
                    0x02, // entry size
                    0x00, // track type: trip
                    0x14, // trip id
                    0x01, // entry type: created_at
                    0x01, // entry size
                    0x00, // timestamp
                    0x6A, // crc
                    0x6F];
        assert_matches!(parse_metadata(buf), Ok((&[], entries)) => {
            assert_eq!(entries, vec![MetadataEntry::TrackType(TrackType::Trip(20)),
                                     MetadataEntry::CreatedAt(0)]);
        });
    }

    #[test]
    fn test_unknown_inbetween_known_entries() {
        #[rustfmt::skip]
        let buf = &[0x03, // two metadata entries
                    0x00, // entry type: track_type
                    0x02, // entry size
                    0x00, // track type: trip
                    0x14, // trip id
                    0xEF, // entry type: unknown!
                    0x14, // entry size
                    0x00, // 20 byte payload
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x01, // entry type: created_at
                    0x01, // entry size
                    0x00, // timestamp
                    0xCB, // crc
                    0xA1];
        assert_matches!(parse_metadata(buf), Ok((&[], entries)) => {
            assert_eq!(entries, vec![MetadataEntry::TrackType(TrackType::Trip(20)),
                                     MetadataEntry::CreatedAt(0)]);
        });
    }

    #[test]
    fn test_invalid_crc() {
        #[rustfmt::skip]
        let buf = &[0x02, // two metadata entries
                    0x00, // entry type: track_type
                    0x02, // entry size
                    0x00, // track type: trip
                    0x14, // trip id
                    0x01, // entry type: created_at
                    0x01, // entry size
                    0x00, // timestamp
                    0x12, // crc
                    0x34];
        assert_matches!(parse_metadata(buf), Err(nom::Err::Error(TracklibError::CRC16Error{expected,
                                                                                           computed})) => {
            assert_eq!(expected, u16::from_le_bytes([0x12, 0x34]));
            assert_eq!(computed, u16::from_le_bytes([0x6A, 0x6F]));
        });
    }
}
