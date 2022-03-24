use super::crc::CRC;
use crate::consts::RWTFMAGIC;
use crate::error::TracklibError;
use nom::{
    bytes::complete::{tag, take},
    number::complete::{le_u16, le_u8},
    IResult,
};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct Header {
    file_version: u8,
    creator_version: u8,
    metadata_offset: u16,
    data_offset: u16,
}

impl Header {
    pub(crate) fn file_version(&self) -> u8 {
        self.file_version
    }

    pub(crate) fn creator_version(&self) -> u8 {
        self.creator_version
    }

    pub(crate) fn metadata_offset(&self) -> u16 {
        self.metadata_offset
    }

    pub(crate) fn data_offset(&self) -> u16 {
        self.data_offset
    }
}

pub(crate) fn parse_header(input: &[u8]) -> IResult<&[u8], Header, TracklibError> {
    let input_start = input;
    let (input, _magic) = tag(RWTFMAGIC)(input)?;
    let (input, file_version) = le_u8(input)?;
    let (input, _fv_reserve) = take(3_usize)(input)?;
    let (input, creator_version) = le_u8(input)?;
    let (input, _cv_reserve) = take(3_usize)(input)?;
    let (input, metadata_offset) = le_u16(input)?;
    let (input, data_offset) = le_u16(input)?;
    let (input, _e_reserve) = take(2_usize)(input)?;
    let (input, checksum) = CRC::<u16>::parser(input_start)(input)?;

    match checksum {
        CRC::Valid(_) => Ok((
            input,
            Header {
                file_version,
                creator_version,
                metadata_offset,
                data_offset,
            },
        )),
        CRC::Invalid { expected, computed } => Err(nom::Err::Error(TracklibError::CRC16Error {
            expected,
            computed,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_parse_header() {
        #[rustfmt::skip]
        let buf = &[0x89, // magic number
                    0x52,
                    0x57,
                    0x54,
                    0x46,
                    0x0A,
                    0x1A,
                    0x0A,
                    0x00, // file version
                    0x00, // fv reserve
                    0x00,
                    0x00,
                    0x00, // creator version
                    0x00, // cv reserve
                    0x00,
                    0x00,
                    0x0A, // metadata table offset
                    0x00,
                    0x1A, // data offset
                    0x00,
                    0x00, // e reserve
                    0x00,
                    0x86, // header crc
                    0xB7];
        assert_matches!(parse_header(buf), Ok((&[], Header{file_version,
                                                           creator_version,
                                                           metadata_offset,
                                                           data_offset})) => {
            assert_eq!(file_version, 0x00);
            assert_eq!(creator_version, 0x00);
            assert_eq!(metadata_offset, 0x0A);
            assert_eq!(data_offset, 0x1A);
        });
    }

    #[test]
    fn test_parse_header_with_invalid_crc() {
        #[rustfmt::skip]
        let buf = &[0x89, // magic number
                    0x52,
                    0x57,
                    0x54,
                    0x46,
                    0x0A,
                    0x1A,
                    0x0A,
                    0x00, // file version
                    0x00, // fv reserve
                    0x00,
                    0x00,
                    0x00, // creator version
                    0x00, // cv reserve
                    0x00,
                    0x00,
                    0x0A, // metadata table offset
                    0x00,
                    0x1A, // data offset
                    0x00,
                    0x00, // e reserve
                    0x00,
                    0x12, // invalid header crc
                    0x34];
        assert_matches!(parse_header(buf), Err(nom::Err::Error(TracklibError::CRC16Error{expected,
                                                                                         computed})) => {
            assert_eq!(expected, u16::from_le_bytes([0x12, 0x34]));
            assert_eq!(computed, u16::from_le_bytes([0x86, 0xB7]));
        });
    }
}
