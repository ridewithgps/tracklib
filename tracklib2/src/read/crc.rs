use crate::error::TracklibError;
use nom::{number::complete::le_u16, IResult, Offset};

pub(crate) trait CRCImpl: Sized {
    fn crc_bytes(bytes: &[u8]) -> Self;
    fn read_bytes(input: &[u8]) -> IResult<&[u8], Self, TracklibError>;
}

impl CRCImpl for u16 {
    fn crc_bytes(bytes: &[u8]) -> Self {
        crate::consts::CRC16.checksum(bytes)
    }

    fn read_bytes(input: &[u8]) -> IResult<&[u8], Self, TracklibError> {
        le_u16(input)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum CRC<T> {
    Valid(T),
    Invalid { expected: T, computed: T },
}

impl<T: PartialEq> CRC<T> {
    fn new(expected: T, computed: T) -> Self {
        if expected == computed {
            CRC::Valid(expected)
        } else {
            CRC::Invalid { expected, computed }
        }
    }
}

impl<T: CRCImpl + PartialEq> CRC<T> {
    pub(crate) fn parser<'a>(
        start: &'a [u8],
    ) -> impl Fn(&'a [u8]) -> IResult<&'a [u8], Self, TracklibError> {
        move |input: &[u8]| {
            let end = start.offset(input);
            let computed = CRCImpl::crc_bytes(&start[..end]);
            let (input, expected) = CRCImpl::read_bytes(input)?;
            Ok((input, Self::new(expected, computed)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    // #[test]
    // fn test_crc_full_thing() {
    //     let bytes = &[0x00, 0xBF, 0x40,];
    //     println!("crc of &[0x00]: {:X?}", u16::crc_bytes(&[0x00]).to_le_bytes());
    //     println!("crc of &{:X?}:  {:X?}", bytes, u16::crc_bytes(bytes).to_le_bytes());
    //     assert_eq!(u16::crc_bytes(bytes), u16::from_le_bytes([0xFF, 0xFF]));
    // }

    #[test]
    fn test_emtpy_crc() {
        let buf = &[0x00, 0x00];
        let result = CRC::<u16>::parser(buf)(buf);
        assert_matches!(result, Ok((&[], CRC::Valid(crc))) => {
            assert_eq!(crc, u16::from_le_bytes([0x00, 0x00]));
        });
    }

    #[test]
    fn test_simple_crc() {
        let buf = &[0x00, 0x40, 0xBF];
        let result = CRC::<u16>::parser(buf)(&buf[1..]);
        assert_matches!(result, Ok((&[], CRC::Valid(crc))) => {
            assert_eq!(crc, u16::from_le_bytes([0x40, 0xBF]));
        });
    }

    #[test]
    fn test_invalid_crc() {
        let buf = &[0x00, 0x12, 0x34];
        let result = CRC::<u16>::parser(buf)(&buf[1..]);
        assert_matches!(result, Ok((&[], CRC::Invalid {expected, computed})) => {
            assert_eq!(expected, u16::from_le_bytes([0x12, 0x34]));
            assert_eq!(computed, u16::from_le_bytes([0x40, 0xBF]));
        });
    }
}
