use crate::error::Result;
use nom::{multi::length_data, number::complete::le_u8};
use nom_leb128::{leb128_i64, leb128_u64};

pub fn read_i64<'a>(data: &'a [u8], prev: &mut i64) -> Result<(&'a [u8], i64)> {
    let (rest, value) = leb128_i64(data)?;
    let new = prev.wrapping_add(value);
    *prev = new;
    Ok((rest, new))
}

pub fn read_byte(data: &[u8]) -> Result<(&[u8], u8)> {
    let (rest, value) = le_u8(data)?;
    Ok((rest, value))
}

pub fn read_bytes(data: &[u8]) -> Result<(&[u8], &[u8])> {
    let (rest, bytes) = length_data(leb128_u64)(data)?;
    Ok((rest, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_read_i64() {
        #[rustfmt::skip]
        let mut buf: &[u8] = &[0x04,
                               0x05];

        let mut prev = 0;
        assert_matches!(read_i64(buf, &mut prev), Ok((rest, value)) => {
            assert_eq!(rest, &buf[1..]);
            assert_eq!(value, 4);
            buf = rest;
        });

        assert_matches!(read_i64(buf, &mut prev), Ok((rest, value)) => {
            assert_eq!(rest, &buf[1..]);
            assert_eq!(value, 9); // 4 + 5 = 9
        });
    }

    #[test]
    fn test_read_byte() {
        #[rustfmt::skip]
        let mut buf: &[u8] = &[0x00,
                               0x01,
                               0xFF];

        assert_matches!(read_byte(buf), Ok((rest, value)) => {
            assert_eq!(rest, &buf[1..]);
            assert_eq!(value, 0);
            buf = rest;
        });

        assert_matches!(read_byte(buf), Ok((rest, value)) => {
            assert_eq!(rest, &buf[1..]);
            assert_eq!(value, 1);
            buf = rest;
        });

        assert_matches!(read_byte(buf), Ok((rest, value)) => {
            assert_eq!(rest, &buf[1..]);
            assert_eq!(value, 255);
        });
    }

    #[test]
    fn test_read_bytes() {
        #[rustfmt::skip]
        let mut buf: &[u8] = &[0x02,
                               b'R',
                               b'W',
                               0x03,
                               b'G',
                               b'P',
                               b'S'];

        assert_matches!(read_bytes(buf), Ok((rest, value)) => {
            assert_eq!(rest, &buf[3..]);
            assert_eq!(value, &[b'R', b'W']);
            buf = rest;
        });

        assert_matches!(read_bytes(buf), Ok((rest, value)) => {
            assert_eq!(rest, &buf[4..]);
            assert_eq!(value, &[b'G', b'P', b'S']);
        });
    }
}
