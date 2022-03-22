use crate::error::{Result, TracklibError};
use nom::{multi::length_data, number::complete::le_u8};
use nom_leb128::{leb128_i64, leb128_u64};

fn helper<'a>(maybe_is_present: Option<bool>, data: &'a [u8]) -> Result<Option<&'a [u8]>> {
    match maybe_is_present {
        Some(true) => Ok(Some(data)),
        Some(false) => Ok(None),
        None => Err(TracklibError::ParseIncompleteError {
            needed: nom::Needed::Unknown,
        }),
    }
}

pub fn read_i64<'a>(
    maybe_is_present: Option<bool>,
    data: &'a [u8],
    prev: &mut i64,
) -> Result<(&'a [u8], Option<i64>)> {
    if let Some(data) = helper(maybe_is_present, data)? {
        let (rest, value) = leb128_i64(data)?;
        let new = *prev + value;
        *prev = new;
        Ok((rest, Some(new)))
    } else {
        Ok((data, None))
    }
}

pub fn read_bool<'a>(
    maybe_is_present: Option<bool>,
    data: &'a [u8],
) -> Result<(&'a [u8], Option<bool>)> {
    if let Some(data) = helper(maybe_is_present, data)? {
        let (rest, value) = le_u8(data)?;
        Ok((rest, Some(value != 0)))
    } else {
        Ok((data, None))
    }
}

pub fn read_bytes<'a>(
    maybe_is_present: Option<bool>,
    data: &'a [u8],
) -> Result<(&'a [u8], Option<&'a [u8]>)> {
    if let Some(data) = helper(maybe_is_present, data)? {
        let (rest, bytes) = length_data(leb128_u64)(data)?;
        Ok((rest, Some(bytes)))
    } else {
        Ok((data, None))
    }
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
        assert_matches!(read_i64(Some(true), buf, &mut prev), Ok((rest, Some(value))) => {
            assert_eq!(rest, &buf[1..]);
            assert_eq!(value, 4);
            buf = rest;
        });

        assert_matches!(read_i64(Some(false), buf, &mut prev), Ok((rest, None)) => {
            assert_eq!(rest, buf);
        });

        assert_matches!(read_i64(Some(true), buf, &mut prev), Ok((rest, Some(value))) => {
            assert_eq!(rest, &buf[1..]);
            assert_eq!(value, 9); // 4 + 5 = 9
        });
    }

    #[test]
    fn test_read_bool() {
        #[rustfmt::skip]
        let mut buf: &[u8] = &[0x00,
                               0x01];

        assert_matches!(read_bool(Some(true), buf), Ok((rest, Some(value))) => {
            assert_eq!(rest, &buf[1..]);
            assert_eq!(value, false);
            buf = rest;
        });

        assert_matches!(read_bool(Some(false), buf), Ok((rest, None)) => {
            assert_eq!(rest, buf);
        });

        assert_matches!(read_bool(Some(true), buf), Ok((rest, Some(value))) => {
            assert_eq!(rest, &buf[1..]);
            assert_eq!(value, true);
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

        assert_matches!(read_bytes(Some(true), buf), Ok((rest, Some(value))) => {
            assert_eq!(rest, &buf[3..]);
            assert_eq!(value, &[b'R', b'W']);
            buf = rest;
        });

        assert_matches!(read_bytes(Some(false), buf), Ok((rest, None)) => {
            assert_eq!(rest, buf);
        });

        assert_matches!(read_bytes(Some(true), buf), Ok((rest, Some(value))) => {
            assert_eq!(rest, &buf[4..]);
            assert_eq!(value, &[b'G', b'P', b'S']);
        });
    }

    #[test]
    fn test_read_none() {
        #[rustfmt::skip]
        let buf: &[u8] = &[0x00,
                           0x00];

        let mut prev = 0;
        assert_matches!(read_i64(None, buf, &mut prev), Err(_));
    }
}
