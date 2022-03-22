use crate::error::Result;
use std::io::Write;

pub fn write_i64(value: Option<&i64>, buf: &mut Vec<u8>, prev: &mut i64) -> Result<()> {
    if let Some(val) = value {
        let v = *val;
        let delta = v - *prev;
        *prev = v;
        leb128::write::signed(buf, delta)?;
    }

    Ok(())
}

pub fn write_bool(value: Option<&bool>, buf: &mut Vec<u8>) -> Result<()> {
    if let Some(val) = value {
        let v = *val as u8;
        buf.write_all(&v.to_le_bytes())?;
    }
    Ok(())
}

pub fn write_bytes(value: Option<&[u8]>, buf: &mut Vec<u8>) -> Result<()> {
    if let Some(val) = value {
        // Write len
        leb128::write::unsigned(buf, u64::try_from(val.len()).expect("usize != u64"))?;
        // Write bytes
        buf.write_all(val)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_write_i64() {
        let mut buf = vec![];
        let mut prev = 0;
        assert_matches!(write_i64(Some(&0), &mut buf, &mut prev), Ok(()));
        assert_matches!(write_i64(None, &mut buf, &mut prev), Ok(()));
        assert_matches!(write_i64(Some(&42), &mut buf, &mut prev), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf, &[0x00,
                          0x2A]);
    }

    #[test]
    fn test_write_bool() {
        let mut buf = vec![];
        assert_matches!(write_bool(Some(&false), &mut buf), Ok(()));
        assert_matches!(write_bool(None, &mut buf), Ok(()));
        assert_matches!(write_bool(Some(&true), &mut buf), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf, &[0x00,
                          0x01]);
    }

    #[test]
    fn test_write_bytes() {
        let mut buf = vec![];
        assert_matches!(write_bytes(Some(&[b'R', b'W']), &mut buf), Ok(()));
        assert_matches!(write_bytes(None, &mut buf), Ok(()));
        assert_matches!(write_bytes(Some(&[b'G', b'P', b'S']), &mut buf), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf, &[0x02,
                          b'R',
                          b'W',
                          0x03,
                          b'G',
                          b'P',
                          b'S']);
    }
}
