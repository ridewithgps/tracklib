use crate::error::Result;
use std::io::Write;

pub fn write_i64<W>(value: Option<&i64>, buf: &mut W, prev: &mut i64) -> Result<()>
where
    W: ?Sized + Write,
{
    if let Some(val) = value {
        let v = *val;
        let delta = v.wrapping_sub(*prev);
        leb128::write::signed(buf, delta)?;
        *prev = v;
    }

    Ok(())
}

pub fn write_byte<W>(value: Option<&u8>, buf: &mut W) -> Result<()>
where
    W: ?Sized + Write,
{
    if let Some(val) = value {
        buf.write_all(std::slice::from_ref(val))?;
    }
    Ok(())
}

pub fn write_bytes<W>(value: &[u8], buf: &mut W) -> Result<()>
where
    W: ?Sized + Write,
{
    // Write len
    leb128::write::unsigned(buf, u64::try_from(value.len()).expect("usize != u64"))?;
    // Write bytes
    buf.write_all(value)?;

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
    fn test_write_byte() {
        let mut buf = vec![];
        assert_matches!(write_byte(Some(&0), &mut buf), Ok(()));
        assert_matches!(write_byte(None, &mut buf), Ok(()));
        assert_matches!(write_byte(Some(&1), &mut buf), Ok(()));
        assert_matches!(write_byte(Some(&255), &mut buf), Ok(()));
        #[rustfmt::skip]
        assert_eq!(buf, &[0x00,
                          0x01,
                          0xFF]);
    }

    #[test]
    fn test_write_bytes() {
        let mut buf = vec![];
        assert_matches!(write_bytes(&[b'R', b'W'], &mut buf), Ok(()));
        assert_matches!(write_bytes(&[b'G', b'P', b'S'], &mut buf), Ok(()));
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
