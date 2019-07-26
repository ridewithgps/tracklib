use nom::{Context, IResult, Err, ErrorKind, Needed, take};

pub const CONTINUATION_BIT: u8 = 1 << 7;
pub const SIGN_BIT: u8 = 1 << 6;

pub(crate) fn take_unsigned_leb128(i: &[u8]) -> IResult<&[u8], u64> {
    let mut result: u64 = 0;
    let mut remainder = i;
    let mut shift = 0;

    let mut byte;

    loop {
        match take!(remainder, 1) {
            Ok((rest, bytes)) => {
                remainder = rest;
                byte = bytes[0];

                if shift == 63 && byte != 0b0000_0000 && byte != 0b0000_0001 {
                    return Err(Err::Error(Context::Code(i, ErrorKind::Custom(0))))
                }

                let low_bits = (byte & !CONTINUATION_BIT) as u64;

                result |= low_bits << shift;
                shift += 7;

                if byte & CONTINUATION_BIT == 0 {
                    return Ok((remainder, result))
                }
            }
            Err(_) => return Err(Err::Incomplete(Needed::Unknown))
        }
    }
}

pub(crate) fn take_signed_leb128(i: &[u8]) -> IResult<&[u8], i64> {
    let mut result: i64 = 0;
    let size = 64;
    let mut remainder = i;
    let mut shift = 0;

    let mut byte;

    loop {
        match take!(remainder, 1) {
            Ok((rest, bytes)) => {
                remainder = rest;
                byte = bytes[0];

                if shift == 63 && byte != 0b0000_0000 && byte != 0b0111_1111 {
                    return Err(Err::Error(Context::Code(i, ErrorKind::Custom(0))))
                }

                let low_bits = (byte & !CONTINUATION_BIT) as i64;

                result |= low_bits << shift;
                shift += 7;

                if byte & CONTINUATION_BIT == 0 {
                    break;
                }
            }
            Err(_) => return Err(Err::Incomplete(Needed::Unknown))
        }
    }

    // sign extend the result
    if shift < size && (SIGN_BIT & byte) == SIGN_BIT {
        result |= !0 << shift;
    }

    return Ok((remainder, result));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed_helper(n: i64) {
        let mut buf = vec![];
        assert!(leb128::write::signed(&mut buf, n).is_ok());
        let r = take_signed_leb128(&buf);
        assert!(r.is_ok());
        let (rest, d) = r.unwrap();
        assert_eq!(0, rest.len());
        assert_eq!(n, d);
    }

    #[test]
    fn test_signed_roundtrips() {
        signed_helper(0);
        signed_helper(1);
        signed_helper(-1);
        signed_helper(50);
        signed_helper(-50);
        signed_helper(500);
        signed_helper(-500);
        signed_helper(std::i16::MIN as i64);
        signed_helper(std::i16::MAX as i64);
        signed_helper(std::i32::MIN as i64);
        signed_helper(std::i32::MAX as i64);
        signed_helper(std::i64::MIN as i64);
        signed_helper(std::i64::MAX as i64);
    }

    fn unsigned_helper(n: u64) {
        let mut buf = vec![];
        assert!(leb128::write::unsigned(&mut buf, n).is_ok());
        let r = take_unsigned_leb128(&buf);
        assert!(r.is_ok());
        let (rest, d) = r.unwrap();
        assert_eq!(0, rest.len());
        assert_eq!(n, d);
    }

    #[test]
    fn test_unsigned_roundtrips() {
        unsigned_helper(0);
        unsigned_helper(1);
        unsigned_helper(50);
        unsigned_helper(500);
        unsigned_helper(std::u16::MAX as u64);
        unsigned_helper(std::u32::MAX as u64);
        unsigned_helper(std::u64::MAX as u64);
    }
}
