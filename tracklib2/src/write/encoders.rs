use super::bitstream;
use crate::error::Result;

pub trait Encoder {
    type T: ?Sized;
    fn encode(
        &mut self,
        value: Option<&Self::T>,
        buf: &mut Vec<u8>,
        presence: &mut Vec<bool>,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct I64Encoder {
    prev: i64,
}

impl Encoder for I64Encoder {
    type T = i64;

    fn encode(
        &mut self,
        value: Option<&Self::T>,
        buf: &mut Vec<u8>,
        presence: &mut Vec<bool>,
    ) -> Result<()> {
        presence.push(value.is_some());
        bitstream::write_i64(value, buf, &mut self.prev)
    }
}

#[derive(Debug, Default)]
pub struct U64Encoder {
    prev: i64,
}

impl Encoder for U64Encoder {
    type T = u64;

    fn encode(
        &mut self,
        value: Option<&Self::T>,
        buf: &mut Vec<u8>,
        presence: &mut Vec<bool>,
    ) -> Result<()> {
        presence.push(value.is_some());
        bitstream::write_i64(value.map(|val| *val as i64).as_ref(), buf, &mut self.prev)
    }
}

#[derive(Debug)]
pub struct F64Encoder {
    prev: i64,
    factor: f64,
}

impl F64Encoder {
    pub fn new(scale: u8) -> Self {
        Self {
            prev: 0,
            factor: 10_f64.powi(i32::from(scale)),
        }
    }
}

impl Encoder for F64Encoder {
    type T = f64;

    fn encode(
        &mut self,
        value: Option<&Self::T>,
        buf: &mut Vec<u8>,
        presence: &mut Vec<bool>,
    ) -> Result<()> {
        presence.push(value.is_some());
        bitstream::write_i64(
            value.map(|val| (*val * self.factor) as i64).as_ref(),
            buf,
            &mut self.prev,
        )
    }
}

#[derive(Debug, Default)]
pub struct BoolEncoder;

impl Encoder for BoolEncoder {
    type T = bool;

    fn encode(
        &mut self,
        value: Option<&Self::T>,
        buf: &mut Vec<u8>,
        presence: &mut Vec<bool>,
    ) -> Result<()> {
        presence.push(value.is_some());
        bitstream::write_bool(value, buf)
    }
}

#[derive(Debug, Default)]
pub struct StringEncoder;

impl Encoder for StringEncoder {
    type T = str;

    fn encode(
        &mut self,
        value: Option<&Self::T>,
        buf: &mut Vec<u8>,
        presence: &mut Vec<bool>,
    ) -> Result<()> {
        presence.push(value.is_some());
        bitstream::write_bytes(value.map(|v| v.as_bytes()), buf)
    }
}

#[derive(Debug, Default)]
pub struct BoolArrayEncoder;

impl Encoder for BoolArrayEncoder {
    type T = [bool];

    fn encode(
        &mut self,
        value: Option<&Self::T>,
        buf: &mut Vec<u8>,
        presence: &mut Vec<bool>,
    ) -> Result<()> {
        presence.push(value.is_some());
        if let Some(array) = value {
            leb128::write::unsigned(buf, u64::try_from(array.len()).expect("usize != u64"))?;
            for b in array {
                bitstream::write_bool(Some(b), buf)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i64_encoder() {
        let mut data_buf = vec![];
        let mut presence_buf = vec![];
        let mut encoder = I64Encoder::default();

        assert!(encoder
            .encode(Some(&1), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&2), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&3), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&-100), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&-100), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&100), &mut data_buf, &mut presence_buf)
            .is_ok());

        #[rustfmt::skip]
        assert_eq!(data_buf, &[0x01, // +1 from 0
                               0x01, // +1 from 1
                               0x01, // +1 from 2
                               0x99, // -103 from 3
                               0x7F,
                               // None
                               // None
                               0x00, // staying at -100
                               0xC8, // +200 from -100
                               0x01]);

        #[rustfmt::skip]
        assert_eq!(presence_buf, &[true,
                                   true,
                                   true,
                                   true,
                                   false,
                                   false,
                                   true,
                                   true]);
    }

    #[test]
    fn test_u64_encoder() {
        let mut data_buf = vec![];
        let mut presence_buf = vec![];
        let mut encoder = U64Encoder::default();

        assert!(encoder
            .encode(Some(&1), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&2), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&100), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&u64::MAX), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&7), &mut data_buf, &mut presence_buf)
            .is_ok());

        #[rustfmt::skip]
        assert_eq!(data_buf, &[0x01,
                               0x01,
                               0xE2,
                               0x00,
                               0x9B,
                               0x7F,
                               0x08]);

        #[rustfmt::skip]
        assert_eq!(presence_buf, &[true,
                                   true,
                                   false,
                                   true,
                                   true,
                                   true]);
    }

    #[test]
    fn test_f64_encoder() {
        let mut data_buf = vec![];
        let mut presence_buf = vec![];
        let mut encoder = F64Encoder::new(7);

        assert!(encoder
            .encode(Some(&0.0), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&1.0), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&2.5), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&3.00001), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&-100.26), &mut data_buf, &mut presence_buf)
            .is_ok());

        #[rustfmt::skip]
        assert_eq!(data_buf, &[0x00, // first storing a 0

                               0x80, // leb128-encoded difference between prev (0.0) and 1.0 * 10e6
                               0xAD,
                               0xE2,
                               0x04,

                               // None

                               0xC0, // leb128-encoded delta between prev and 2.5 * 10e6
                               0xC3,
                               0x93,
                               0x07,

                               0xA4, // leb128-encoded delta between prev and 3.00001 * 10e6
                               0x97,
                               0xB1,
                               0x02,

                               0xDC, // leb128-encoded delta between prev and -100.26 * 10e6
                               0x8B,
                               0xCF,
                               0x93,
                               0x7C]);

        #[rustfmt::skip]
        assert_eq!(presence_buf, &[true,
                                   true,
                                   false,
                                   true,
                                   true,
                                   true]);
    }

    #[test]
    fn test_f64_encoder_smaller_scale_factor() {
        let mut data_buf = vec![];
        let mut presence_buf = vec![];
        let mut encoder = F64Encoder::new(2);

        assert!(encoder
            .encode(Some(&0.0), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&1.0), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&-20.0), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&-20.1234567), &mut data_buf, &mut presence_buf)
            .is_ok());

        #[rustfmt::skip]
        assert_eq!(data_buf, &[0x00,
                               0xE4,
                               0x00,
                               0xCC,
                               0x6F,
                               0x74,
        ]);

        #[rustfmt::skip]
        assert_eq!(presence_buf, &[true,
                                   true,
                                   true,
                                   true]);
    }

    #[test]
    fn test_bool_encoder() {
        let mut data_buf = vec![];
        let mut presence_buf = vec![];
        let mut encoder = BoolEncoder::default();

        assert!(encoder
            .encode(Some(&true), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&true), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&false), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&false), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&true), &mut data_buf, &mut presence_buf)
            .is_ok());

        #[rustfmt::skip]
        assert_eq!(data_buf, &[0x01,   // true
                               0x01,   // true
                               0x00,   // false
                               // None
                               // None
                               0x00,   // false
                               0x01]); // true

        #[rustfmt::skip]
        assert_eq!(presence_buf, &[true,
                                   true,
                                   true,
                                   false,
                                   false,
                                   true,
                                   true]);
    }

    #[test]
    fn test_string_encoder() {
        let mut data_buf = vec![];
        let mut presence_buf = vec![];
        let mut encoder = StringEncoder::default();

        assert!(encoder
            .encode(Some("A"), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some("B"), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some("C"), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some("Hello, World!"), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());

        #[rustfmt::skip]
        assert_eq!(data_buf, &[0x01, // length
                               b'A', // A
                               // None
                               0x01, // length
                               b'B', // B
                               // None
                               0x01, // length
                               b'C', // C
                               // None
                               0x0D, // length
                               b'H',
                               b'e',
                               b'l',
                               b'l',
                               b'o',
                               b',',
                               b' ',
                               b'W',
                               b'o',
                               b'r',
                               b'l',
                               b'd',
                               b'!'
                               // None
        ]);

        #[rustfmt::skip]
        assert_eq!(presence_buf, &[true,
                                   false,
                                   true,
                                   false,
                                   true,
                                   false,
                                   true,
                                   false]);
    }

    #[test]
    fn test_bool_array_encoder() {
        let mut data_buf = vec![];
        let mut presence_buf = vec![];
        let mut encoder = BoolArrayEncoder::default();

        assert!(encoder
            .encode(
                Some(&[true, false, false]),
                &mut data_buf,
                &mut presence_buf
            )
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(
                Some(&[false, false, false, false, true, true]),
                &mut data_buf,
                &mut presence_buf
            )
            .is_ok());
        assert!(encoder
            .encode(Some(&[true]), &mut data_buf, &mut presence_buf)
            .is_ok());

        #[rustfmt::skip]
        assert_eq!(data_buf, &[0x03, // array len three
                               0x01, // true
                               0x00, // false
                               0x00, // false
                               0x06, // array len six
                               0x00, // false
                               0x00, // false
                               0x00, // false
                               0x00, // false
                               0x01, // true
                               0x01, // true
                               0x01, // array len one
                               0x01]); // true

        #[rustfmt::skip]
        assert_eq!(presence_buf, &[true,
                                   false,
                                   true,
                                   true]);
    }
}
