use super::bitstream;
use crate::error::Result;

pub trait Encoder: Default {
    type T;
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
pub struct F64Encoder {
    prev: i64,
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
            value.map(|val| (*val * 10e6) as i64).as_ref(),
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
    type T = String;

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
    fn test_f64_encoder() {
        let mut data_buf = vec![];
        let mut presence_buf = vec![];
        let mut encoder = F64Encoder::default();

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
            .encode(Some(&"A".to_string()), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&"B".to_string()), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(Some(&"C".to_string()), &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(None, &mut data_buf, &mut presence_buf)
            .is_ok());
        assert!(encoder
            .encode(
                Some(&"Hello, World!".to_string()),
                &mut data_buf,
                &mut presence_buf
            )
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
}
