use crate::error::Result;
use std::convert::TryFrom;
use std::io::Write;

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
        if let Some(v) = value {
            let value = *v;
            let delta = value - self.prev;
            self.prev = value;
            leb128::write::signed(buf, delta)?;
        }

        Ok(())
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
        if let Some(v) = value {
            let v = *(value.unwrap_or(&false)) as u8;
            buf.write_all(&v.to_le_bytes())?;
        }
        Ok(())
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
        if let Some(string) = value {
            // Write the length of the string
            leb128::write::unsigned(buf, u64::try_from(string.len()).unwrap())?;
            // Write the string itself
            buf.write_all(string.as_bytes())?;
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
    fn test_bool_encoder() {
        let mut data_buf = vec![];
        let mut presence_buf = vec![];
        let mut encoder = BoolEncoder;

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
        let mut encoder = StringEncoder;

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
