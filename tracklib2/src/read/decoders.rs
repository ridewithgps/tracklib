use super::bitstream;
use super::crc::CRC;
use super::presence_column::PresenceColumnView;
use crate::error::Result;
use crate::error::TracklibError;
use nom_leb128::leb128_u64;

pub(crate) trait Decoder {
    type T;
    fn decode(&mut self) -> Result<Option<Self::T>>;
}

fn validate_column(data: &[u8]) -> Result<&[u8]> {
    const CRC_BYTES: usize = 4;
    let (column_data, crc_bytes) = data.split_at(data.len() - CRC_BYTES);
    let (_, checksum) = CRC::<u32>::parser(column_data)(crc_bytes)?;

    match checksum {
        CRC::Valid(_) => Ok(column_data),
        CRC::Invalid { expected, computed } => {
            Err(TracklibError::CRC32Error { expected, computed })
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) struct I64Decoder<'a> {
    data: &'a [u8],
    presence_column_view: PresenceColumnView<'a>,
    prev: i64,
}

impl<'a> I64Decoder<'a> {
    pub(crate) fn new(
        data: &'a [u8],
        presence_column_view: PresenceColumnView<'a>,
    ) -> Result<Self> {
        let column_data = validate_column(data)?;

        Ok(Self {
            data: column_data,
            presence_column_view,
            prev: 0,
        })
    }
}

impl<'a> Decoder for I64Decoder<'a> {
    type T = i64;

    fn decode(&mut self) -> Result<Option<Self::T>> {
        match self.presence_column_view.next() {
            Some(true) => {
                let (rest, value) = bitstream::read_i64(self.data, &mut self.prev)?;
                self.data = rest;
                Ok(Some(value))
            }
            Some(false) => Ok(None),
            None => Err(TracklibError::ParseIncompleteError {
                needed: nom::Needed::Unknown,
            }),
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) struct U64Decoder<'a> {
    data: &'a [u8],
    presence_column_view: PresenceColumnView<'a>,
    prev: i64,
}

impl<'a> U64Decoder<'a> {
    pub(crate) fn new(
        data: &'a [u8],
        presence_column_view: PresenceColumnView<'a>,
    ) -> Result<Self> {
        let column_data = validate_column(data)?;

        Ok(Self {
            data: column_data,
            presence_column_view,
            prev: 0,
        })
    }
}

impl<'a> Decoder for U64Decoder<'a> {
    type T = u64;

    fn decode(&mut self) -> Result<Option<Self::T>> {
        match self.presence_column_view.next() {
            Some(true) => {
                let (rest, value) = bitstream::read_i64(self.data, &mut self.prev)?;
                self.data = rest;
                Ok(Some(value as u64))
            }
            Some(false) => Ok(None),
            None => Err(TracklibError::ParseIncompleteError {
                needed: nom::Needed::Unknown,
            }),
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) struct F64Decoder<'a> {
    data: &'a [u8],
    presence_column_view: PresenceColumnView<'a>,
    prev: i64,
    factor: f64,
}

impl<'a> F64Decoder<'a> {
    pub(crate) fn new(
        data: &'a [u8],
        presence_column_view: PresenceColumnView<'a>,
        scale: u8,
    ) -> Result<Self> {
        let column_data = validate_column(data)?;

        Ok(Self {
            data: column_data,
            presence_column_view,
            prev: 0,
            factor: 10_f64.powi(i32::from(scale)),
        })
    }
}

impl<'a> Decoder for F64Decoder<'a> {
    type T = f64;

    fn decode(&mut self) -> Result<Option<Self::T>> {
        match self.presence_column_view.next() {
            Some(true) => {
                let (rest, value) = bitstream::read_i64(self.data, &mut self.prev)?;
                self.data = rest;
                Ok(Some((value as f64) / self.factor))
            }
            Some(false) => Ok(None),
            None => Err(TracklibError::ParseIncompleteError {
                needed: nom::Needed::Unknown,
            }),
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) struct BoolDecoder<'a> {
    data: &'a [u8],
    presence_column_view: PresenceColumnView<'a>,
}

impl<'a> BoolDecoder<'a> {
    pub(crate) fn new(
        data: &'a [u8],
        presence_column_view: PresenceColumnView<'a>,
    ) -> Result<Self> {
        let column_data = validate_column(data)?;

        Ok(Self {
            data: column_data,
            presence_column_view,
        })
    }
}

impl<'a> Decoder for BoolDecoder<'a> {
    type T = bool;

    fn decode(&mut self) -> Result<Option<Self::T>> {
        match self.presence_column_view.next() {
            Some(true) => {
                let (rest, value) = bitstream::read_bool(self.data)?;
                self.data = rest;
                Ok(Some(value))
            }
            Some(false) => Ok(None),
            None => Err(TracklibError::ParseIncompleteError {
                needed: nom::Needed::Unknown,
            }),
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) struct StringDecoder<'a> {
    data: &'a [u8],
    presence_column_view: PresenceColumnView<'a>,
}

impl<'a> StringDecoder<'a> {
    pub(crate) fn new(
        data: &'a [u8],
        presence_column_view: PresenceColumnView<'a>,
    ) -> Result<Self> {
        let column_data = validate_column(data)?;

        Ok(Self {
            data: column_data,
            presence_column_view,
        })
    }
}

impl<'a> Decoder for StringDecoder<'a> {
    type T = String;

    fn decode(&mut self) -> Result<Option<Self::T>> {
        match self.presence_column_view.next() {
            Some(true) => {
                let (rest, value) = bitstream::read_bytes(self.data)?;
                self.data = rest;
                Ok(Some(String::from_utf8_lossy(value).into_owned()))
            }
            Some(false) => Ok(None),
            None => Err(TracklibError::ParseIncompleteError {
                needed: nom::Needed::Unknown,
            }),
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) struct BoolArrayDecoder<'a> {
    data: &'a [u8],
    presence_column_view: PresenceColumnView<'a>,
}

impl<'a> BoolArrayDecoder<'a> {
    pub(crate) fn new(
        data: &'a [u8],
        presence_column_view: PresenceColumnView<'a>,
    ) -> Result<Self> {
        let column_data = validate_column(data)?;

        Ok(Self {
            data: column_data,
            presence_column_view,
        })
    }
}

impl<'a> Decoder for BoolArrayDecoder<'a> {
    type T = Vec<bool>;

    fn decode(&mut self) -> Result<Option<Self::T>> {
        match self.presence_column_view.next() {
            Some(true) => {
                let (mut data, array_len) = leb128_u64(self.data)?;
                let mut array =
                    Vec::with_capacity(usize::try_from(array_len).expect("usize != u64"));
                for _ in 0..array_len {
                    let (rest, value) = bitstream::read_bool(data)?;
                    data = rest;
                    array.push(value);
                }
                self.data = data;

                Ok(Some(array))
            }
            Some(false) => Ok(None),
            None => Err(TracklibError::ParseIncompleteError {
                needed: nom::Needed::Unknown,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::presence_column::parse_presence_column;
    use assert_matches::assert_matches;
    use float_cmp::assert_approx_eq;

    #[test]
    fn test_decode_i64() {
        #[rustfmt::skip]
        let presence_buf = &[0b00000001,
                             0b00000001,
                             0b00000000,
                             0b00000001,
                             0b00000001,
                             0x32, // crc
                             0x65,
                             0x57,
                             0xFB];
        let presence_column =
            assert_matches!(parse_presence_column(1, 5)(presence_buf), Ok((&[], pc)) => pc);
        let presence_column_view = assert_matches!(presence_column.view(0), Some(v) => v);
        #[rustfmt::skip]
        let buf = &[0x20,
                    0x42,
                    0x00,
                    0x1,
                    0x0C, // crc
                    0x01,
                    0x49,
                    0xA3];
        assert_matches!(I64Decoder::new(buf, presence_column_view), Ok(mut decoder) => {
            assert_matches!(decoder.decode(), Ok(Some(32)));
            assert_matches!(decoder.decode(), Ok(Some(-30)));
            assert_matches!(decoder.decode(), Ok(None));
            assert_matches!(decoder.decode(), Ok(Some(-30)));
            assert_matches!(decoder.decode(), Ok(Some(-29)));
        });
    }

    #[test]
    fn test_decode_u64() {
        #[rustfmt::skip]
        let presence_buf = &[0b00000001,
                             0b00000001,
                             0b00000000,
                             0b00000001,
                             0b00000001,
                             0b00000001,
                             0x94, // crc
                             0x59,
                             0xA0,
                             0x40];
        let presence_column =
            assert_matches!(parse_presence_column(1, 6)(presence_buf), Ok((&[], pc)) => pc);
        let presence_column_view = assert_matches!(presence_column.view(0), Some(v) => v);
        #[rustfmt::skip]
        let buf = &[1, 1, 226, 0, 155, 127, 8,
                    0x2F, 0x93, 0xEE, 0xA7,
        ];
        assert_matches!(U64Decoder::new(buf, presence_column_view), Ok(mut decoder) => {
            assert_matches!(decoder.decode(), Ok(Some(1)));
            assert_matches!(decoder.decode(), Ok(Some(2)));
            assert_matches!(decoder.decode(), Ok(None));
            assert_matches!(decoder.decode(), Ok(Some(100)));
            assert_matches!(decoder.decode(), Ok(Some(u64::MAX)));
            assert_matches!(decoder.decode(), Ok(Some(7)));
        });
    }

    #[test]
    fn test_decode_f64() {
        #[rustfmt::skip]
        let presence_buf = &[0b00000001,
                             0b00000001,
                             0b00000000,
                             0b00000001,
                             0b00000001,
                             0b00000001,
                             0x94, // crc
                             0x59,
                             0xA0,
                             0x40];
        let presence_column =
            assert_matches!(parse_presence_column(1, 6)(presence_buf), Ok((&[], pc)) => pc);
        let presence_column_view = assert_matches!(presence_column.view(0), Some(v) => v);
        #[rustfmt::skip]
        let buf = &[0x00, // first storing a 0

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
                    0x7C,

                    0x52, // crc
                    0xD3,
                    0xE9,
                    0x35];
        assert_matches!(F64Decoder::new(buf, presence_column_view, 7), Ok(mut decoder) => {
            assert_matches!(decoder.decode(), Ok(Some(v)) => {
                assert_approx_eq!(f64, v, 0.0);
            });
            assert_matches!(decoder.decode(), Ok(Some(v)) => {
                assert_approx_eq!(f64, v, 1.0);
            });
            assert_matches!(decoder.decode(), Ok(None));
            assert_matches!(decoder.decode(), Ok(Some(v)) => {
                assert_approx_eq!(f64, v, 2.5);
            });
            assert_matches!(decoder.decode(), Ok(Some(v)) => {
                assert_approx_eq!(f64, v, 3.00001);
            });
            assert_matches!(decoder.decode(), Ok(Some(v)) => {
                assert_approx_eq!(f64, v, -100.26);
            });
        });
    }

    #[test]
    fn test_decode_bool() {
        #[rustfmt::skip]
        let presence_buf = &[0b00000000,
                             0b00000001,
                             0b00000001,
                             0x94, // crc
                             0x5E,
                             0x43,
                             0x9E];
        let presence_column =
            assert_matches!(parse_presence_column(1, 3)(presence_buf), Ok((&[], pc)) => pc);
        let presence_column_view = assert_matches!(presence_column.view(0), Some(v) => v);
        #[rustfmt::skip]
        let buf = &[0x01,
                    0x00,
                    0x5E, // crc
                    0x5A,
                    0x51,
                    0x2D];
        assert_matches!(BoolDecoder::new(buf, presence_column_view), Ok(mut decoder) => {
            assert_matches!(decoder.decode(), Ok(None));
            assert_matches!(decoder.decode(), Ok(Some(true)));
            assert_matches!(decoder.decode(), Ok(Some(false)));
        });
    }

    #[test]
    fn test_decode_string() {
        #[rustfmt::skip]
        let presence_buf = &[0b00000000,
                             0b00000001,
                             0b00000001,
                             0x94, // crc
                             0x5E,
                             0x43,
                             0x9E];
        let presence_column =
            assert_matches!(parse_presence_column(1, 3)(presence_buf), Ok((&[], pc)) => pc);
        let presence_column_view = assert_matches!(presence_column.view(0), Some(v) => v);
        #[rustfmt::skip]
        let buf = &[0x01,
                    b'R',
                    0x03,
                    b'i',
                    b'd',
                    b'e',
                    0x73, // crc
                    0x91,
                    0x5A,
                    0x74];
        assert_matches!(StringDecoder::new(buf, presence_column_view), Ok(mut decoder) => {
            assert_matches!(decoder.decode(), Ok(None));
            assert_matches!(decoder.decode(), Ok(Some(s)) => {
                assert_eq!(s, "R");
            });
            assert_matches!(decoder.decode(), Ok(Some(s)) => {
                assert_eq!(s, "ide");
            });
        });
    }

    #[test]
    fn test_decode_string_with_invalid_utf8() {
        #[rustfmt::skip]
        let presence_buf = &[0b00000001,
                             0xFC, // crc
                             0x5D,
                             0x36,
                             0xB5];
        let presence_column =
            assert_matches!(parse_presence_column(1, 1)(presence_buf), Ok((&[], pc)) => pc);
        let presence_column_view = assert_matches!(presence_column.view(0), Some(v) => v);
        #[rustfmt::skip]
        let buf = &[0x05,
                    b'a', // name with invalid utf8
                    0xF0,
                    0x90,
                    0x80,
                    b'b',
                    0xE2, // crc
                    0x76,
                    0xD0,
                    0x42];
        assert_matches!(StringDecoder::new(buf, presence_column_view), Ok(mut decoder) => {
            assert_matches!(decoder.decode(), Ok(Some(s)) => {
                assert_eq!(s, "a�b");
            });
        });
    }

    #[test]
    fn test_decode_bool_array() {
        #[rustfmt::skip]
        let presence_buf = &[0b00000001,
                             0b00000001,
                             0xE9, // crc
                             0x47,
                             0x90,
                             0x29];
        let presence_column =
            assert_matches!(parse_presence_column(1, 2)(presence_buf), Ok((&[], pc)) => pc);
        let presence_column_view = assert_matches!(presence_column.view(0), Some(v) => v);
        #[rustfmt::skip]
        let buf = &[0x05, // array len 5
                    0x00, // false
                    0x01, // true
                    0x01, // true
                    0x01, // true
                    0x00, // false
                    0x00, // array len 0
                    0xE6, // crc
                    0xB2,
                    0x88,
                    0x9C];
        assert_matches!(BoolArrayDecoder::new(buf, presence_column_view), Ok(mut decoder) => {
            assert_matches!(decoder.decode(), Ok(Some(v)) => {
                assert_eq!(v, &[false, true, true, true, false]);
            });
            assert_matches!(decoder.decode(), Ok(Some(v)) => {
                assert_eq!(v, &[]);
            });
        });
    }

    #[test]
    fn test_decode_bad_crc() {
        #[rustfmt::skip]
        let presence_buf = &[0b00000000,
                             0b00000001,
                             0b00000001,
                             0x94, // crc
                             0x5E,
                             0x43,
                             0x9E];
        let presence_column =
            assert_matches!(parse_presence_column(1, 3)(presence_buf), Ok((&[], pc)) => pc);
        let presence_column_view = assert_matches!(presence_column.view(0), Some(v) => v);
        #[rustfmt::skip]
        let buf = &[0x00,
                    0x01,
                    0x02,
                    0x00, // invalid crc
                    0x00,
                    0x00,
                    0x00];
        assert_matches!(
            StringDecoder::new(buf, presence_column_view),
            Err(crate::error::TracklibError::CRC32Error {
                expected: 0x00000000,
                computed: 0x9300784D
            })
        );
    }
}
