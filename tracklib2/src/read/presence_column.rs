use super::crc::CRC;
use crate::error::TracklibError;
use nom::{bytes::complete::take, IResult};

#[cfg_attr(test, derive(Debug))]
pub(crate) struct PresenceColumn<'a> {
    data: &'a [u8],
    fields: usize,
    rows: usize,
}

impl<'a> PresenceColumn<'a> {
    fn iter(&self) -> PresenceColumnIter {
        PresenceColumnIter {
            data: &self.data,
            fields: self.fields,
            bytes_required: (self.fields + 7) / 8,
        }
    }
}

struct PresenceColumnIter<'a> {
    data: &'a [u8],
    fields: usize,
    bytes_required: usize,
}

impl<'a> Iterator for PresenceColumnIter<'a> {
    type Item = PresenceRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes_required <= self.data.len() {
            let (row, rest) = self.data.split_at(self.bytes_required);
            self.data = rest;
            Some(PresenceRow::new(row, self.fields))
        } else {
            None
        }
    }
}

struct PresenceRow<'a> {
    data: &'a [u8],
    mask: u8,
    bit_index: usize,
    fields: usize,
}

impl<'a> PresenceRow<'a> {
    fn new(data: &'a [u8], fields: usize) -> Self {
        Self {
            data,
            mask: 1,
            bit_index: (fields + 7) & !7, // next multiple of 8
            fields,
        }
    }
}

impl<'a> Iterator for PresenceRow<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.fields > 0 {
            let byte_index = ((self.bit_index + 7) / 8) - 1;
            let is_field_present = self.data[byte_index] & self.mask > 0;
            self.mask = self.mask.rotate_left(1);
            self.fields -= 1;
            self.bit_index -= 1;
            Some(is_field_present)
        } else {
            None
        }
    }
}

pub(crate) fn parse_presence_column<'a>(
    fields: usize,
    rows: usize,
) -> impl Fn(&'a [u8]) -> IResult<&'a [u8], PresenceColumn<'a>, TracklibError> {
    let bytes_required = (fields + 7) / 8;
    let size = bytes_required * rows;
    move |input: &[u8]| {
        let input_start = input;
        let (input, data) = take(size)(input)?;
        let (input, checksum) = CRC::<u32>::parser(input_start)(input)?;

        match checksum {
            CRC::Valid(_) => Ok((input, PresenceColumn { data, fields, rows })),
            CRC::Invalid { expected, computed } => {
                Err(nom::Err::Error(TracklibError::CRC32Error {
                    expected,
                    computed,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_read_presence_column() {
        #[rustfmt::skip]
        let buf = &[0b00000011,
                    0b00000101,
                    0b00000110,
                    0b00000111,
                    0xD2, // crc
                    0x61,
                    0xA7,
                    0xA5];
        assert_matches!(parse_presence_column(3, 4)(buf), Ok((&[], presence_column)) => {
            let vals: Vec<Vec<bool>> = presence_column.iter().map(|row| row.collect()).collect();
            #[rustfmt::skip]
            assert_eq!(&vals, &[&[true, true, false],
                                &[true, false, true],
                                &[false, true, true],
                                &[true, true, true]]);

        });
    }

    #[test]
    fn test_read_multibyte_presence_column() {
        #[rustfmt::skip]
        let buf = &[0b00001111, 0b11111111, 0b11111111,
                    0b00001111, 0b11111111, 0b11111111,
                    0xDD, // crc
                    0xCB,
                    0x18,
                    0x17];
        assert_matches!(parse_presence_column(20, 2)(buf), Ok((&[], presence_column)) => {
            let vals: Vec<Vec<bool>> = presence_column.iter().map(|row| row.collect()).collect();
            assert_eq!(&vals, &[std::iter::repeat(true).take(20).collect::<Vec<bool>>(),
                                std::iter::repeat(true).take(20).collect::<Vec<bool>>()]);

        });
    }

    #[test]
    fn test_read_huge_presence_column() {
        #[rustfmt::skip]
        let buf = &[0b11111000, // 10
                    0b00101111, // 9
                    0b00000000, // 8
                    0b11001111, // 7
                    0b11111110, // 6
                    0b11111111, // 5
                    0b00011111, // 4
                    0b11111100, // 3
                    0b11111111, // 2
                    0b11111111, // 1
                    0x92, // crc
                    0x0E,
                    0x6F,
                    0xC2];
        assert_matches!(parse_presence_column(80, 1)(buf), Ok((&[], presence_column)) => {
            let vals: Vec<Vec<bool>> = presence_column.iter().map(|row| row.collect()).collect();
            #[rustfmt::skip]
            assert_eq!(&vals, &[&[
                true,  true,  true,  true,  true,  true,  true,  true,  // 1
                true,  true,  true,  true,  true,  true,  true,  true,  // 2
                false, false, true,  true,  true,  true,  true,  true,  // 3
                true,  true,  true,  true,  true,  false, false, false, // 4
                true,  true,  true,  true,  true,  true,  true,  true,  // 5
                false, true,  true,  true,  true,  true,  true,  true,  // 6
                true,  true,  true,  true,  false, false, true,  true,  // 7
                false, false, false, false, false, false, false, false, // 8
                true,  true,  true,  true,  false, true,  false, false, // 9
                false, false, false, true,  true,  true,  true,  true,  // 10
            ]]);
        });
    }
}
