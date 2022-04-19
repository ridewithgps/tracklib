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
    /// View into a particular field in the presence column.
    pub(crate) fn view(&self, index: usize) -> Option<PresenceColumnView<'a>> {
        if index < self.fields {
            let bit_index = 1 << (index % 8);
            let presence_bytes_required = (self.fields + 7) / 8;
            let x = (presence_bytes_required * 8 - index + 7) & !7; // next multiple of 8
            let byte_index = (x / 8) - 1;

            Some(PresenceColumnView::new(
                self.data,
                bit_index,
                byte_index,
                presence_bytes_required,
                self.rows,
            ))
        } else {
            None
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) struct PresenceColumnView<'a> {
    data: &'a [u8],
    mask: u8,
    offset: usize,
    step: usize,
    index: usize,
    max: usize,
}

impl<'a> PresenceColumnView<'a> {
    fn new(data: &'a [u8], mask: u8, offset: usize, step: usize, max: usize) -> Self {
        Self {
            data,
            mask,
            offset,
            step,
            index: 0,
            max,
        }
    }
}

impl<'a> Iterator for PresenceColumnView<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.max {
            let address = self.offset + (self.index * self.step);
            self.index += 1;
            Some(self.data[address] & self.mask > 0)
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
            let vals = (0..80)
                .map(|i| presence_column.view(i).unwrap())
                .flat_map(|view| view.collect::<Vec<bool>>())
                .collect::<Vec<bool>>();

            #[rustfmt::skip]
            assert_eq!(&vals, &[
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
            ]);
        });
    }

    #[test]
    fn test_presence_column_view() {
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
            let view = presence_column.view(0).unwrap();
            assert_eq!(view.collect::<Vec<bool>>(), &[true, true, false, true]);

            let view = presence_column.view(1).unwrap();
            assert_eq!(view.collect::<Vec<bool>>(), &[true, false, true, true]);

            let view = presence_column.view(2).unwrap();
            assert_eq!(view.collect::<Vec<bool>>(), &[false, true, true, true]);
        });
    }

    #[test]
    fn test_presence_column_view_overflowing_column_index() {
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
            assert!(presence_column.view(10).is_none());
        });
    }

    #[test]
    fn test_multibyte_presence_column_view() {
        #[rustfmt::skip]
        let buf = &[0b00000101, 0b11110100, 0b10111110,
                    0b00001111, 0b11100111, 0b11111111,
                    0xDF, // crc
                    0xC7,
                    0x91,
                    0xF5];
        assert_matches!(parse_presence_column(20, 2)(buf), Ok((&[], presence_column)) => {
            assert_eq!(presence_column.view(0).unwrap().collect::<Vec<bool>>(), &[false, true]);
            assert_eq!(presence_column.view(1).unwrap().collect::<Vec<bool>>(), &[true, true]);

            assert_eq!(presence_column.view(9).unwrap().collect::<Vec<bool>>(), &[false, true]);
            assert_eq!(presence_column.view(10).unwrap().collect::<Vec<bool>>(), &[true, true]);
            assert_eq!(presence_column.view(11).unwrap().collect::<Vec<bool>>(), &[false, false]);
            assert_eq!(presence_column.view(12).unwrap().collect::<Vec<bool>>(), &[true, false]);
        });
    }
}
