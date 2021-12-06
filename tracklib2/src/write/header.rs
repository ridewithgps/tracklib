use crate::consts::{CRC16, RWTFMAGIC};
use crate::error::Result;
use std::io::Write;

#[rustfmt::skip]
pub(crate) fn write_header<W: Write>(out: &mut W, file_version: u8, creator_version: u8, metadata_table_offset: u16, data_table_offset: u16) -> Result<usize> {
    let mut buf = Vec::with_capacity(24);

    buf.write_all(&RWTFMAGIC)?;                                    // 8 bytes - Magic Number
    buf.write_all(&file_version.to_le_bytes())?;                   // 1 byte  - File Version
    buf.write_all(&[0x00, 0x00, 0x00])?;                           // 3 bytes - FV Reserve
    buf.write_all(&creator_version.to_le_bytes())?;                // 1 byte  - Creator Version
    buf.write_all(&[0x00, 0x00, 0x00])?;                           // 3 bytes - CV Reserve
    buf.write_all(&metadata_table_offset.to_le_bytes())?;          // 2 bytes - Offset to Metadata Table
    buf.write_all(&data_table_offset.to_le_bytes())?;              // 2 bytes - Offset to Data Table
    buf.write_all(&[0x00, 0x00])?;                                 // 2 bytes - E Reserve
    buf.write_all(&CRC16.checksum(&buf).to_le_bytes())?;           // 2 bytes - Header CRC

    out.write_all(&buf)?;
    Ok(buf.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_header() {
        let mut buf = vec![];
        let written = write_header(&mut buf, 0x00, 0x00, 0x0A, 0x1A);
        assert!(written.is_ok());
        #[rustfmt::skip]
        let expected = &[0x89, // magic number
                         0x52,
                         0x57,
                         0x54,
                         0x46,
                         0x0A,
                         0x1A,
                         0x0A,
                         0x00, // file version
                         0x00, // fv reserve
                         0x00,
                         0x00,
                         0x00, // creator version
                         0x00, // cv reserve
                         0x00,
                         0x00,
                         0x0A, // metadata table offset
                         0x00,
                         0x1A, // data offset
                         0x00,
                         0x00, // e reserve
                         0x00,
                         0x86, // header crc
                         0xB7];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }
}
