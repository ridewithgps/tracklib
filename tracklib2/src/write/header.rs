use super::crcwriter::CrcWriter;
use crate::consts::{RWTFMAGIC, RWTF_CREATOR_VERSION, RWTF_FILE_VERSION};
use crate::error::Result;
use std::io::Write;

#[rustfmt::skip]
pub(crate) fn write_header<W: Write>(out: &mut W, metadata_table_offset: u16, data_table_offset: u16) -> Result<()> {
    let mut crcwriter = CrcWriter::new16(out);
    crcwriter.write_all(&RWTFMAGIC)?;                                    // 8 bytes - Magic Number
    crcwriter.write_all(&RWTF_FILE_VERSION.to_le_bytes())?;              // 1 byte  - File Version
    crcwriter.write_all(&[0x00, 0x00, 0x00])?;                           // 3 bytes - FV Reserve
    crcwriter.write_all(&RWTF_CREATOR_VERSION.to_le_bytes())?;           // 1 byte  - Creator Version
    crcwriter.write_all(&[0x00, 0x00, 0x00])?;                           // 3 bytes - CV Reserve
    crcwriter.write_all(&metadata_table_offset.to_le_bytes())?;          // 2 bytes - Offset to Metadata Table
    crcwriter.write_all(&data_table_offset.to_le_bytes())?;              // 2 bytes - Offset to Data Table
    crcwriter.write_all(&[0x00, 0x00])?;                                 // 2 bytes - E Reserve
    crcwriter.append_crc()?;                                             // 2 bytes - Header CRC
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_write_header() {
        let mut buf = vec![];
        assert_matches!(write_header(&mut buf, 0x0A, 0x1A), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x89, // magic number
                         0x52,
                         0x57,
                         0x54,
                         0x46,
                         0x0A,
                         0x1A,
                         0x0A,
                         0x01, // file version
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
                         0x76]);
        });
    }
}
