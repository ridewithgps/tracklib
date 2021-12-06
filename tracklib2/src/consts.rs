pub(crate) const CRC16: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_USB);
pub(crate) const CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_BZIP2);

#[rustfmt::skip]
pub(crate) const RWTFMAGIC: [u8; 8] = [0x89,  // non-ascii
                                       0x52,  // R
                                       0x57,  // W
                                       0x54,  // T
                                       0x46,  // F
                                       0x0A,  // newline
                                       0x1A,  // ctrl-z
                                       0x0A]; // newline

pub(crate) const RWTF_FILE_VERSION: u8 = 0x01;
pub(crate) const RWTF_CREATOR_VERSION: u8 = 0x00;
