#[rustfmt::skip]
pub(crate) const RWTFMAGIC: [u8; 8] = [0x89,  // non-ascii
                                       0x52,  // R
                                       0x57,  // W
                                       0x54,  // T
                                       0x46,  // F
                                       0x0A,  // newline
                                       0x1A,  // ctrl-z
                                       0x0A]; // newline
