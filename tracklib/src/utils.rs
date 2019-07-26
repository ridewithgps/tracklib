use std::io::{Write, Result};

pub(crate) fn write<W: Write>(out: &mut W, bytes: &[u8]) -> Result<usize> {
    out.write_all(bytes)?;
    Ok(bytes.len())
}
