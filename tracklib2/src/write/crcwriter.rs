use crate::consts::{CRC16, CRC32};
use std::io::{self, Write};

pub(crate) struct CrcWriter<'a, W, B: crc::Width> {
    inner: W,
    digest: crc::Digest<'a, B>,
}

///////////
// CRC16 //
///////////
impl<'a, W> CrcWriter<'a, W, u16> {
    pub(crate) fn new16(writer: W) -> Self {
        Self {
            inner: writer,
            digest: CRC16.digest(),
        }
    }
}

impl<'a, W> CrcWriter<'a, W, u16> {
    pub(crate) fn into_inner_and_crc(self) -> (W, u16) {
        (self.inner, self.digest.finalize())
    }
}

impl<'a, W: Write> Write for CrcWriter<'a, W, u16> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let amt = self.inner.write(buf)?;
        self.digest.update(&buf[..amt]);
        Ok(amt)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<'a, W: Write> CrcWriter<'a, W, u16> {
    pub(crate) fn append_crc(self) -> io::Result<W> {
        let (mut writer, crc) = self.into_inner_and_crc();
        writer.write_all(&crc.to_le_bytes())?;
        Ok(writer)
    }
}

///////////
// CRC32 //
///////////
impl<'a, W> CrcWriter<'a, W, u32> {
    pub(crate) fn new32(writer: W) -> Self {
        Self {
            inner: writer,
            digest: CRC32.digest(),
        }
    }
}

impl<'a, W> CrcWriter<'a, W, u32> {
    pub(crate) fn into_inner_and_crc(self) -> (W, u32) {
        (self.inner, self.digest.finalize())
    }
}

impl<'a, W: Write> Write for CrcWriter<'a, W, u32> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let amt = self.inner.write(buf)?;
        self.digest.update(&buf[..amt]);
        Ok(amt)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<'a, W: Write> CrcWriter<'a, W, u32> {
    pub(crate) fn append_crc(self) -> io::Result<W> {
        let (mut writer, crc) = self.into_inner_and_crc();
        writer.write_all(&crc.to_le_bytes())?;
        Ok(writer)
    }
}
