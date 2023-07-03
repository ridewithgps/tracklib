use std::io::Write;

#[derive(Debug)]
pub(crate) struct LimitedWriteWrapper<T> {
    buf: T,
    limit: usize,
}

impl<T> LimitedWriteWrapper<T> {
    pub(crate) fn new(buf: T, limit: usize) -> Self {
        Self { buf, limit }
    }
    pub(crate) fn expand(&mut self, new_limit: usize) {
        self.limit = new_limit;
    }

    pub(crate) fn into_inner(self) -> T {
        self.buf
    }
}

impl<T: Write> Write for LimitedWriteWrapper<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.len() > self.limit {
            Err(std::io::Error::new(std::io::ErrorKind::OutOfMemory, "buffer is full"))
        } else {
            let len = buf.len();
            match self.buf.write_all(buf) {
                Ok(_) => {
                    self.limit -= len;
                    Ok(len)
                }
                Err(e) => Err(e),
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buf.flush()
    }
}
