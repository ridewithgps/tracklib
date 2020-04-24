pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    JNIError(jni::errors::Error),
    NomError,
}

impl From<jni::errors::Error> for Error {
    fn from(e: jni::errors::Error) -> Self {
        Error::JNIError(e)
    }
}

impl From<nom::Err<&[u8]>> for Error {
    fn from(_e: nom::Err<&[u8]>) -> Self {
        Error::NomError
    }
}
