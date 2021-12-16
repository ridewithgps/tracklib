use thiserror::Error;

#[derive(Error, Debug)]
pub enum TracklibError {
    #[error("Parse Error")]
    ParseError { error_kind: nom::error::ErrorKind },

    #[error("Parse Incomplete")]
    ParseIncompleteError { needed: nom::Needed },

    #[error("CRC Error")]
    CRC16Error { expected: u16, computed: u16 },

    #[error("Numeric Bounds Error")]
    BoundsError {
        #[from]
        source: std::num::TryFromIntError,
    },

    #[error("IO Error")]
    IOError {
        #[from]
        source: std::io::Error,
    },
}

pub type Result<T, E = TracklibError> = std::result::Result<T, E>;

impl<I: Sized> nom::error::ParseError<I> for TracklibError {
    fn from_error_kind(input: I, kind: nom::error::ErrorKind) -> Self {
        Self::ParseError { error_kind: kind }
    }

    fn append(input: I, kind: nom::error::ErrorKind, other: Self) -> Self {
        other
    }
}

impl<I: Sized> nom::error::ContextError<I> for TracklibError {}

impl From<nom::Err<TracklibError>> for TracklibError {
    fn from(error: nom::Err<TracklibError>) -> Self {
        match error {
            nom::Err::Incomplete(needed) => Self::ParseIncompleteError { needed },
            nom::Err::Error(e) => e,
            nom::Err::Failure(e) => e,
        }
    }
}
