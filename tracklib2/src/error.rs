use thiserror::Error;

#[derive(Error, Debug)]
pub enum TracklibError {
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
