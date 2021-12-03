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

    #[error("Time Error?!")]
    TimeError {
        #[from]
        source: std::time::SystemTimeError,
    },
}

pub type Result<T, E = TracklibError> = std::result::Result<T, E>;
