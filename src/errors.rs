use std::io;

use failure::Fail;

#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    IOError(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    SerdeError(#[cause] serde_json::Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Unknown command")]
    UnknownCommand,

    #[fail(display = "Log file not found")]
    LogFileNotFound,

    #[fail(display = "Unknown engine type")]
    UnknownEngineType,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::IOError(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        KvsError::SerdeError(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
