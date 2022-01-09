use failure::Fail;
use sled;
use std::{io, string};

#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "sled error: {}", _0)]
    SledError(#[cause] sled::Error),

    #[fail(display = "io error: {}", _0)]
    IOError(#[cause] io::Error),

    #[fail(display = "serde error: {}", _0)]
    SerdeError(#[cause] serde_json::Error),

    #[fail(display = "encoding error: {}", _0)]
    EncodingError(#[cause] string::FromUtf8Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Unknown command")]
    UnknownCommand,

    #[fail(display = "toy bitcask error: Log file not found")]
    LogFileNotFound,

    #[fail(display = "Unknown engine type")]
    UnknownEngineType,

    #[fail(display = "server error: {}", _0)]
    ServerErrorMessage(String),
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

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> Self {
        KvsError::SledError(err)
    }
}

impl From<string::FromUtf8Error> for KvsError {
    fn from(err: string::FromUtf8Error) -> Self {
        KvsError::EncodingError(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
