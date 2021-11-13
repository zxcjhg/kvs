use failure::Fail;
use serde_json::Error;
use std::io;
use std::string::FromUtf8Error;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
    #[fail(display = "Error with de/serialization  {}", _0)]
    Serde(#[cause] serde_json::Error),
    #[fail(display = "Error with sled storage  {}", _0)]
    Sled(#[cause] sled::Error),
    #[fail(display = "Problem with IO {}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "Problem with Utf8 {}", _0)]
    Utf8(#[cause] FromUtf8Error),
}

impl From<serde_json::Error> for KvsError {
    fn from(err: Error) -> Self {
        KvsError::Serde(err)
    }
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::Io(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> Self {
        KvsError::Sled(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> Self {
        KvsError::Utf8(err)
    }
}
