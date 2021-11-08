use failure::Fail;
use serde_json::Error;
use std::io;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
    #[fail(display = "Error with de/serialization  {}", _0)]
    Serde(#[cause] serde_json::Error),
    #[fail(display = "Problem with IO {}", _0)]
    Io(#[cause] io::Error),
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
