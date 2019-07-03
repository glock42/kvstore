use std::io;
use std::string::FromUtf8Error;
#[derive(Debug, Fail)]
pub enum KvError {
    #[fail(display = "io error: {}", _0)]
    IoError(#[fail(cause)] io::Error),

    #[fail(display = "sled error: {}", _0)]
    Sled(#[cause] sled::Error),

    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] FromUtf8Error),

    #[fail(display = "key not exit")]
    KeyNotExit,
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError::IoError(err)
    }
}

impl From<sled::Error> for KvError {
    fn from(err: sled::Error) -> KvError {
        KvError::Sled(err)
    }
}

impl From<FromUtf8Error> for KvError {
    fn from(err: FromUtf8Error) -> KvError {
        KvError::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, KvError>;
