use std::io;
#[derive(Debug, Fail)]
pub enum KvError {
    #[fail(display = "io error")]
    IoError(#[fail(cause)] io::Error),
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError::IoError(err)
    }
}

pub type Result<T> = std::result::Result<T, KvError>;
