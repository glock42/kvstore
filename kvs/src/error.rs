#[derive(Debug, Fail)]
pub enum KvError {
    #[fail(display = "io error")]
    IoError,
}

pub type Result<T> = core::result::Result<T, KvError>;
