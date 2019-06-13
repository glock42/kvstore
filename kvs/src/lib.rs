pub use error::KvError;
pub use error::Result;
pub use kv::KvStore;
#[macro_use]
extern crate failure;

mod error;
mod kv;
