pub use error::KvError;
pub use error::Result;
pub use kv::KvStore;
#[macro_use]
extern crate failure;
extern crate serde;
extern crate serde_json;

mod error;
mod kv;
