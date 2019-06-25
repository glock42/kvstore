pub use error::KvError;
pub use error::Result;
pub use kv::KvStore;
pub use kv::KvsEngine;
#[macro_use]
extern crate failure;
extern crate serde;
extern crate serde_json;

mod error;
mod kv;
