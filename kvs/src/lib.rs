pub use common::Command;
pub use error::KvError;
pub use error::Result;
pub use kv::KvStore;
pub use kv::KvsEngine;

#[macro_use]
extern crate failure;
extern crate env_logger;
extern crate log;
extern crate serde;
extern crate serde_json;

pub mod common;
mod error;
mod kv;
