extern crate clap;
#[macro_use]
extern crate log;
use clap::{App, Arg};
use kvs::engine::kv::KvStore;
use kvs::engine::sled::SledKvsEngine;
use kvs::engine::KvsEngine;
use kvs::server::KvsServer;
use kvs::Result;
use log::LevelFilter;
use std::env::current_dir;
use std::str;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

fn main() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("addr")
                .value_name("IP_PORT")
                .takes_value(true)
                .short("a")
                .long("addr")
                .default_value(DEFAULT_LISTENING_ADDRESS),
        )
        .arg(
            Arg::with_name("engine")
                .value_name("ENGINE-NAME")
                .takes_value(true)
                .short("e")
                .long("engine"),
        )
        .get_matches();
    let mut address = "";
    let mut engine = "";
    if let Some(addr) = matches.value_of("addr") {
        address = addr;
    }

    if let Some(_engine) = matches.value_of("engine") {
        engine = _engine;
    }
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", address);
    if engine == "sled" {
        run_with_engine(
            address.to_owned(),
            SledKvsEngine::open(current_dir().unwrap().as_path()).unwrap(),
        )?
    } else if engine == "kvs" {
        run_with_engine(
            address.to_owned(),
            KvStore::open(current_dir().unwrap().as_path()).unwrap(),
        )?
    }
    Ok(())
}

fn run_with_engine<T: KvsEngine>(address: String, engine: T) -> Result<()> {
    let mut server = KvsServer::new(address.to_owned(), engine);
    server.run()
}
