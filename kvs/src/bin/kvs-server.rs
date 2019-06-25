extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;
use clap::{App, AppSettings, Arg, SubCommand};
use kvs::KvStore;
use kvs::Result;
use log::LevelFilter;
use log::{info, trace};

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
    Ok(())
}
