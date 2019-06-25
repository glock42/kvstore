extern crate clap;
use clap::{App, AppSettings, Arg, SubCommand};
use kvs::KvStore;
use kvs::Result;
use std::env::current_dir;
use std::net::SocketAddr;
use std::process::exit;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

fn main() -> Result<()> {
    let addr_arg = Arg::with_name("addr")
        .value_name("IP_PORT")
        .takes_value(true)
        .short("a")
        .long("addr")
        .default_value(DEFAULT_LISTENING_ADDRESS)
        .validator(|addr| {
            if addr.parse::<SocketAddr>().is_ok() {
                Ok(())
            } else {
                Err(String::from("the ip address format is error: IP:PORT"))
            }
        });

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").required(true))
                .arg(Arg::with_name("VALUE").required(true))
                .arg(&addr_arg),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").required(true))
                .arg(&addr_arg),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").required(true))
                .arg(&addr_arg),
        )
        .get_matches();

    let mut store = KvStore::open(current_dir()?.as_path())?;

    if let Some(_matches) = matches.subcommand_matches("get") {
        let key = _matches.value_of("KEY").unwrap();
        match store.get(key.to_owned())? {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        };
        exit(0);
    } else if let Some(_matches) = matches.subcommand_matches("set") {
        let key = _matches.value_of("KEY").unwrap();
        let value = _matches.value_of("VALUE").unwrap();
        store.set(key.to_owned(), value.to_owned())?;
        exit(0);
    } else if let Some(_matches) = matches.subcommand_matches("rm") {
        let key = _matches.value_of("KEY").unwrap();
        match store.remove(key.to_owned()) {
            Err(_) => {
                println!("Key not found");
                exit(-1);
            }
            Ok(_) => {
                exit(0);
            }
        };
    }
    Ok(())
}
