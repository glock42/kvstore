extern crate clap;
use clap::{App, AppSettings, Arg, SubCommand};
use kvs::KvStore;
use kvs::Result;
use std::env::current_dir;
use std::process::exit;

fn main() -> Result<()> {
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
                .arg(Arg::with_name("VALUE").required(true)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").required(true)),
        )
        .get_matches();

    let mut store = KvStore::open(current_dir()?.as_path())?;

    if let Some(_matches) = matches.subcommand_matches("get") {
        let key = _matches.value_of("KEY").unwrap();
        store.get(key.to_owned())?;
        exit(0);
    } else if let Some(_matches) = matches.subcommand_matches("set") {
        let key = _matches.value_of("KEY").unwrap();
        let value = _matches.value_of("VALUE").unwrap();
        store.set(key.to_owned(), value.to_owned())?;
        exit(0);
    } else if let Some(_matches) = matches.subcommand_matches("rm") {
        let key = _matches.value_of("KEY").unwrap();
        store.remove(key.to_owned())?;
        exit(0);
    }
    Ok(())
}
