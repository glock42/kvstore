extern crate clap;
use clap::{App, AppSettings, Arg, SubCommand};
use std::process::exit;

fn main() {
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

    if let Some(_matches) = matches.subcommand_matches("get") {
        eprintln!("unimplemented");
        exit(1);
    } else if let Some(_matches) = matches.subcommand_matches("set") {
        eprintln!("unimplemented");
        exit(1);
    } else if let Some(_matches) = matches.subcommand_matches("rm") {
        eprintln!("unimplemented");
        exit(1);
    }
}
