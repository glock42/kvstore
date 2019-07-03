extern crate clap;
extern crate log;

use clap::{App, AppSettings, Arg, SubCommand};
use kvs::client::KvsClient;
use kvs::common::{Action, Command, Response};
use kvs::Result;
use log::LevelFilter;
use std::net::SocketAddr;
use std::process::exit;
use std::str;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

fn main() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();
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

    let mut command = None;
    let mut addr = None;
    if let Some(_matches) = matches.subcommand_matches("get") {
        let key = _matches.value_of("KEY").unwrap();
        addr = _matches.value_of("addr");
        command = Some(Command::new(Action::GET, key.to_owned(), "".to_owned()));
    } else if let Some(_matches) = matches.subcommand_matches("set") {
        let key = _matches.value_of("KEY").unwrap();
        let value = _matches.value_of("VALUE").unwrap();
        addr = _matches.value_of("addr");
        command = Some(Command::new(Action::SET, key.to_owned(), value.to_owned()));
    } else if let Some(_matches) = matches.subcommand_matches("rm") {
        let key = _matches.value_of("KEY").unwrap();
        command = Some(Command::new(Action::RM, key.to_owned(), "".to_owned()));
        addr = _matches.value_of("addr");
    }

    let client = KvsClient::new();
    let res = client.send_command(&command, addr.unwrap_or(DEFAULT_LISTENING_ADDRESS));

    match res {
        Response::Err(err) => match command.unwrap().action {
            Action::RM => {
                eprintln!("{}", err);
                exit(-1)
            }
            _ => {
                println!("{}", err);
                exit(0)
            }
        },
        Response::Ok(Some(val)) => println!("{}", val),
        Response::Ok(None) => {}
    };

    Ok(())
}
