extern crate clap;
#[macro_use]
extern crate log;

use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use clap::{App, AppSettings, Arg, SubCommand};
use kvs::common::{Action, Command, Response};
use kvs::KvStore;
use kvs::Result;
use log::LevelFilter;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Read, SeekFrom, Write};
use std::net::SocketAddr;
use std::net::TcpStream;
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

    let res = send_command(&command, addr);

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

fn send_command(
    command: &std::option::Option<Command>,
    addr: std::option::Option<&str>,
) -> Response {
    let stream =
        TcpStream::connect(addr.unwrap_or(DEFAULT_LISTENING_ADDRESS)).expect("connection failed");
    let mut writer = BufWriter::new(&stream);
    let mut reader = BufReader::new(&stream);
    let serialized = serde_json::to_string(&command).unwrap();
    writer.write_u32::<LE>(serialized.len() as u32).unwrap();
    writer.flush().unwrap();
    writer.write(serialized.as_bytes()).unwrap();
    writer.flush().unwrap();

    let res_len = reader.read_u32::<LE>().unwrap();
    let mut buf = vec![0; res_len as usize];
    reader.read_exact(&mut buf).unwrap();
    let response: Response = serde_json::from_str(str::from_utf8(&buf).unwrap()).unwrap();
    response
}
