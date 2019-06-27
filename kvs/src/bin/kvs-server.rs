extern crate clap;
#[macro_use]
extern crate log;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use clap::{App, Arg};
use kvs::common::{Action, Command, Response};
use kvs::engine::KvsEngine;
use kvs::KvStore;
use kvs::Result;
use log::LevelFilter;
use std::env::current_dir;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
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
    run(address.to_owned(), engine.to_owned())
}

fn run(addr: String, _engine: String) -> Result<()> {
    let listener = TcpListener::bind(addr).expect("could not start server");
    // accept connections and get a TcpStream
    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                if let Err(e) = handle_connection(stream) {
                    println!("error {:?}", e);
                }
            }
            Err(e) => {
                println!("connection failed {}", e);
            }
        }
    }
    Ok(())
}

fn handle_connection(stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let command_len = reader.read_u32::<LE>()?;
    info!("server recv: {:?}", command_len);
    let mut buf = vec![0; command_len as usize];
    reader.read_exact(&mut buf)?;
    let command: Command = serde_json::from_str(str::from_utf8(&buf).unwrap()).unwrap();
    info!("server recv: {:?}", command);
    let res = exec(command);
    let serialized = serde_json::to_string(&res).unwrap();
    writer.write_u32::<LE>(serialized.len() as u32)?;
    writer.flush()?;
    writer.write(serialized.as_bytes())?;
    writer.flush()?;
    Ok(())
}

fn exec(command: Command) -> Response {
    let mut store = KvStore::open(current_dir().unwrap().as_path()).unwrap();
    match command.action {
        Action::GET => match store.get(command.key).unwrap() {
            Some(value) => Response::Ok(Some(value)),
            None => Response::Err("Key not found".to_owned()),
        },
        Action::SET => {
            store
                .set(command.key.to_owned(), command.value.to_owned())
                .unwrap();
            Response::Ok(None)
        }
        Action::RM => match store.remove(command.key.to_owned()) {
            Ok(_) => Response::Ok(None),
            Err(_) => Response::Err("Key not found".to_owned()),
        },
    }
}
