use crate::common::{Action, Command, Response};
use crate::engine::sled::SledKvsEngine;
use crate::engine::KvsEngine;
use crate::KvStore;
use crate::Result;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use log::LevelFilter;
use std::env::current_dir;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str;

pub struct KvsServer<T: KvsEngine> {
    engine: T,
    address: String,
}
impl<T: KvsEngine> KvsServer<T> {
    pub fn new(address_: String, engine_: T) -> Self {
        KvsServer {
            engine: engine_,
            address: address_,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.address).expect("could not start server");
        // accept connections and get a TcpStream
        for connection in listener.incoming() {
            match connection {
                Ok(stream) => {
                    if let Err(e) = self.handle_connection(stream) {
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

    fn handle_connection(&mut self, stream: TcpStream) -> Result<()> {
        let mut reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let command_len = reader.read_u32::<LE>()?;
        info!("server recv: {:?}", command_len);
        let mut buf = vec![0; command_len as usize];
        reader.read_exact(&mut buf)?;
        let command: Command = serde_json::from_str(str::from_utf8(&buf).unwrap()).unwrap();
        info!("server recv: {:?}", command);
        let res = self.exec(command);
        let serialized = serde_json::to_string(&res).unwrap();
        writer.write_u32::<LE>(serialized.len() as u32)?;
        writer.flush()?;
        writer.write(serialized.as_bytes())?;
        writer.flush()?;
        Ok(())
    }

    fn exec(&mut self, command: Command) -> Response {
        match command.action {
            Action::GET => match self.engine.get(command.key).unwrap() {
                Some(value) => Response::Ok(Some(value)),
                None => Response::Err("Key not found".to_owned()),
            },
            Action::SET => {
                self.engine
                    .set(command.key.to_owned(), command.value.to_owned())
                    .unwrap();
                Response::Ok(None)
            }
            Action::RM => match self.engine.remove(command.key.to_owned()) {
                Ok(_) => Response::Ok(None),
                Err(_) => Response::Err("Key not found".to_owned()),
            },
        }
    }
}
