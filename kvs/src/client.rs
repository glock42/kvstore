use crate::common::{Command, Response};
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::str;

pub struct KvsClient {}

impl KvsClient {
    pub fn new() -> Self {
        KvsClient {}
    }

    pub fn send_command(&self, command: &std::option::Option<Command>, addr: &str) -> Response {
        //let stream = TcpStream::connect(addr.unwrap_or(DEFAULT_LISTENING_ADDRESS))
        let stream = TcpStream::connect(addr).expect("connection failed");
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
}
