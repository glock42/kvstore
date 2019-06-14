use super::error::Result;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::path::PathBuf;

pub struct KvStore {
    store: HashMap<String, String>,
    dir: PathBuf,
    log_id: u32,
    log: File,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Entry {
    key: String,
    value: String,
}

impl KvStore {
    pub fn open(path: &Path) -> Result<KvStore> {
        let path = PathBuf::from(path);
        let mut current = path.clone();
        current.push("current");
        let mut log_id: u32 = 0;
        match File::open(current.as_path()) {
            Err(_) => {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(current.as_path())
                    .unwrap();
                file.write_all(b"0")?;
                file.sync_all()?;
            }
            Ok(file) => {
                let mut buf_reader = BufReader::new(&file);
                let mut contents = String::new();
                buf_reader.read_to_string(&mut contents)?;
                log_id = contents.parse().unwrap();
            }
        };

        let log_file_name = format!("log_{}", log_id);
        let mut log_path = path.clone();
        log_path.push(log_file_name.clone());
        //println!("{:?}", log_path);

        let log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(log_path)
            .unwrap();

        Ok(KvStore {
            store: HashMap::new(),
            dir: path,
            log_id: 0,
            log: log_file,
        })
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        //println!("set key: {}, value: {}", key, val);
        let mut writer = BufWriter::new(&self.log);

        let entry = Entry {
            key: key.to_string(),
            value: val.to_string(),
        };

        let serialized = serde_json::to_string(&entry).unwrap();
        writer.write_u32::<LE>(serialized.len() as u32)?;
        writer.write(serialized.as_bytes())?;
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<std::option::Option<String>> {
        let mut reader = BufReader::new(&self.log);
        let mut serialized = String::new();
        reader.read_to_string(&mut serialized)?;
        println!("{}", serialized);
        let entries: Vec<Entry> = serde_json::from_str(&serialized).unwrap();
        Ok(self.store.get(&key).cloned())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(&key);
        Ok(())
    }
}
