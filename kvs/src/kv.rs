use super::error::KvError;
use super::error::Result;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::{BufReader, BufWriter, Read, SeekFrom, Write};
use std::option::Option;
use std::path::Path;
use std::path::PathBuf;
use std::str;

pub struct KvStore {
    store: HashMap<String, String>,
    log_path: PathBuf,
    log_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Entry {
    key: String,
    value: String,
    tag: Tag,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum Tag {
    Normal,
    Deleted,
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

        // OpenOptions::new()
        //     .append(true)
        //     .create(true)
        //     .read(true)
        //     .open(log_path.clone())
        //     .unwrap();

        Ok(KvStore {
            store: HashMap::new(),
            log_path: log_path,
            log_id: 0,
        })
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.append_to_log(key, val, Tag::Normal)?;
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.load_log()?;
        Ok(self.store.get(&key).cloned())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.load_log()?;
        match self.store.get(&key) {
            Some(val) => {
                self.append_to_log(key.to_owned(), val.to_string(), Tag::Deleted)?;
                Ok(())
            }
            None => Err(KvError::KeyNotExit),
        }
    }

    fn append_to_log(&self, key: String, val: String, tag: Tag) -> Result<()> {
        let file = self.open_log()?;
        let mut writer = BufWriter::new(file);
        let entry = Entry {
            key: key.to_string(),
            value: val.to_string(),
            tag: tag,
        };

        let serialized = serde_json::to_string(&entry).unwrap();
        writer.write_u32::<LE>(serialized.len() as u32)?;
        writer.write(serialized.as_bytes())?;
        writer.flush()?;
        Ok(())
    }

    fn load_log(&mut self) -> Result<()> {
        let file = self.open_log()?;
        let mut reader = BufReader::new(&file);
        loop {
            let entry_len = match reader.read_u32::<LE>() {
                Ok(len) => len,
                Err(_) => break,
            };

            let mut buf = vec![0; entry_len as usize];
            reader.read_exact(&mut buf)?;
            let entry: Entry = serde_json::from_str(str::from_utf8(&buf).unwrap()).unwrap();
            if entry.tag == Tag::Normal {
                self.store.insert(entry.key.clone(), entry.value.clone());
            } else if entry.tag == Tag::Deleted {
                self.store.remove(&entry.key);
            }
        }
        Ok(())
    }

    fn open_log(&self) -> Result<File> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(self.log_path.as_path())
            .unwrap();
        Ok(file)
    }
}
