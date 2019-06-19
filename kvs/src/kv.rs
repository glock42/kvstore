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
    memtable: HashMap<String, u64>,
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
        let mut kv_store = KvStore {
            memtable: HashMap::new(),
            log_path: log_path,
            log_id: 0,
        };
        kv_store.load_log()?;
        Ok(kv_store)
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.append_to_log(key, val, Tag::Normal)?;
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.memtable.get(&key) {
            Some(pos) => {
                let file = self.open_log()?;
                let mut reader = BufReader::new(&file);
                Ok(Some(self.read_entry(&mut reader, pos.clone())?.value))
            }
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.memtable.get(&key) {
            Some(_) => {
                self.append_to_log(key.to_owned(), "".to_owned(), Tag::Deleted)?;
                Ok(())
            }
            None => Err(KvError::KeyNotExit),
        }
    }

    fn read_entry(&self, reader: &mut BufReader<&File>, pos: u64) -> Result<Entry> {
        reader.seek(SeekFrom::Start(pos))?;
        let entry_len = match reader.read_u32::<LE>() {
            Ok(len) => len,
            Err(_) => return Err(KvError::KeyNotExit),
        };

        let mut buf = vec![0; entry_len as usize];
        reader.read_exact(&mut buf)?;
        let entry: Entry = serde_json::from_str(str::from_utf8(&buf).unwrap()).unwrap();
        Ok(entry)
    }

    fn append_to_memtable(&mut self, entry: &Entry, pos: u64) -> Result<()> {
        if entry.tag == Tag::Normal {
            self.memtable.insert(entry.key.clone(), pos);
        } else if entry.tag == Tag::Deleted {
            self.memtable.remove(&entry.key);
        }
        Ok(())
    }

    fn append_to_log(&mut self, key: String, val: String, tag: Tag) -> Result<()> {
        let file = self.open_log()?;
        let mut writer = BufWriter::new(file);
        let entry = Entry {
            key: key.to_string(),
            value: val.to_string(),
            tag: tag,
        };

        let pos = writer.seek(SeekFrom::End(0))?;
        let serialized = serde_json::to_string(&entry).unwrap();
        writer.write_u32::<LE>(serialized.len() as u32)?;
        writer.write(serialized.as_bytes())?;
        writer.flush()?;
        self.append_to_memtable(&entry, pos)?;
        Ok(())
    }

    fn load_log(&mut self) -> Result<()> {
        let file = self.open_log()?;
        let mut reader = BufReader::new(&file);
        loop {
            let pos = reader.seek(SeekFrom::Current(0))?;
            let entry = match self.read_entry(&mut reader, pos) {
                Ok(entry) => entry,
                Err(_) => break,
            };
            self.append_to_memtable(&entry, pos)?;
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
