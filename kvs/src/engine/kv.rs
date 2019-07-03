use crate::engine::KvsEngine;
use crate::error::KvError;
use crate::error::Result;
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
    memtable: Box<HashMap<String, u64>>,
    immutable: Box<HashMap<String, u64>>,
    dir: PathBuf,
    log_id: u32,
    append_num: u32,
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

const LOG_APPEND_NUM: u32 = 1000;

impl Entry {
    fn new(k: String, v: String, t: Tag) -> Self {
        Entry {
            key: k,
            value: v,
            tag: t,
        }
    }
}

impl KvStore {
    pub fn open(path: &Path) -> Result<KvStore> {
        let path = PathBuf::from(path);
        let mut current = path.clone();
        current.push("current");
        let mut log_id: u32 = 0;
        match File::open(current.as_path()) {
            Err(_) => {
                let mut file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(current.as_path())
                    .unwrap();
                file.write_u32::<LE>(log_id)?;
                file.sync_all()?;
            }
            Ok(file) => {
                let mut buf_reader = BufReader::new(&file);
                log_id = buf_reader.read_u32::<LE>()?;
            }
        };

        let mut kv_store = KvStore {
            memtable: Box::new(HashMap::new()),
            immutable: Box::new(HashMap::new()),
            dir: path,
            log_id: log_id,
            append_num: 0,
        };
        kv_store.load_log()?;
        Ok(kv_store)
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

    fn append_entry(&self, entry: &Entry, file: &File) -> Result<(u64)> {
        let mut writer = BufWriter::new(file);
        let pos = writer.seek(SeekFrom::End(0))?;
        let serialized = serde_json::to_string(&entry).unwrap();
        writer.write_u32::<LE>(serialized.len() as u32)?;
        writer.write(serialized.as_bytes())?;
        writer.flush()?;
        Ok(pos)
    }

    fn append_to_memtable(&mut self, entry: &Entry, pos: u64) -> Result<()> {
        if entry.tag == Tag::Normal {
            self.memtable.insert(entry.key.clone(), pos);
        } else if entry.tag == Tag::Deleted {
            self.memtable.remove(&entry.key);
        }
        Ok(())
    }

    fn start_write(&mut self, key: String, val: String, tag: Tag) -> Result<()> {
        let entry = Entry::new(key, val, tag);
        let file = self.open_log(self.get_log_path(self.log_id)?)?;
        let pos = self.append_entry(&entry, &file)?;
        self.append_to_memtable(&entry, pos)?;
        self.append_num += 1;

        if self.append_num >= LOG_APPEND_NUM {
            println!("start compaction!");
            self.compaction()?;
            self.append_num = 0;
        }
        Ok(())
    }

    fn compaction(&mut self) -> Result<()> {
        self.log_id += 1;
        let read_file = self.open_log(self.get_log_path(self.log_id - 1)?)?;
        let write_file = self.open_log(self.get_log_path(self.log_id)?)?;
        let mut reader = BufReader::new(&read_file);
        let mut new_mem: Box<HashMap<String, u64>> = Box::new(HashMap::new());

        for (_, pointer) in self.memtable.iter() {
            let entry = self.read_entry(&mut reader, *pointer)?;
            let pos = self.append_entry(&entry, &write_file)?;
            if entry.tag == Tag::Normal {
                new_mem.insert(entry.key.clone(), pos);
            } else if entry.tag == Tag::Deleted {
                new_mem.remove(&entry.key);
            }
        }
        self.memtable = new_mem;
        std::fs::remove_file(self.get_log_path(self.log_id - 1)?)?;
        Ok(())
    }

    fn get_log_path(&self, log_id: u32) -> Result<(PathBuf)> {
        let log_file_name = format!("log_{}", log_id);
        let mut log_path = self.dir.clone();
        log_path.push(log_file_name);
        Ok(log_path)
    }

    fn load_log(&mut self) -> Result<()> {
        let file = self.open_log(self.get_log_path(self.log_id)?)?;
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

    fn open_log(&self, path: PathBuf) -> Result<File> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path.as_path())
            .unwrap();
        Ok(file)
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, val: String) -> Result<()> {
        self.start_write(key, val, Tag::Normal)?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.memtable.get(&key) {
            Some(pos) => {
                let file = self.open_log(self.get_log_path(self.log_id)?)?;
                let mut reader = BufReader::new(&file);
                Ok(Some(self.read_entry(&mut reader, pos.clone())?.value))
            }
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.memtable.get(&key) {
            Some(_) => {
                self.start_write(key.to_owned(), "".to_owned(), Tag::Deleted)?;
                Ok(())
            }
            None => Err(KvError::KeyNotExit),
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        let mut current = self.dir.clone();
        current.push("current");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(current.as_path())
            .unwrap();
        file.write_u32::<LE>(self.log_id).expect("write_u32 failed");
        file.sync_all().expect("sync_all failed");
    }
}
