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
    reader: BufReader<File>,
    writer: BufWriter<File>,
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

const LOG_THRESHOLD: u64 = 4 * 1024 * 1024;

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
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;
        let path = PathBuf::from(path);
        println!("open kvstore: {:?}", path);
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

        //println!("kv opened, log_id: {}", log_id);
        let reader = BufReader::new(open_log(get_log_path(path.clone(), log_id)?)?);
        let writer = BufWriter::new(open_log(get_log_path(path.clone(), log_id)?)?);
        let mut kv_store = KvStore {
            memtable: Box::new(HashMap::new()),
            immutable: Box::new(HashMap::new()),
            dir: path,
            log_id: log_id,
            reader: reader,
            writer: writer,
        };
        kv_store.load_log()?;
        Ok(kv_store)
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
        let pos = append_entry(&mut self.writer, &entry)?;
        self.append_to_memtable(&entry, pos)?;

        if log_size(get_log_path(self.dir.clone(), self.log_id)?)? >= LOG_THRESHOLD {
            self.compaction()?;
        }
        Ok(())
    }

    fn compaction(&mut self) -> Result<()> {
        // println!(
        //     "start compaction, current log_id: {}, append_num: {}",
        //     self.log_id, self.append_num
        // );
        self.log_id += 1;
        let mut new_writer =
            BufWriter::new(open_log(get_log_path(self.dir.clone(), self.log_id)?)?);
        let mut new_mem: Box<HashMap<String, u64>> = Box::new(HashMap::new());

        for (_, pointer) in self.memtable.iter() {
            let entry = read_entry(&mut self.reader, *pointer)?;
            let pos = append_entry(&mut new_writer, &entry)?;
            if entry.tag == Tag::Normal {
                new_mem.insert(entry.key.clone(), pos);
            } else if entry.tag == Tag::Deleted {
                new_mem.remove(&entry.key);
            }
        }
        self.memtable = new_mem;
        self.writer = new_writer;
        self.reader = BufReader::new(open_log(get_log_path(self.dir.clone(), self.log_id)?)?);
        std::fs::remove_file(get_log_path(self.dir.clone(), self.log_id - 1)?)?;
        self.update_current()?;
        Ok(())
    }

    fn load_log(&mut self) -> Result<()> {
        loop {
            let pos = self.reader.seek(SeekFrom::Current(0))?;
            let entry = match read_entry(&mut self.reader, pos) {
                Ok(entry) => entry,
                Err(_) => break,
            };
            self.append_to_memtable(&entry, pos)?;
        }
        Ok(())
    }

    fn update_current(&self) -> Result<()> {
        // println!("update current, log_id: {}", self.log_id);
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
        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, val: String) -> Result<()> {
        //println!("set {}, {}", key, val);
        self.start_write(key, val, Tag::Normal)?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        println!("get {}", key);
        match self.memtable.get(&key) {
            Some(pos) => Ok(Some(read_entry(&mut self.reader, pos.clone())?.value)),
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
        println!("kv droped");
        self.update_current().expect("update current failed");
    }
}

fn get_log_path(dir: PathBuf, log_id: u32) -> Result<(PathBuf)> {
    let log_file_name = format!("log_{}", log_id);
    let mut log_path = dir.clone();
    log_path.push(log_file_name);
    Ok(log_path)
}

fn open_log(path: PathBuf) -> Result<File> {
    //println!("open log: {:?}", path);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path.as_path())
        .expect("open log file failed");
    Ok(file)
}

fn log_size(path: PathBuf) -> Result<u64> {
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.len())
}

fn read_entry(reader: &mut BufReader<File>, pos: u64) -> Result<Entry> {
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

fn append_entry(writer: &mut BufWriter<File>, entry: &Entry) -> Result<(u64)> {
    let pos = writer.seek(SeekFrom::End(0))?;
    let serialized = serde_json::to_string(&entry).unwrap();
    writer.write_u32::<LE>(serialized.len() as u32)?;
    writer.write(serialized.as_bytes())?;
    writer.flush()?;
    Ok(pos)
}
