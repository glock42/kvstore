use super::error::Result;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;

pub struct KvStore {
    store: HashMap<String, String>,
    dir: PathBuf,
    log_id: u32,
    log: BufReader<File>,
}

impl KvStore {
    pub fn open(path: PathBuf) -> Result<KvStore> {
        println!("{:?}", path);
        let mut current = path.clone();
        current.push("/current");
        let file = match File::open(current.as_path()) {
            Err(_) => {
                let mut file = File::create(&path)?;
                file.write_all(b"0")?;
                file
            }
            Ok(file) => file,
        };
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents)?;
        let log_id: u32 = contents.parse().unwrap();
        let log_file_name = format!("log_{}", log_id);

        let log_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(log_file_name)
            .unwrap();

        let log = BufReader::new(log_file);
        Ok(KvStore {
            store: HashMap::new(),
            dir: path,
            log_id: 0,
            log: log,
        })
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.store.insert(key, val);
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<std::option::Option<String>> {
        Ok(self.store.get(&key).cloned())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(&key);
        Ok(())
    }
}
