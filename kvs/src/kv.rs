use super::error::Result;
use std::collections::HashMap;
use std::path::Path;

pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    pub fn open(_path: &Path) -> Result<KvStore> {
        Ok(KvStore {
            store: HashMap::new(),
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
