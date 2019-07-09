use crate::engine::KvsEngine;
use crate::error::KvError;
use crate::error::Result;
use sled::{Db, Tree};
use std::path::{Path, PathBuf};
pub struct SledKvsEngine {
    tree: sled::Db,
}

impl SledKvsEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let path = path.into();
        Ok(SledKvsEngine {
            tree: Db::start_default(path)?,
        })
    }
}
impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.tree.set(key, value.into_bytes())?;
        self.tree.flush()?;
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self
            .tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.tree.del(key)?.ok_or(KvError::KeyNotExit)?;
        self.tree.flush()?;
        Ok(())
    }
}
