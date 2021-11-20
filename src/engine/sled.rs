use crate::common::Result;
use crate::engine::KvsEngine;
use crate::error::KvsError;

use std::path::Path;

#[derive(Clone)]
pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    pub fn open(path: &Path) -> Result<SledStore> {
        Ok(SledStore {
            db: sled::open(path)?,
        })
    }
}

impl KvsEngine for SledStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes().to_vec())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let value = self.db.get(&key)?;
        match value {
            Some(v) => Ok(Some(String::from_utf8(v.to_vec())?)),
            None => Ok(None),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}
