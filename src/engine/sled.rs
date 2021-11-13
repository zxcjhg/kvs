use crate::{KvsEngine, KvsError, Result};

use std::path::Path;

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
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes().to_vec())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = self.db.get(&key)?;
        match value {
            Some(v) => Ok(Some(String::from_utf8(v.to_vec())?)),
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}
