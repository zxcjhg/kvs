use crate::error::KvsError;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;

const LOG_FILENAME: &str = "log.txt";

#[derive(Serialize, Deserialize)]
enum Command<'a> {
    Get(&'a str),
    Rm(&'a str),
    Set(&'a str, &'a str),
}

pub type Result<T> = std::result::Result<T, KvsError>;

/// Key Value struct
pub struct KvStore {
    reader: RefCell<BufReader<File>>,
    writer: BufWriter<File>,
    index: HashMap<String, u64>,
}

impl KvStore {
    /// Sets a `value` for a given `key`
    /// Overrides with new `value` if `key` already exists

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let log_position = self.writer.stream_position()?;
        serde_json::to_writer(&mut self.writer, &Command::Set(&key, &value))?;
        self.writer.write_all(b"\n")?;
        self.index.insert(key, log_position);
        self.writer.flush()?;
        Ok(())
    }
    /// Retrieves value from storage for a given `key`
    ///
    /// # Panics
    ///
    /// Panics if `key` doesn't exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        if !self.index.contains_key(&key) {
            return Ok(None);
        }

        let log_position = self.index[&key];
        self.reader
            .borrow_mut()
            .seek(SeekFrom::Start(log_position))?;
        let mut temp_buffer = String::new();
        self.reader.borrow_mut().read_line(&mut temp_buffer)?;
        match serde_json::from_str(&temp_buffer)? {
            Command::Set(_key, _value) => Ok(Some(_value.to_string())),
            _ => Err(KvsError::UnexpectedCommandType),
        }
    }

    /// Removes a entry for a given `key`
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        serde_json::to_writer(&mut self.writer, &Command::Rm(&key))?;
        self.writer.write_all(b"\n")?;
        self.index.remove(&key);
        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut file_path = path.into();
        file_path.push(LOG_FILENAME);
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)?;

        let mut writer = BufWriter::new(file);
        let reader = RefCell::new(BufReader::new(File::open(file_path)?));
        let mut index = HashMap::<String, u64>::new();
        let mut temp_buffer = String::new();
        let mut reader_pointer = reader.borrow_mut();

        let mut log_position = reader_pointer.stream_position()?;
        while reader_pointer.read_line(&mut temp_buffer)? > 0 {
            match serde_json::from_str(&temp_buffer)? {
                Command::Set(_key, _value) => index.insert(_key.to_string(), log_position),
                Command::Rm(key) => index.remove(key),
                _ => return Err(KvsError::UnexpectedCommandType),
            };
            log_position = reader_pointer.stream_position()?;
            temp_buffer.clear();
        }
        writer.seek(SeekFrom::End(0))?;
        reader_pointer.seek(SeekFrom::Start(0))?;
        drop(reader_pointer);

        Ok(KvStore {
            index,
            reader,
            writer,
        })
    }
}
