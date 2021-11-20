use crate::common::{Command, Result};
use crate::engine::KvsEngine;
use crate::error::KvsError;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

/// Compaction threshold, initiated after `NUM` of write operations
// const COMPACT_THRESHOLD: u64 = 10000;
/// 2mb log file size, after that a new file is created
const MAX_FILE_SIZE: u64 = 20000000;
/// A flag in the log filename that is not compacted, but full
const FULL_FLAG: &str = "!";
/// A flag in the log filename that is compacted and full
const COMP_FLAG: &str = "#";
/// A flag in the log filename that is being written into
const WRITE_FLAG: &str = "?";
/// Extension of a log file
const LOG_EXT: &str = "log";

#[derive(Debug, Clone)]
enum LogState {
    Write,
    Full,
    Compacted,
}

#[derive(Clone)]
struct LogPointer {
    pos: u64,
    size: u64,
    filename: String,
}

/// Key Value struct
///
/// @TODO create one buffer for reading

#[derive(Clone)]
pub struct LogStructKVStore {
    log_writer: Arc<RwLock<BufWriter<File>>>,
    index: Arc<RwLock<HashMap<String, LogPointer>>>,
    path: Arc<PathBuf>,
    log: Arc<RwLock<String>>,
}

impl KvsEngine for LogStructKVStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let pos = {
            let mut log_writer = self.log_writer.write().unwrap();
            let log_position = log_writer.stream_position()?;
            let set_cmd = Command::Set { key, value };
            bincode::serialize_into(&mut *log_writer, &set_cmd)?;

            if let Command::Set { key, value: _ } = set_cmd {
                let mut index = self.index.write().unwrap();
                index.insert(
                    key,
                    LogPointer {
                        pos: log_position,
                        size: log_writer.stream_position()? - log_position,
                        filename: self.log.read().unwrap().clone(),
                    },
                );
            }
            log_writer.flush()?;
            log_writer.stream_position()?
        };

        if pos >= MAX_FILE_SIZE {
            self.compact_logs()?;
        }
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let index = self.index.read().unwrap();
        if !index.contains_key(&key) {
            return Ok(None);
        }

        let log = index.get(&key).unwrap();
        let mut reader = create_file_reader(&self.generate_full_log_path(&log.filename)?)?;
        reader.seek(SeekFrom::Start(log.pos))?;
        match bincode::deserialize_from(&mut reader)? {
            Command::Set { key: _, value } => Ok(Some(value)),
            _ => Err(KvsError::UnexpectedCommandType),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        if !self.index.read().unwrap().contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        let pos = {
            let mut log_writer = self.log_writer.write().unwrap();
            self.index.write().unwrap().remove(&key);
            bincode::serialize_into(&mut *log_writer, &Command::Rm { key })?;
            log_writer.flush()?;
            log_writer.stream_position()?
        };
        if pos >= MAX_FILE_SIZE {
            self.compact_logs()?;
        }
        Ok(())
    }
}

impl LogStructKVStore {
    pub fn open(path: &Path) -> Result<LogStructKVStore> {
        let filenames = get_sorted_log_files(path);
        let current_folder = PathBuf::from(path);

        let log_filename = if filenames.is_empty() {
            current_folder.join(create_new_filename(LogState::Write)?)
        } else {
            filenames.last().unwrap().to_path_buf()
        };

        let log_writer = Arc::new(RwLock::new(create_file_writer(&log_filename)?));
        let log_filename = parse_filename(&log_filename)?;

        let index = Arc::new(RwLock::new(build_index(&filenames)?));

        Ok(LogStructKVStore {
            log_writer,
            index,
            path: Arc::new(current_folder),
            log: Arc::new(RwLock::new(log_filename)),
        })
    }

    /// Compact logs
    /// Iterates over index and save latest commands in the newly generatd log files
    /// Redundant are removed

    fn compact_logs(&self) -> Result<()> {
        let current_folder = &self.path;
        let old_files = get_sorted_log_files(current_folder);

        {
            let mut current_log = self.log.write().unwrap();
            *current_log = create_new_filename(LogState::Write)?;
            let mut log_writer = self.log_writer.write().unwrap();
            *log_writer = create_file_writer(&self.generate_full_log_path(&current_log)?)?;
        }
        let mut new_index = HashMap::<String, LogPointer>::new();

        {
            let mut comp_log = create_new_filename(LogState::Compacted)?;
            let mut comp_writer = create_file_writer(&self.generate_full_log_path(&comp_log)?)?;

            let index = self.index.read().unwrap();
            for (key, log_pointer) in index.iter() {
                let mut buf = vec![0u8; log_pointer.size as usize];

                let mut current_reader =
                    create_file_reader(&self.generate_full_log_path(&log_pointer.filename)?)?;

                current_reader.seek(SeekFrom::Start(log_pointer.pos))?;
                current_reader.read_exact(&mut buf)?;

                new_index.insert(
                    key.clone(),
                    LogPointer {
                        pos: comp_writer.stream_position()?,
                        filename: comp_log.clone(),
                        size: log_pointer.size,
                    },
                );

                comp_writer.write_all(&buf)?;
                if comp_writer.stream_position()? > MAX_FILE_SIZE {
                    comp_log = create_new_filename(LogState::Compacted)?;
                    comp_writer = create_file_writer(&self.generate_full_log_path(&comp_log)?)?;
                }
            }
        }
        {
            let mut index = self.index.write().unwrap();
            *index = new_index;
        }

        for filename in old_files.iter() {
            fs::remove_file(&filename)?;
        }
        Ok(())
    }

    fn generate_full_log_path(&self, filename: &str) -> Result<PathBuf> {
        Ok(self.path.join(filename))
    }
}

// fn build_log_states(filenames: &[PathBuf]) -> Result<HashMap<String, LogState>> {
//     let mut index = HashMap::<String, LogState>::new();
//     for filename in filenames {
//         let (state, filename) = parse_filename(&filename)?;
//         index.insert(filename, state);
//     }
//     Ok(index)
// }

/// Builds index from all the log files
fn build_index(filenames: &[PathBuf]) -> Result<HashMap<String, LogPointer>> {
    let mut index = HashMap::<String, LogPointer>::new();

    for filename in filenames {
        let mut reader = create_file_reader(filename)?;
        let mut log_position = reader.stream_position()?;
        let filename = parse_filename(filename)?;
        while let Ok(cmd) = bincode::deserialize_from(&mut reader) {
            match cmd {
                Command::Set { key, value: _ } => index.insert(
                    key,
                    LogPointer {
                        pos: log_position,
                        size: reader.stream_position()? - log_position,
                        filename: filename.clone(),
                    },
                ),
                Command::Rm { key } => index.remove(&key),
                _ => return Err(KvsError::UnexpectedCommandType),
            };
            log_position = reader.stream_position()?;
        }
    }
    Ok(index)
}

fn parse_filename(path: &Path) -> Result<String> {
    Ok(path.file_name().unwrap().to_str().unwrap().to_string())
}
/// Generates new log filename with given `state`
fn create_new_filename(state: LogState) -> Result<String> {
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let flag = match state {
        LogState::Write => WRITE_FLAG,
        LogState::Full => FULL_FLAG,
        LogState::Compacted => COMP_FLAG,
    };
    Ok(format!("{}{}.{}", flag, timestamp, LOG_EXT))
}

/// Created a buffered writer for a given file
fn create_file_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = OpenOptions::new().append(true).create(true).open(&path)?;
    let mut log_writer = BufWriter::new(file);
    log_writer.seek(SeekFrom::End(0))?;
    Ok(log_writer)
}
/// Created a buffered reader for a given file
fn create_file_reader(path: &Path) -> Result<BufReader<File>> {
    Ok(BufReader::new(File::open(&path)?))
}

/// Returns all the log file paths in the current directory
fn get_sorted_log_files(path: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(path)
        .unwrap()
        .into_iter()
        .map(|x| x.unwrap().path())
        .filter(|x| x.file_name().unwrap().to_str().unwrap().ends_with(&LOG_EXT))
        .collect::<Vec<PathBuf>>();

    files.sort();
    files
}
