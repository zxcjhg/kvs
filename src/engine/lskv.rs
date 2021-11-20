use crate::common::{Command, Result};
use crate::engine::KvsEngine;
use crate::error::KvsError;
use std::cmp::max;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

/// Compaction threshold, initiated after `NUM` of write operations
// const COMPACT_THRESHOLD: u64 = 10000;
/// 2mb log file size, after that a new file is created
const MAX_FILE_SIZE: u64 = 2000;
/// A flag in the log filename that is not compacted, but full
const FULL_FLAG: &str = "!";
/// A flag in the log filename that is compacted and full
const COMP_FLAG: &str = "#";
/// A flag in the log filename that is being written into
const WRITE_FLAG: &str = "?";

const LOG_WRITE: u8 = 1;
const LOG_FULL: u8 = 2;
const LOG_COMP: u8 = 3;
/// Extension of a log file
const LOG_EXT: &str = "log";

#[derive(Clone)]
struct LogPointer {
    pos: Arc<AtomicU64>,
    size: u64,
    log: Arc<AtomicU64>,
    log_state: Arc<AtomicU8>,
}

/// Key Value struct
///
/// @TODO create one buffer for reading

#[derive(Clone)]
pub struct LogStructKVStore {
    log_writer: Arc<Mutex<BufWriter<File>>>,
    index: Arc<RwLock<HashMap<String, LogPointer>>>,
    path: Arc<PathBuf>,
    log: Arc<AtomicU64>,
    log_counter: Arc<AtomicU64>,
}

impl KvsEngine for LogStructKVStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let set_cmd = Command::Set { key, value };

        let mut log_writer = self.log_writer.lock().unwrap();
        let pos_before = log_writer.stream_position()?;
        bincode::serialize_into(&mut *log_writer, &set_cmd)?;
        log_writer.flush()?;
        let pos_after = log_writer.stream_position()?;

        if let Command::Set { key, value: _ } = set_cmd {
            let mut index = self.index.write().unwrap();
            index.insert(
                key,
                LogPointer {
                    pos: Arc::new(AtomicU64::new(pos_before)),
                    size: pos_after - pos_before,
                    log: Arc::new(AtomicU64::new(self.log.load(Ordering::Relaxed))),
                    log_state: Arc::new(AtomicU8::new(LOG_WRITE)),
                },
            );
        }

        if pos_after >= MAX_FILE_SIZE {
            self.compact_logs(log_writer)?;
        }
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let index = self.index.read().unwrap();
        if !index.contains_key(&key) {
            return Ok(None);
        }

        let log = index.get(&key).unwrap();
        let mut reader = create_file_reader(&self.generate_full_log_path(
            &log.log.load(Ordering::Relaxed),
            &log.log_state.load(Ordering::Relaxed),
        )?)?;
        reader.seek(SeekFrom::Start(log.pos.load(Ordering::Relaxed)))?;
        match bincode::deserialize_from(&mut reader)? {
            Command::Set { key: _, value } => Ok(Some(value)),
            _ => Err(KvsError::UnexpectedCommandType),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        if !self.index.read().unwrap().contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        let cmd = Command::Rm { key };
        let mut log_writer = self.log_writer.lock().unwrap();
        bincode::serialize_into(&mut *log_writer, &cmd)?;
        log_writer.flush()?;

        if let Command::Rm { key } = cmd {
            self.index.write().unwrap().remove(&key);
        }

        if log_writer.stream_position()? >= MAX_FILE_SIZE {
            self.compact_logs(log_writer)?;
        }
        Ok(())
    }
}

impl LogStructKVStore {
    pub fn open(path: &Path) -> Result<LogStructKVStore> {
        let filenames = get_sorted_log_files(path);
        let current_folder = PathBuf::from(path);

        let mut log_counter = 0u64;
        for filename in filenames.iter() {
            let (log, _) = parse_filename(filename)?;
            log_counter = max(log_counter, log);
        }

        let log_counter = Arc::new(AtomicU64::new(log_counter));

        let log_filename = if filenames.is_empty() {
            current_folder.join(create_new_filename(LOG_WRITE, &log_counter)?)
        } else {
            filenames.last().unwrap().to_path_buf()
        };

        let log_writer = Arc::new(Mutex::new(create_file_writer(&log_filename)?));
        let (log, _) = parse_filename(&log_filename)?;

        let index = Arc::new(RwLock::new(build_index(&filenames)?));

        Ok(LogStructKVStore {
            log_writer,
            index,
            path: Arc::new(current_folder),
            log: Arc::new(AtomicU64::new(log)),
            log_counter,
        })
    }

    /// Compact logs
    /// Iterates over index and save latest commands in the newly generatd log files
    /// Redundant are removed

    fn compact_logs(&self, mut log_writer: MutexGuard<BufWriter<File>>) -> Result<()> {
        let current_folder = &self.path;
        let old_files = get_sorted_log_files(current_folder);

        let current_log = self.log_counter.fetch_add(1, Ordering::Relaxed);
        self.log.store(current_log, Ordering::Relaxed);
        *log_writer = create_file_writer(&self.generate_full_log_path(&current_log, &LOG_WRITE)?)?;

        {
            let mut comp_log = self.log_counter.fetch_add(1, Ordering::Relaxed);
            let mut comp_writer =
                create_file_writer(&self.generate_full_log_path(&comp_log, &LOG_COMP)?)?;

            let index = self.index.read().unwrap();
            for (_, log_pointer) in index.iter() {
                let mut buf = vec![0u8; log_pointer.size as usize];

                let mut current_reader = create_file_reader(&self.generate_full_log_path(
                    &log_pointer.log.load(Ordering::Relaxed),
                    &log_pointer.log_state.load(Ordering::Relaxed),
                )?)?;

                current_reader.seek(SeekFrom::Start(log_pointer.pos.load(Ordering::Relaxed)))?;
                current_reader.read_exact(&mut buf)?;

                log_pointer
                    .pos
                    .store(comp_writer.stream_position()?, Ordering::Relaxed);
                log_pointer.log.store(comp_log, Ordering::Relaxed);
                log_pointer.log_state.store(LOG_COMP, Ordering::Relaxed);

                comp_writer.write_all(&buf)?;
                if comp_writer.stream_position()? > MAX_FILE_SIZE {
                    comp_log = self.log_counter.fetch_add(1, Ordering::Relaxed);
                    comp_writer =
                        create_file_writer(&self.generate_full_log_path(&comp_log, &LOG_COMP)?)?;
                }
            }
        }
        for filename in old_files.iter() {
            fs::remove_file(&filename)?;
        }
        Ok(())
    }

    fn generate_full_log_path(&self, log: &u64, log_state: &u8) -> Result<PathBuf> {
        Ok(self
            .path
            .join(format!("{}{}.{}", get_state_flag(log_state), log, LOG_EXT)))
    }
}

fn get_state_flag(state: &u8) -> &str {
    match state {
        &LOG_WRITE => WRITE_FLAG,
        &LOG_FULL => FULL_FLAG,
        &LOG_COMP => COMP_FLAG,
        _ => "",
    }
}
/// Generates new log filename with given `state`
fn create_new_filename(state: u8, log_counter: &AtomicU64) -> Result<String> {
    let file_counter = log_counter.load(Ordering::Relaxed);
    let filename = format!("{}{}.{}", get_state_flag(&state), file_counter, LOG_EXT);
    log_counter.store(file_counter + 1, Ordering::Relaxed);
    Ok(filename)
}

/// Builds index from all the log files
fn build_index(filenames: &[PathBuf]) -> Result<HashMap<String, LogPointer>> {
    let mut index = HashMap::<String, LogPointer>::new();

    for filename in filenames {
        let mut reader = create_file_reader(filename)?;
        let mut log_position = reader.stream_position()?;
        let (log, log_state) = parse_filename(filename)?;
        while let Ok(cmd) = bincode::deserialize_from(&mut reader) {
            match cmd {
                Command::Set { key, value: _ } => index.insert(
                    key,
                    LogPointer {
                        pos: Arc::new(AtomicU64::new(log_position)),
                        size: reader.stream_position()? - log_position,
                        log: Arc::new(AtomicU64::new(log)),
                        log_state: Arc::new(AtomicU8::new(log_state)),
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

fn parse_filename(path: &Path) -> Result<(u64, u8)> {
    let fullname = path.file_name().unwrap().to_str().unwrap();
    let log_state = match &fullname[0..1] {
        WRITE_FLAG => LOG_WRITE,
        FULL_FLAG => LOG_FULL,
        COMP_FLAG => LOG_COMP,
        _ => LOG_WRITE,
    };
    let log_id = fullname[1..fullname.len() - LOG_EXT.len() - 1]
        .parse::<u64>()
        .unwrap();
    Ok((log_id, log_state))
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
