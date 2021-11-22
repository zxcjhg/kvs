use crate::common::{Command, Result};
use crate::engine::KvsEngine;
use crate::error::KvsError;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use std::cmp::max;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Max log file size
const MAX_FILE_SIZE: u64 = 20000;
/// Size in bytes of redundant commands
const COMPACT_THRESHOLD: u64 = 2000000;
/// A flag in the log filename that is not compacted, but full
const FULL_FLAG: &str = "!";
/// A flag in the log filename that is compacted and full
const COMP_FLAG: &str = "#";
/// A flag in the log filename that is being written into
const WRITE_FLAG: &str = "?";
// @TODO convert to enum
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

impl LogPointer {
    fn new(pos: u64, size: u64, log: u64, log_state: u8) -> Result<LogPointer> {
        Ok(LogPointer {
            pos: Arc::new(AtomicU64::new(pos)),
            size,
            log: Arc::new(AtomicU64::new(log)),
            log_state: Arc::new(AtomicU8::new(log_state)),
        })
    }
}
/// Key Value struct

#[derive(Clone)]
pub struct OptLogStructKvs {
    log_writer: Arc<Mutex<BufWriter<File>>>,
    key_dir: Arc<SkipMap<String, LogPointer>>,
    path: Arc<PathBuf>,
    log: Arc<AtomicU64>,
    log_counter: Arc<AtomicU64>,
    uncompacted_size: Arc<AtomicU64>,
}

impl KvsEngine for OptLogStructKvs {
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut log_writer = self.log_writer.lock().unwrap();
        let pos_before = log_writer.stream_position()?;
        let set_cmd = Command::Set { key, value };
        bincode::serialize_into(&mut *log_writer, &set_cmd)?;
        log_writer.flush()?;
        let pos_after = log_writer.stream_position()?;

        if let Command::Set { key, value: _ } = set_cmd {
            let old_entry = self.key_dir.get(&key);
            self.key_dir.insert(
                key,
                LogPointer::new(
                    pos_before,
                    pos_after - pos_before,
                    self.log.load(Ordering::Relaxed),
                    LOG_WRITE,
                )?,
            );
            self.update_uncompacted_size(old_entry, log_writer)?;
        }

        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let entry = self.key_dir.get(&key);
        return if let Some(entry) = entry {
            let log_pointer = entry.value();
            let mut reader = create_file_reader(&self.generate_full_log_path(
                &log_pointer.log.load(Ordering::Relaxed),
                &log_pointer.log_state.load(Ordering::Relaxed),
            )?)?;
            reader.seek(SeekFrom::Start(log_pointer.pos.load(Ordering::Relaxed)))?;
            match bincode::deserialize_from(&mut reader)? {
                Command::Set { key: _, value } => Ok(Some(value)),
                _ => Err(KvsError::UnexpectedCommandType),
            }
        } else {
            Ok(None)
        };
    }

    fn remove(&self, key: String) -> Result<()> {
        if !self.key_dir.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        let cmd = Command::Rm { key };
        let mut log_writer = self.log_writer.lock().unwrap();
        bincode::serialize_into(&mut *log_writer, &cmd)?;
        log_writer.flush()?;

        if let Command::Rm { key } = cmd {
            let remove_result = self.key_dir.remove(&key);
            self.update_uncompacted_size(remove_result, log_writer)?;
        }

        Ok(())
    }
}

impl OptLogStructKvs {
    pub fn open(path: &Path) -> Result<OptLogStructKvs> {
        let filenames = get_sorted_log_files(path);
        let current_folder = PathBuf::from(path);

        let (key_dir, uncompacted_size, mut log_counter) = build_key_dir(&filenames)?;
        let key_dir = Arc::new(key_dir);
        let uncompacted_size = Arc::new(AtomicU64::new(uncompacted_size));
        let log_filename = if filenames.is_empty() {
            log_counter += 1;
            current_folder.join(format!("{}{}.{}", WRITE_FLAG, log_counter - 1, LOG_EXT))
        } else {
            filenames.last().unwrap().to_path_buf()
        };

        let log_writer = Arc::new(Mutex::new(create_file_writer(&log_filename)?));
        let (log, _) = parse_filename(&log_filename)?;
        let log_counter = Arc::new(AtomicU64::new(log_counter));

        Ok(OptLogStructKvs {
            log_writer,
            key_dir,
            path: Arc::new(current_folder),
            log: Arc::new(AtomicU64::new(log)),
            log_counter,
            uncompacted_size,
        })
    }

    fn update_uncompacted_size(
        &self,
        old_entry: Option<Entry<String, LogPointer>>,
        log_writer: MutexGuard<BufWriter<File>>,
    ) -> Result<()> {
        if let Some(old_entry) = old_entry {
            let old_log_pointer = old_entry.value();
            let mut comp_thresh = self
                .uncompacted_size
                .fetch_add(old_log_pointer.size, Ordering::Relaxed);
            comp_thresh += old_log_pointer.size;

            if comp_thresh >= COMPACT_THRESHOLD {
                self.compact_logs(log_writer)?;
            }
        }
        Ok(())
    }

    fn get_new_log(&self) -> u64 {
        self.log_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Compact logs
    /// Iterates over key_dir and save latest commands in the newly generatd log files
    /// Redundant are removed

    fn compact_logs(&self, mut log_writer: MutexGuard<BufWriter<File>>) -> Result<()> {
        let current_folder = &self.path;
        let old_files = get_sorted_log_files(current_folder);

        let current_log = self.get_new_log();
        self.log.store(current_log, Ordering::Relaxed);
        *log_writer = create_file_writer(&self.generate_full_log_path(&current_log, &LOG_WRITE)?)?;

        {
            let mut comp_log = self.get_new_log();
            let mut comp_writer =
                create_file_writer(&self.generate_full_log_path(&comp_log, &LOG_COMP)?)?;

            for entry in self.key_dir.iter() {
                let log_pointer = entry.value();
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
                    comp_log = self.get_new_log();
                    comp_writer =
                        create_file_writer(&self.generate_full_log_path(&comp_log, &LOG_COMP)?)?;
                }
            }
        }
        self.uncompacted_size.store(0, Ordering::Relaxed);
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
    match *state {
        LOG_WRITE => WRITE_FLAG,
        LOG_FULL => FULL_FLAG,
        LOG_COMP => COMP_FLAG,
        _ => "",
    }
}

/// Builds key_dir from all the log files
fn build_key_dir(filenames: &[PathBuf]) -> Result<(SkipMap<String, LogPointer>, u64, u64)> {
    let key_dir = SkipMap::<String, LogPointer>::new();
    let mut uncompacted_size = 0u64;
    let mut log_counter = 0u64;

    for filename in filenames {
        let mut reader = create_file_reader(filename)?;
        let mut log_position = reader.stream_position()?;
        let (log, log_state) = parse_filename(filename)?;
        log_counter = max(log_counter, log);
        while let Ok(cmd) = bincode::deserialize_from(&mut reader) {
            match cmd {
                Command::Set { key, value: _ } => {
                    if let Some(old_entry) = key_dir.get(&key) {
                        uncompacted_size += old_entry.value().size;
                    }
                    key_dir.insert(
                        key,
                        LogPointer::new(
                            log_position,
                            reader.stream_position()? - log_position,
                            log,
                            log_state,
                        )?,
                    );
                }
                Command::Rm { key } => {
                    if let Some(old_entry) = key_dir.remove(&key) {
                        uncompacted_size += old_entry.value().size;
                    }
                }
                _ => return Err(KvsError::UnexpectedCommandType),
            };
            log_position = reader.stream_position()?;
        }
    }
    Ok((key_dir, uncompacted_size, log_counter))
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
