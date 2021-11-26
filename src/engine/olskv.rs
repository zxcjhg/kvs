use crate::common::{Command, Result};
use crate::engine::KvsEngine;
use crate::error::KvsError;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::{SkipMap, SkipSet};
use std::cmp::max;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Size in bytes of redundant commands
const COMPACT_THRESHOLD: u64 = 2000000;
/// A flag in the log filename that is compacted and full
const COMP_FLAG: char = '#';
/// A flag in the log filename that is being written into
const WRITE_FLAG: char = '?';
/// Extension of a log file
const LOG_EXT: &str = "log";

#[derive(Clone, Debug, Copy)]
struct LogPointer {
    pos: u64,
    size: u64,
    log: u64,
    log_state: char,
}

struct LogWriter {
    writer: BufWriter<File>,
    log: u64,
    pos: u64,
}

impl LogWriter {
    fn new(folder: &Path, log: u64, log_state: char) -> Result<LogWriter> {
        let mut writer =
            create_file_writer(generate_full_log_path(folder, &log, &log_state)?.as_path())?;
        Ok(LogWriter {
            pos: writer.stream_position()?,
            writer,
            log,
        })
    }

    fn write_cmd(&mut self, cmd: &Command) -> Result<u64> {
        let pos_before = self.pos;
        bincode::serialize_into(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        self.pos = self.writer.stream_position()?;
        Ok(self.pos - pos_before)
    }

    fn write_buf(&mut self, buf: &[u8]) -> Result<u64> {
        let pos_before = self.pos;
        self.writer.write_all(buf)?;
        self.writer.flush()?;
        self.pos = self.writer.stream_position()?;
        Ok(self.pos - pos_before)
    }
}

struct LogReader {
    readers: SkipMap<(u64, char), File>,
    to_clean: SkipSet<(u64, char)>,
    folder: PathBuf,
}

impl LogReader {
    fn new(folder: PathBuf) -> Result<LogReader> {
        Ok(LogReader {
            folder,
            to_clean: SkipSet::new(),
            readers: SkipMap::new(),
        })
    }
    fn read_log(&self, log_pointer: &LogPointer) -> Result<Vec<u8>> {
        let entry = self.readers.get_or_insert(
            (log_pointer.log, log_pointer.log_state),
            File::open(generate_full_log_path(
                &self.folder,
                &log_pointer.log,
                &log_pointer.log_state,
            )?)?,
        );

        let reader = entry.value();
        let mut buf = vec![0u8; log_pointer.size as usize];
        reader.read_exact_at(&mut buf, log_pointer.pos)?;
        Ok(buf)
    }

    fn deserialize(&self, log_pointer: &LogPointer) -> Result<Command> {
        Ok(bincode::deserialize(&self.read_log(log_pointer)?)?)
    }

    fn read_log_clean_after(&self, log_pointer: &LogPointer) -> Result<Vec<u8>> {
        let buf = self.read_log(log_pointer)?;
        self.to_clean
            .insert((log_pointer.log, log_pointer.log_state));
        Ok(buf)
    }

    fn clean_up(&self) -> Result<()> {
        for log in self.to_clean.iter() {
            self.readers.remove(log.value());
        }
        self.to_clean.clear();
        Ok(())
    }
}

/// Optimized version of Log Structured Key Value Storage
/// 1) Change HashMap to SkipMap +
/// 2) Utilize pread +
/// 3) Optimize Compaction, create only one db file +
/// 4) Optimize log_pointer update with bit mask and atomics - failed T_T
/// 5) Implement PBufReader @TODO
/// 6) Separate thread for compaction
#[derive(Clone)]
pub struct OptLogStructKvs {
    log_writer: Arc<Mutex<LogWriter>>,
    key_dir: Arc<SkipMap<String, AtomicCell<LogPointer>>>,
    folder: Arc<PathBuf>,
    reader: Arc<LogReader>,
    log_counter: Arc<AtomicU64>,
    uncompacted_size: Arc<AtomicU64>,
    comp_lock: Arc<Mutex<()>>,
}

impl KvsEngine for OptLogStructKvs {
    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key, value };
        let log_pointer = {
            let mut log_writer = self.log_writer.lock().unwrap();
            LogPointer {
                pos: log_writer.pos,
                size: log_writer.write_cmd(&cmd)?,
                log: log_writer.log,
                log_state: WRITE_FLAG,
            }
        };

        let key = extract_key_from_cmd(cmd);
        let old_entry = self.key_dir.get(&key);
        if let Some(old_entry) = old_entry {
            old_entry.value().store(log_pointer);
            self.update_uncompacted_size(old_entry.value().load().size)?;
        } else {
            self.key_dir.insert(key, AtomicCell::new(log_pointer));
        }
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(entry) = self.key_dir.get(&key) {
            match self.reader.deserialize(&entry.value().load())? {
                Command::Set { key: _, value } => Ok(Some(value)),
                _ => Err(KvsError::UnexpectedCommandType),
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        if !self.key_dir.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        let cmd = Command::Rm { key };
        let size = {
            let mut log_writer = self.log_writer.lock().unwrap();
            log_writer.write_cmd(&cmd)?
        }; // Remove command not needed

        let key = extract_key_from_cmd(cmd);
        if let Some(old_entry) = self.key_dir.remove(&key) {
            self.update_uncompacted_size(old_entry.value().load().size + size)?;
        }

        Ok(())
    }
}

impl OptLogStructKvs {
    pub fn open(path: &Path) -> Result<OptLogStructKvs> {
        let filenames = get_sorted_log_files(path);
        let current_folder = PathBuf::from(path);

        let (key_dir, uncompacted_size, log_counter) = build_key_dir(&filenames)?;
        let key_dir = Arc::new(key_dir);
        let uncompacted_size = Arc::new(AtomicU64::new(uncompacted_size));
        let log = if filenames.is_empty() {
            log_counter
        } else {
            parse_filename(&filenames.last().unwrap().to_path_buf())?.0
        };
        let log_writer = Arc::new(Mutex::new(LogWriter::new(
            &current_folder,
            log,
            WRITE_FLAG,
        )?));
        let log_counter = Arc::new(AtomicU64::new(log_counter));
        log_counter.fetch_add(1, Ordering::Relaxed);

        Ok(OptLogStructKvs {
            reader: Arc::new(LogReader::new(current_folder.clone())?),
            log_writer,
            key_dir,
            folder: Arc::new(current_folder),
            log_counter,
            uncompacted_size,
            comp_lock: Arc::new(Mutex::new(())),
        })
    }
    /// Monitoring the number of bytes of redundant command logs
    /// If it hits threshold, merging launches
    fn update_uncompacted_size(&self, redundant_size: u64) -> Result<()> {
        let mut comp_thresh = self
            .uncompacted_size
            .fetch_add(redundant_size, Ordering::Release);
        comp_thresh += redundant_size;

        if comp_thresh >= COMPACT_THRESHOLD && self.comp_lock.try_lock().is_ok() {
            self.compact_logs()?;
        }
        Ok(())
    }

    fn get_new_log(&self) -> u64 {
        self.log_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Log compaction
    /// Creates a new log for writing
    /// Merges all the commands for a given key to one, saves to COMPACTED log
    /// Redundant commands and logs are removed

    fn compact_logs(&self) -> Result<()> {
        let old_files = get_sorted_log_files(&self.folder);
        let new_log = self.get_new_log();

        {
            let mut log_writer = self.log_writer.lock().unwrap();
            *log_writer = LogWriter::new(&self.folder, new_log, WRITE_FLAG)?;
        }

        let mut comp_log_writer = LogWriter::new(&self.folder, new_log, COMP_FLAG)?;

        for entry in self.key_dir.iter() {
            let log_pointer = entry.value();
            let buf = self.reader.read_log_clean_after(&log_pointer.load())?;
            comp_log_writer.write_buf(&buf)?;

            log_pointer.store(LogPointer {
                pos: comp_log_writer.pos,
                size: buf.len() as u64,
                log: comp_log_writer.log,
                log_state: COMP_FLAG,
            });
        }
        self.reader.clean_up()?;
        for filename in old_files.iter() {
            fs::remove_file(&filename)?;
        }
        self.uncompacted_size.store(0, Ordering::Relaxed);
        Ok(())
    }
}

fn generate_full_log_path(folder: &Path, log: &u64, log_state: &char) -> Result<PathBuf> {
    Ok(folder.join(format!("{}{}.{}", log_state, log, LOG_EXT)))
}

/// Recreates key dir from all the log files
fn build_key_dir(
    filenames: &[PathBuf],
) -> Result<(SkipMap<String, AtomicCell<LogPointer>>, u64, u64)> {
    let key_dir = SkipMap::<String, AtomicCell<LogPointer>>::new();
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
                        uncompacted_size += old_entry.value().load().size;
                    }
                    key_dir.insert(
                        key,
                        AtomicCell::new(LogPointer {
                            pos: log_position,
                            size: reader.stream_position()? - log_position,
                            log,
                            log_state,
                        }),
                    );
                }
                Command::Rm { key } => {
                    if let Some(old_entry) = key_dir.remove(&key) {
                        uncompacted_size += old_entry.value().load().size;
                        uncompacted_size += reader.stream_position()? - log_position;
                    }
                }
                _ => return Err(KvsError::UnexpectedCommandType),
            };
            log_position = reader.stream_position()?;
        }
    }
    Ok((key_dir, uncompacted_size, log_counter))
}
/// Parses to log and log state (WRITE, COMPACTED)
fn parse_filename(path: &Path) -> Result<(u64, char)> {
    let fullname = path.file_name().unwrap().to_str().unwrap();
    let log_id = fullname[1..fullname.len() - LOG_EXT.len() - 1]
        .parse::<u64>()
        .unwrap();
    Ok((log_id, fullname.chars().next().unwrap()))
}

fn create_file_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = OpenOptions::new().append(true).create(true).open(&path)?;
    let mut log_writer = BufWriter::new(file);
    log_writer.seek(SeekFrom::End(0))?;
    Ok(log_writer)
}
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

fn extract_key_from_cmd(cmd: Command) -> String {
    match cmd {
        Command::Rm { key } => key,
        Command::Get { key } => key,
        Command::Set { key, value: _ } => key,
    }
}
