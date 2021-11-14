use crate::{Command, KvsEngine, KvsError, Result};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::SystemTime;
use bincode;

/// 2mb log file size, after that a new file is created
const COMP_THRESHOLD: u64 = 2000000;
/// A flag in the log filename that is not compacted, but full
const FULL_FLAG: &str = "!";
/// A flag in the log filename that is compacted and full
const COMP_FLAG: &str = "#";
/// A flag in the log filename that is being written into
const WRITE_FLAG: &str = "?";
/// Extension of a log file
const LOG_EXT: &str = "log";
const DELIMITER: u8 = b'\n';

enum LogState {
    Write,
    Full,
    Compacted,
}

struct LogPointer {
    pos: u64,
    reader: Rc<RefCell<BufReader<File>>>,
}

/// Key Value struct
///
/// @TODO create one buffer for reading
pub struct KvStore {
    current_writer: BufWriter<File>,
    index: HashMap<String, RefCell<LogPointer>>,
    current_reader: Rc<RefCell<BufReader<File>>>,
    current_log: PathBuf,
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let log_position = self.current_writer.stream_position()?;
        let set_cmd = Command::Set { key, value };
        bincode::serialize_into(&mut self.current_writer, &set_cmd)?;
        self.current_writer.write_all(b"\n")?;
        if let Command::Set { key, value: _ } = set_cmd {
            self.index.insert(
                key,
                RefCell::new(LogPointer {
                    pos: log_position,
                    reader: Rc::clone(&self.current_reader),
                }),
            );
        }
        self.current_writer.flush()?;
        self.compact_logs()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if !self.index.contains_key(&key) {
            return Ok(None);
        }

        let log_pointer = self.index.get(&key).unwrap();
        let current_pointer = log_pointer.borrow_mut();
        let mut reader = current_pointer.reader.borrow_mut();
        let log_position = current_pointer.pos;
        let mut temp_buffer = Vec::with_capacity(1000000);
        reader.seek(SeekFrom::Start(log_position))?;
        reader.read_until(DELIMITER, &mut temp_buffer)?;
        match bincode::deserialize(&temp_buffer)? {
            Command::Set { key: _, value } => Ok(Some(value)),
            _ => Err(KvsError::UnexpectedCommandType),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        self.index.remove(&key);
        bincode::serialize_into(&mut self.current_writer, &Command::Rm { key })?;
        self.current_writer.write_all(b"\n")?;
        self.compact_logs()?;
        Ok(())
    }
}

impl KvStore {
    /// Builds index from all the log files
    fn build_index(filenames: &[PathBuf]) -> Result<HashMap<String, RefCell<LogPointer>>> {
        let mut index = HashMap::<String, RefCell<LogPointer>>::new();
        let mut temp_buffer = Vec::with_capacity(10000000);

        for filename in filenames {
            let reader = KvStore::create_file_reader(filename)?;
            let mut reader_pointer = reader.borrow_mut();
            let mut log_position = reader_pointer.stream_position()?;
            temp_buffer.clear();
            while reader_pointer.read_until(DELIMITER, &mut temp_buffer)? > 0 {
                match bincode::deserialize(&temp_buffer)? {
                    Command::Set { key, value: _ } => index.insert(
                        key,
                        RefCell::new(LogPointer {
                            pos: log_position,
                            reader: Rc::clone(&reader),
                        }),
                    ),
                    Command::Rm { key } => index.remove(&key),
                    _ => return Err(KvsError::UnexpectedCommandType),
                };
                log_position = reader_pointer.stream_position()?;
                temp_buffer.clear();
            }
            reader_pointer.seek(SeekFrom::Start(0))?;
        }
        Ok(index)
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
        Ok(format!("{}_{}.{}", flag, timestamp, LOG_EXT))
    }

    pub fn open(path: &Path) -> Result<KvStore> {
        let filenames = KvStore::get_sorted_log_files(path);
        let mut current_log = PathBuf::from(path);

        if filenames.is_empty()
            || !filenames
                .last()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(WRITE_FLAG)
        {
            current_log.push(KvStore::create_new_filename(LogState::Write)?);
        } else {
            current_log.push(filenames.last().unwrap());
        }
        let current_writer = KvStore::create_file_writer(&current_log)?;
        let current_reader = KvStore::create_file_reader(&current_log)?;
        let index = KvStore::build_index(&filenames)?;
        Ok(KvStore {
            current_writer,
            index,
            current_reader,
            current_log,
        })
    }

    /// Compact logs
    /// Iterates over index and save latest commands in the newly generatd log files
    /// Redundant are removed

    fn compact_logs(&mut self) -> Result<()> {
        if self.current_writer.stream_position()? < COMP_THRESHOLD {
            return Ok(());
        }
        fs::rename(
            &self.current_log,
            self.current_log
                .to_str()
                .unwrap()
                .replace(WRITE_FLAG, FULL_FLAG),
        )?;
        self.current_log.pop();
        let mut current_path = PathBuf::from(&self.current_log);
        let log_filenames = KvStore::get_sorted_log_files(&current_path);
        self.current_log
            .push(KvStore::create_new_filename(LogState::Write)?);

        self.current_writer = KvStore::create_file_writer(&self.current_log)?;
        self.current_reader = KvStore::create_file_reader(&self.current_log)?;

        let mut temp_buffer = String::new();
        current_path.push(KvStore::create_new_filename(LogState::Compacted)?);
        let mut comp_writer = KvStore::create_file_writer(&current_path)?;
        let mut comp_reader = KvStore::create_file_reader(&current_path)?;

        for log_pointer in self.index.values() {
            temp_buffer.clear();
            let mut current_pointer = log_pointer.borrow_mut();

            current_pointer
                .reader
                .borrow_mut()
                .seek(SeekFrom::Start(current_pointer.pos))?;
            current_pointer
                .reader
                .borrow_mut()
                .read_line(&mut temp_buffer)?;
            current_pointer.pos = comp_writer.stream_position().unwrap();
            current_pointer.reader = Rc::clone(&comp_reader);

            comp_writer.write_all(temp_buffer.as_bytes())?;

            if comp_writer.stream_position()? > COMP_THRESHOLD {
                current_path.pop();
                current_path.push(KvStore::create_new_filename(LogState::Compacted)?);
                comp_writer = KvStore::create_file_writer(&current_path)?;
                comp_reader = KvStore::create_file_reader(&current_path)?;
            }
        }

        for filename in log_filenames.iter().filter(|x| {
            !x.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(&WRITE_FLAG)
        }) {
            current_path.pop();
            current_path.push(filename);
            fs::remove_file(&current_path)?;
        }
        Ok(())
    }
    /// Created a buffered writer for a given file
    fn create_file_writer(path: &Path) -> Result<BufWriter<File>> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let mut current_writer = BufWriter::new(file);
        current_writer.seek(SeekFrom::End(0))?;
        Ok(current_writer)
    }
    /// Created a buffered reader for a given file
    fn create_file_reader(path: &Path) -> Result<Rc<RefCell<BufReader<File>>>> {
        Ok(Rc::new(RefCell::new(BufReader::new(File::open(&path)?))))
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
}
