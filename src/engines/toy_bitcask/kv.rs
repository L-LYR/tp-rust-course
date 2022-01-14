use crate::{
    engines::toy_bitcask::handle::{open, reader_of, writer_of, ReadHandle, WriteHandle},
    KvsEngine, KvsError, Result,
};
use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::OsStr,
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024; // 1MB

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Command {
    Set {
        timestamp: i64,
        key: String,
        value: String,
    },
    Remove {
        timestamp: i64,
        key: String,
    },
}

impl Command {
    pub(crate) fn set(key: String, value: String) -> Command {
        return Self::Set {
            timestamp: Utc::now().timestamp(),
            key,
            value,
        };
    }
    pub(crate) fn remove(key: String) -> Command {
        return Self::Remove {
            timestamp: Utc::now().timestamp(),
            key,
        };
    }
}

#[derive(Debug)]
pub(crate) struct CommandMeta {
    pub file_id: u64,  // id of log file where command is saved
    pub position: u64, // position of command in log file
    pub size: u64,     // size of command
}

impl From<(u64, u64, u64)> for CommandMeta {
    fn from((file_id, value_pos, value_size): (u64, u64, u64)) -> Self {
        CommandMeta {
            file_id,
            position: value_pos,
            size: value_size,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct KvStore {
    dir: Arc<PathBuf>,
    // key dir
    key_dir: Arc<DashMap<String, CommandMeta>>,
    // log
    stable_log: StableLog,
    // writer
    active_log: Arc<Mutex<ActiveLog>>,
}

impl KvStore {
    pub fn open<T>(dir: T) -> Result<KvStore>
    where
        T: Into<PathBuf>,
    {
        let dir = Arc::new(dir.into());
        fs::create_dir_all(dir.as_ref())?;

        let file_ids = list_log_file_in(&dir)?;
        let key_dir: DashMap<String, CommandMeta> = DashMap::new();
        let active_file_id = file_ids.last().unwrap_or(&0) + 1;
        let mut read_handles = BTreeMap::new();
        let (write_handle, read_handle) = open(&dir.join(log_file_of(active_file_id)))?;
        read_handles.insert(active_file_id.clone(), read_handle);
        let mut uncompacted = 0;
        for &id in &file_ids {
            let mut read_handle = reader_of(&dir.join(log_file_of(id)))?;
            let mut pos = read_handle.seek(SeekFrom::Start(0))?;
            let mut iter = Deserializer::from_reader(&mut read_handle).into_iter::<Command>();
            while let Some(cmd) = iter.next() {
                let new_pos = iter.byte_offset() as u64;
                match cmd? {
                    Command::Set { key, .. } => {
                        let meta = (id.clone(), pos, new_pos - pos).into();
                        if let Some(old_meta) = key_dir.insert(key, meta) {
                            uncompacted += old_meta.size;
                        }
                    }
                    Command::Remove { key, .. } => {
                        if let Some((_, old_meta)) = key_dir.remove(&key) {
                            uncompacted += old_meta.size;
                        }
                        uncompacted += new_pos - pos; // add 'remove' cmd itself which will be compacted next time
                    }
                }
                pos = new_pos;
            }
            read_handles.insert(id, read_handle);
        }
        let key_dir = Arc::new(key_dir);
        let stable_log = StableLog {
            dir: Arc::clone(&dir),
            read_handles: RefCell::new(read_handles),
            compacted_id: Arc::new(AtomicU64::new(0)),
        };
        let active_log = ActiveLog {
            dir: Arc::clone(&dir),
            file_id: active_file_id,
            write_handle,
            key_dir: Arc::clone(&key_dir),
            uncompacted,
            stable_log: stable_log.clone(),
        };

        // read_handles.insert(active_file_id, read_handle);
        Ok(KvStore {
            dir: Arc::clone(&dir),
            key_dir: Arc::clone(&key_dir),
            stable_log,
            active_log: Arc::new(Mutex::new(active_log)),
        })
    }
}

fn log_file_of(id: u64) -> String {
    format!("{}.log", id)
}

fn list_log_file_in(dir: &Path) -> Result<Vec<u64>> {
    let mut log_file_ids: Vec<_> = fs::read_dir(&dir)?
        .flat_map(|entry| -> Result<PathBuf> { Ok(entry?.path()) })
        .filter(|file_path| -> bool {
            file_path.is_file() && file_path.extension() == Some("log".as_ref())
        })
        .filter_map(|file_path| -> Option<Option<u64>> {
            file_path
                .file_name()
                .and_then(OsStr::to_str)
                .map(|str| -> Option<u64> { str.trim_end_matches(".log").parse::<u64>().ok() })
        })
        .flatten()
        .collect();

    log_file_ids.sort_unstable();

    Ok(log_file_ids)
}

impl KvsEngine for KvStore {
    // The user invokes kvs set mykey myvalue
    // kvs creates a value representing the "set" command, containing its key and value
    // It then serializes that command to a String
    // It then appends the serialized command to a file containing the log
    // If that succeeds, it exits silently with error code 0
    // If it fails, it exits by printing the error and returning a non-zero error code

    fn set(&self, key: String, value: String) -> Result<()> {
        self.active_log.lock().unwrap().set(key, value)
    }

    // The user invokes kvs get mykey
    // kvs reads the entire log, one command at a time, recording the affected key and file offset of the command to an in-memory key -> log pointer map
    // It then checks the map for the log pointer
    // If it fails, it prints "Key not found", and exits with exit code 0
    // If it succeeds
    // It deserializes the command to get the last recorded value of the key
    // It prints the value to stdout and exits with exit code 0

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_meta) = self.key_dir.get(&key).as_deref() {
            self.stable_log.get_value(cmd_meta)
        } else {
            Ok(None)
        }
    }

    // The user invokes kvs rm mykey
    // Same as the "get" command, kvs reads the entire log to build the in-memory index
    // It then checks the map if the given key exists
    // If the key does not exist, it prints "Key not found", and exits with a non-zero error code
    // If it succeeds
    // It creates a value representing the "rm" command, containing its key
    // It then appends the serialized command to the log
    // If that succeeds, it exits silently with error code 0

    fn remove(&self, key: String) -> Result<()> {
        self.active_log.lock().unwrap().remove(key)
    }
}

struct ActiveLog {
    // active log file id
    pub file_id: u64,
    // log directory
    dir: Arc<PathBuf>,
    // write handle of active log file
    write_handle: WriteHandle<File>,
    // in-memory key dir
    key_dir: Arc<DashMap<String, CommandMeta>>,
    // uncompacted log length
    uncompacted: u64,
    // stable log
    stable_log: StableLog,
}

impl ActiveLog {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        // write in active log file
        let prev_pos = self.write_handle.pos;
        let new_cmd = Command::set(key.clone(), value);
        serde_json::to_writer(&mut self.write_handle, &new_cmd)?;
        self.write_handle.flush()?;
        // insert <key, meta> pair in keydir
        let meta: CommandMeta = (self.file_id, prev_pos, self.write_handle.pos - prev_pos).into();
        if let Some(old_meta) = self.key_dir.insert(key, meta) {
            self.uncompacted += old_meta.size;
        }
        // compaction
        if self.uncompacted >= COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        // check
        if self.key_dir.contains_key(&key) {
            // write in active log file
            let new_cmd = Command::remove(key.clone());
            serde_json::to_writer(&mut self.write_handle, &new_cmd)?;
            self.write_handle.flush()?;
            // remove <key, meta> pair from keydir
            if let Some((_, old_meta)) = self.key_dir.remove(&key) {
                self.uncompacted += old_meta.size;
            }
            // compaction
            if self.uncompacted >= COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn compact(&mut self) -> Result<()> {
        let compaction_id = self.file_id + 1;
        self.file_id += 2;
        self.write_handle = writer_of(&self.dir.join(log_file_of(self.file_id)))?;
        let mut compaction_writer = writer_of(&self.dir.join(log_file_of(compaction_id)))?;

        let mut compacted_pos: u64 = 0;
        for mut entry in self.key_dir.iter_mut() {
            let len = self.stable_log.locate_and(entry.value(), |mut handle| {
                Ok(io::copy(&mut handle, &mut compaction_writer)?)
            })?;
            *entry = (compaction_id, compacted_pos, len).into();
            // self.key_dir.insert(
            //     entry.key().clone(),
            //     (compaction_id, compacted_pos, len).into(),
            // );
            compacted_pos += len;
        }
        compaction_writer.flush()?;

        self.stable_log
            .compacted_id
            .store(compaction_id, Ordering::SeqCst);
        self.stable_log.remove_stale_log();

        let stale_log_file_ids: Vec<_> = list_log_file_in(self.dir.as_ref())?
            .into_iter()
            .filter(|&id| -> bool { id < compaction_id })
            .collect();

        for id in stale_log_file_ids {
            let log_file_path = self.dir.join(log_file_of(id));
            if let Err(e) = fs::remove_file(&log_file_path) {
                error!("{:?} cannot be removed, cause {}", log_file_path, e);
            }
        }

        self.uncompacted = 0;
        Ok(())
    }
}

struct StableLog {
    // directory
    dir: Arc<PathBuf>,

    // file id -> reader handle
    // Because our log file name is in ascending order, here we use BTreeMap instead.
    read_handles: RefCell<BTreeMap<u64, ReadHandle<File>>>,

    // compacted
    compacted_id: Arc<AtomicU64>,
}

impl StableLog {
    // get the value of the given meta
    fn get_value(&self, meta: &CommandMeta) -> Result<Option<String>> {
        self.locate_and(meta, |handle| {
            if let Command::Set { value, .. } = serde_json::from_reader(handle)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnknownCommand)
            }
        })
    }
    fn locate_and<F, R>(&self, meta: &CommandMeta, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut ReadHandle<File>>) -> Result<R>,
    {
        self.remove_stale_log();
        let mut read_handles = self.read_handles.borrow_mut();
        if !read_handles.contains_key(&meta.file_id) {
            let read_handle = reader_of(self.dir.join(log_file_of(meta.file_id)).as_path())?;
            read_handles.insert(meta.file_id, read_handle);
        }
        let handle = read_handles
            .get_mut(&meta.file_id)
            .ok_or(KvsError::LogFileNotFound)?;
        handle.seek(SeekFrom::Start(meta.position))?;
        f(handle.take(meta.size))
    }
    fn remove_stale_log(&self) {
        let mut read_handles = self.read_handles.borrow_mut();
        while let Some(&id) = read_handles.keys().next() {
            if id >= self.compacted_id.load(Ordering::SeqCst) {
                break;
            }
            read_handles.remove(&id);
        }
    }
}

impl Clone for StableLog {
    fn clone(&self) -> StableLog {
        StableLog {
            dir: Arc::clone(&self.dir),
            compacted_id: Arc::clone(&self.compacted_id),
            read_handles: RefCell::new(BTreeMap::new()),
        }
    }
}
