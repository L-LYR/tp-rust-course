use crate::{
    engines::toy_bitcask::handle::{open, reader_of, writer_of, ReadHandle, WriteHandle},
    KvsError, Result,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
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

#[allow(dead_code)]
pub(crate) struct CommandMeta {
    pub file_id: u64,   // id of log file where command is saved
    pub size: u64,      // size of command
    pub position: u64,  // position of command in log file
    pub timestamp: i64, // last modified timestamp
}

impl From<(u64, u64, u64, i64)> for CommandMeta {
    fn from((file_id, value_pos, value_size, timestamp): (u64, u64, u64, i64)) -> Self {
        CommandMeta {
            file_id,
            size: value_size,
            position: value_pos,
            timestamp,
        }
    }
}

pub struct KvStore {
    // log files
    log: Log,
    // key dir
    key_dir: KeyDir,
}

impl KvStore {
    pub fn open<T: Into<PathBuf>>(dir: T) -> Result<KvStore> {
        let dir: PathBuf = dir.into();
        fs::create_dir_all(&dir)?;
        let mut read_handles = HashMap::new();
        let mut file_ids = list_log_file_in(&dir)?;
        let mut key_dir = KeyDir::default();
        file_ids.sort_unstable();
        for &id in &file_ids {
            let mut read_handle = reader_of(&dir.join(log_file_of(id)))?;
            key_dir.load_from(&id, &mut read_handle)?;
            read_handles.insert(id, read_handle);
        }
        let active_file_id = file_ids.last().unwrap_or(&0) + 1;
        let (write_handle, read_handle) = open(&dir.join(log_file_of(active_file_id)))?;
        read_handles.insert(active_file_id, read_handle);
        Ok(KvStore {
            log: Log {
                dir,
                active_file_id,
                read_handles,
                write_handle,
            },
            key_dir,
        })
    }
}

fn log_file_of(id: u64) -> String {
    format!("{}.log", id)
}

fn list_log_file_in(dir: &Path) -> Result<Vec<u64>> {
    Ok(fs::read_dir(&dir)?
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
        .collect())
}

impl KvStore {
    // The user invokes kvs set mykey myvalue
    // kvs creates a value representing the "set" command, containing its key and value
    // It then serializes that command to a String
    // It then appends the serialized command to a file containing the log
    // If that succeeds, it exits silently with error code 0
    // If it fails, it exits by printing the error and returning a non-zero error code

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let new_cmd = Command::set(key, value);
        let meta = self.log.append(&new_cmd)?.ok_or(KvsError::UnknownCommand)?;
        if let Command::Set { key, .. } = new_cmd {
            self.key_dir.insert(key, meta);
        }
        self.key_dir.maybe_compact(&mut self.log)?;
        Ok(())
    }

    // The user invokes kvs get mykey
    // kvs reads the entire log, one command at a time, recording the affected key and file offset of the command to an in-memory key -> log pointer map
    // It then checks the map for the log pointer
    // If it fails, it prints "Key not found", and exits with exit code 0
    // If it succeeds
    // It deserializes the command to get the last recorded value of the key
    // It prints the value to stdout and exits with exit code 0

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_meta) = self.key_dir.get(&key) {
            self.log.get_value(cmd_meta)
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

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.key_dir.contains(&key) {
            let new_cmd = Command::remove(key);
            self.log.append(&new_cmd)?;
            if let Command::Remove { key, .. } = new_cmd {
                self.key_dir.remove(&key);
            }
            self.key_dir.maybe_compact(&mut self.log)?;
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

struct Log {
    // directory
    dir: PathBuf,
    // id of current active log file
    active_file_id: u64,
    // file id -> reader handle
    read_handles: HashMap<u64, ReadHandle<File>>,
    // write handle of the current active log file
    write_handle: WriteHandle<File>,
}

impl Log {
    // append a command into the active log file
    fn append(&mut self, cmd: &Command) -> Result<Option<CommandMeta>> {
        let prev_pos = self.write_handle.pos;
        serde_json::to_writer(&mut self.write_handle, cmd)?;
        self.write_handle.flush()?;
        match cmd {
            Command::Set { timestamp, .. } => Ok(Some(
                (
                    self.active_file_id,
                    prev_pos,
                    self.write_handle.pos - prev_pos,
                    timestamp.to_owned(),
                )
                    .into(),
            )),
            Command::Remove { .. } => Ok(None),
            // _ => Err(KvsError::UnknownCommand),
        }
    }

    // get the read handle of the given meta
    fn get_read_handle(&mut self, meta: &CommandMeta) -> Result<&mut ReadHandle<File>> {
        self.read_handles
            .get_mut(&meta.file_id)
            .ok_or(KvsError::LogFileNotFound)
    }

    // get the raw command json string of the given meta
    fn get_raw_cmd(&mut self, meta: &CommandMeta) -> Result<String> {
        let handle = self.get_read_handle(meta)?;
        handle.seek(SeekFrom::Start(meta.position))?;
        let mut buf = String::new();
        handle.take(meta.size).read_to_string(&mut buf)?;
        Ok(buf)
    }

    // get the value of the given meta
    fn get_value(&mut self, meta: &CommandMeta) -> Result<Option<String>> {
        if let Command::Set { value, .. } = serde_json::from_str(self.get_raw_cmd(meta)?.as_str())?
        {
            Ok(Some(value))
        } else {
            Err(KvsError::UnknownCommand)
        }
    }

    // step means open a new active log file whose id is the previous id plus 2
    // the skipped one is for compaction
    fn step_for_compaction(&mut self) -> Result<(u64, WriteHandle<File>)> {
        let compaction_id = self.active_file_id + 1;
        self.active_file_id += 2;
        self.write_handle = writer_of(&self.dir.join(log_file_of(self.active_file_id)))?;
        let compaction_writer = writer_of(&self.dir.join(log_file_of(compaction_id)))?;
        Ok((compaction_id, compaction_writer))
    }

    // remove all stale log files whose id is less than compaction log id
    fn remove_stale_log_files(&mut self, compaction_id: u64) -> Result<()> {
        let stale_file_ids: Vec<_> = self
            .read_handles
            .keys()
            .filter(|&&id| -> bool { id < compaction_id })
            .cloned()
            .collect();
        for stale_file_id in stale_file_ids {
            self.read_handles.remove(&stale_file_id);
            fs::remove_file(self.dir.join(log_file_of(stale_file_id)))?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct KeyDir {
    // in-memory key dir
    key_to_meta: HashMap<String, CommandMeta>,
    // uncompacted log length
    uncompacted: u64,
}

impl KeyDir {
    // load key-to-meta from log files into in-memory hashmap
    fn load_from(&mut self, id: &u64, read_handle: &mut ReadHandle<File>) -> Result<()> {
        let mut pos = read_handle.seek(SeekFrom::Start(0))?;
        let mut iter = Deserializer::from_reader(read_handle).into_iter::<Command>();
        while let Some(cmd) = iter.next() {
            let new_pos = iter.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => {
                    let meta = (id.clone(), pos, new_pos - pos, Utc::now().timestamp()).into();
                    if let Some(old_meta) = self.key_to_meta.insert(key, meta) {
                        self.uncompacted += old_meta.size;
                    }
                }
                Command::Remove { key, .. } => {
                    if let Some(old_meta) = self.key_to_meta.remove(&key) {
                        self.uncompacted += old_meta.size;
                    }
                    self.uncompacted += new_pos - pos; // add 'remove' cmd itself which will be compacted next time
                }
            }
            pos = new_pos;
        }
        Ok(())
    }

    // insert a (key, meta) pair
    fn insert(&mut self, key: String, meta: CommandMeta) {
        if let Some(old_meta) = self.key_to_meta.insert(key, meta) {
            self.uncompacted += old_meta.size;
        }
    }

    // get the meta of the given key
    fn get(&self, key: &String) -> Option<&CommandMeta> {
        self.key_to_meta.get(key)
    }

    fn contains(&self, key: &String) -> bool {
        self.key_to_meta.contains_key(key)
    }

    fn remove(&mut self, key: &String) -> Option<CommandMeta> {
        let old_meta = self.key_to_meta.remove(key);
        // add 'remove' command length
        self.uncompacted += old_meta.as_ref().map_or(0, |meta| meta.size);
        old_meta
    }

    fn maybe_compact(&mut self, log: &mut Log) -> Result<()> {
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact_into(log)?;
        }
        Ok(())
    }

    fn compact_into(&mut self, log: &mut Log) -> Result<()> {
        let (compaction_id, mut compaction_writer) = log.step_for_compaction()?;
        let mut compacted_pos: u64 = 0;
        for old_meta in self.key_to_meta.values_mut() {
            let handle = log.get_read_handle(old_meta)?;
            if handle.pos != old_meta.position {
                handle.seek(SeekFrom::Start(old_meta.position))?;
            }
            let len = io::copy(&mut handle.take(old_meta.size), &mut compaction_writer)?;
            *old_meta = (compaction_id, compacted_pos, len, Utc::now().timestamp()).into();
            compacted_pos += len;
        }
        compaction_writer.flush()?;
        log.remove_stale_log_files(compaction_id)?;
        self.uncompacted = 0;
        Ok(())
    }
}
