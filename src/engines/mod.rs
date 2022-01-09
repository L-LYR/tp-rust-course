pub mod sled_wrapper;
pub mod toy_bitcask;

use std::{fs, path::PathBuf};

use crate::{EngineType, KvsError, Result};

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

pub fn engine_type_of(path: &PathBuf) -> Result<Option<EngineType>> {
    let type_marker = path.join("engine");
    if !type_marker.exists() {
        return Ok(None);
    }
    match fs::read_to_string(type_marker)?.as_str() {
        "kvs" => Ok(Some(EngineType::kvs)),
        "sled" => Ok(Some(EngineType::sled)),
        _ => Err(KvsError::UnknownEngineType),
    }
}
