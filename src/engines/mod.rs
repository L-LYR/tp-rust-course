pub mod sled_wrapper;
pub mod toy_bitcask;

use std::{fs, path::PathBuf};

use crate::{EngineType, KvsError, Result};

pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&self, key: String) -> Result<()>;
}

pub fn engine_type_of(path: &PathBuf) -> Result<Option<EngineType>> {
    let type_marker = path.join("engine");
    if !type_marker.exists() {
        info!("No engine marker");
        return Ok(None);
    }
    match fs::read_to_string(type_marker)?.as_str() {
        "kvs" => Ok(Some(EngineType::kvs)),
        "sled" => Ok(Some(EngineType::sled)),
        _ => Err(KvsError::UnknownEngineType),
    }
}

pub fn set_engine_type(path: &PathBuf, engine_type: &EngineType) -> Result<()> {
    fs::write(path.join("engine"), format!("{}", engine_type))?;
    Ok(())
}
