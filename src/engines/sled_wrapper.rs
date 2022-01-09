use crate::{KvsEngine, KvsError, Result};
use sled::Db;

#[derive(Clone)]
pub struct SledWrapper(Db);

impl KvsEngine for SledWrapper {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.0.insert(key.as_bytes(), value.as_bytes())?;
        self.0.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value) = self.0.get(key)? {
            Ok(Some(String::from_utf8(value.as_ref().to_vec())?))
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.0.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.0.flush()?;
        Ok(())
    }
}
