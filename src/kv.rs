use std::collections::HashMap;

#[derive(Default)]
pub struct KvStore {
    inner_store: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            inner_store: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.inner_store.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.inner_store.get(&key).cloned()
    }

    pub fn remove(&mut self, key: String) {
        self.inner_store.remove(&key);
    }
}
