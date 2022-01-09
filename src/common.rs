use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

#[derive(Debug, Deserialize, Serialize)]
pub enum SetResponse {
    Ok(()),
    Err(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum RemoveResponse {
    Ok(()),
    Err(String),
}
