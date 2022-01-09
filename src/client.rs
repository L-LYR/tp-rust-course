use std::{
    io::{BufReader, BufWriter, Write},
    net::{TcpStream, ToSocketAddrs},
};

use serde::Deserialize;
use serde_json::{self, de::IoRead, Deserializer};

use crate::{
    common::{GetResponse, RemoveResponse, Request, SetResponse},
    KvsError::ServerErrorMessage,
    Result,
};

pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn connect<T>(addr: T) -> Result<Self>
    where
        T: ToSocketAddrs,
    {
        let tcp_reader = TcpStream::connect(addr)?;
        let tcp_writer = tcp_reader.try_clone()?;
        Ok(KvsClient {
            reader: Deserializer::from_reader(BufReader::new(tcp_reader)),
            writer: BufWriter::new(tcp_writer),
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;
        match SetResponse::deserialize(&mut self.reader)? {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(s) => Err(ServerErrorMessage(s)),
        }
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;
        match GetResponse::deserialize(&mut self.reader)? {
            GetResponse::Ok(s) => Ok(s),
            GetResponse::Err(s) => Err(ServerErrorMessage(s)),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;
        match RemoveResponse::deserialize(&mut self.reader)? {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(s) => Err(ServerErrorMessage(s)),
        }
    }
}
