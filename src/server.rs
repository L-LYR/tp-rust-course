use std::{
    io::{BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

use serde_json::Deserializer;

use crate::{
    common::{GetResponse, RemoveResponse, Request, SetResponse},
    KvsEngine, Result,
};

pub struct KvsServer<'a, E>
where
    E: KvsEngine,
{
    engine: E,
    logger: &'a slog::Logger,
}

impl<'a, E> KvsServer<'a, E>
where
    E: KvsEngine,
{
    pub fn new(engine: E, logger: &'a slog::Logger) -> Self {
        Self { engine, logger }
    }

    pub fn run<T>(&mut self, addr: &T) -> Result<()>
    where
        T: ToSocketAddrs,
    {
        for stream in TcpListener::bind(addr)?.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        slog::error!(self.logger, "Error when serving client: {}", e)
                    }
                }
                Err(err) => {
                    slog::error!(self.logger, "connection failed: {}", err);
                }
            }
        }
        Ok(())
    }

    fn serve(&mut self, tcp_stream: TcpStream) -> Result<()> {
        let client_addr = tcp_stream.peer_addr()?;
        let reader = BufReader::new(&tcp_stream);
        let mut writer = BufWriter::new(&tcp_stream);

        macro_rules! send_resp {
            ($resp:expr) => {{
                let resp = $resp;
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
                slog::debug!(
                    self.logger,
                    "send response to {}, details: {:?}",
                    client_addr,
                    resp
                );
            }};
        }

        for request in Deserializer::from_reader(reader).into_iter() {
            match request? {
                Request::Get { key } => {
                    send_resp!(match self.engine.get(key) {
                        Ok(value) => GetResponse::Ok(value),
                        Err(e) => GetResponse::Err(e.to_string()),
                    })
                }
                Request::Set { key, value } => {
                    send_resp!(match self.engine.set(key, value) {
                        Ok(_) => SetResponse::Ok(()),
                        Err(e) => SetResponse::Err(e.to_string()),
                    })
                }
                Request::Remove { key } => {
                    send_resp!(match self.engine.remove(key) {
                        Ok(_) => RemoveResponse::Ok(()),
                        Err(e) => RemoveResponse::Err(e.to_string()),
                    })
                }
            }
        }

        Ok(())
    }
}
