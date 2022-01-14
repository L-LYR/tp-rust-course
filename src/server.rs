use std::{
    io::{BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

use serde_json::Deserializer;

use crate::{
    common::{GetResponse, RemoveResponse, Request, SetResponse},
    thread_pool::ThreadPool,
    KvsEngine, Result,
};

pub struct KvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    engine: E,
    pool: P,
}

impl<E, P> KvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    pub fn new(engine: E, pool: P) -> Self {
        Self { engine, pool }
    }

    pub fn run<T>(&mut self, addr: &T) -> Result<()>
    where
        T: ToSocketAddrs,
    {
        for stream in TcpListener::bind(addr)?.incoming() {
            let engine = self.engine.clone();
            self.pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = Self::serve(engine, stream) {
                        error!("Error when serving client: {}", e)
                    }
                }
                Err(err) => {
                    error!("Connection failed: {}", err);
                }
            })
        }
        Ok(())
    }

    fn serve(engine: E, tcp_stream: TcpStream) -> Result<()> {
        let client_addr = tcp_stream.peer_addr()?;
        let reader = BufReader::new(&tcp_stream);
        let mut writer = BufWriter::new(&tcp_stream);

        macro_rules! send_resp {
            ($resp:expr) => {{
                let resp = $resp;
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
                debug!("Send response to {}, details: {:?}", client_addr, resp);
            }};
        }

        for request in Deserializer::from_reader(reader).into_iter() {
            match request? {
                Request::Get { key } => {
                    send_resp!(match engine.get(key) {
                        Ok(value) => GetResponse::Ok(value),
                        Err(e) => GetResponse::Err(e.to_string()),
                    })
                }
                Request::Set { key, value } => {
                    send_resp!(match engine.set(key, value) {
                        Ok(_) => SetResponse::Ok(()),
                        Err(e) => SetResponse::Err(e.to_string()),
                    })
                }
                Request::Remove { key } => {
                    send_resp!(match engine.remove(key) {
                        Ok(_) => RemoveResponse::Ok(()),
                        Err(e) => RemoveResponse::Err(e.to_string()),
                    })
                }
            }
        }

        Ok(())
    }
}
