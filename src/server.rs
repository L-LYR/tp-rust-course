use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::{KvsEngine, Result};

pub struct KvsServer<E>
where
    E: KvsEngine,
{
    engine: E,
    logger: slog::Logger,
}

impl<E> KvsServer<E>
where
    E: KvsEngine,
{
    pub fn new(engine: E, logger: slog::Logger) -> Self {
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
        todo!()
    }
}
