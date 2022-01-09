use crate::Result;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

pub(crate) struct ReadHandle<R>
where
    R: Seek + Read,
{
    buf_reader: BufReader<R>,
    pub pos: u64,
}

impl<R> ReadHandle<R>
where
    R: Seek + Read,
{
    pub(crate) fn new(reader: R) -> Self {
        Self {
            buf_reader: BufReader::new(reader),
            pos: 0,
        }
    }
}

impl<R> Read for ReadHandle<R>
where
    R: Seek + Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.buf_reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R> Seek for ReadHandle<R>
where
    R: Seek + Read,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.buf_reader.seek(pos)?;
        Ok(self.pos)
    }
}

pub(crate) struct WriteHandle<W>
where
    W: Seek + Write,
{
    buf_writer: BufWriter<W>,
    pub pos: u64,
}

impl<W> WriteHandle<W>
where
    W: Seek + Write,
{
    pub(crate) fn new(writer: W) -> Self {
        Self {
            buf_writer: BufWriter::new(writer),
            pos: 0,
        }
    }
}

impl<W> Write for WriteHandle<W>
where
    W: Seek + Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.buf_writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.buf_writer.flush()
    }
}

impl<W> Seek for WriteHandle<W>
where
    W: Seek + Write,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.buf_writer.seek(pos)?;
        Ok(self.pos)
    }
}

pub(crate) fn reader_of(path: &Path) -> Result<ReadHandle<File>> {
    Ok(ReadHandle::new(File::open(&path)?))
}

pub(crate) fn writer_of(path: &Path) -> Result<WriteHandle<File>> {
    Ok(WriteHandle::new(
        OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&path)?,
    ))
}

pub(crate) fn open(path: &Path) -> Result<(WriteHandle<File>, ReadHandle<File>)> {
    Ok((writer_of(&path)?, reader_of(&path)?))
}
