use std::thread;

use crossbeam::channel::{Receiver, Sender};

use super::ThreadPool;
use crate::Result;

type Task = dyn FnOnce() + Send + 'static;

pub struct SharedQueueThreadPool {
    manager: Sender<Box<Task>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(n_thread: usize) -> Result<Self>
    where
        Self: Sized,
    {
        let (s, r) = crossbeam::channel::unbounded();

        for _ in 0..n_thread {
            let worker = Worker {
                receiver: r.clone(),
            };
            thread::Builder::new().spawn(|| register(worker))?;
        }

        Ok(SharedQueueThreadPool { manager: s })
    }

    fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Err(e) = self.manager.send(Box::new(task)) {
            error!("Fail to send task, cause {}", e);
        }
    }
}

#[derive(Clone)]
struct Worker {
    receiver: Receiver<Box<Task>>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            let new_worker = self.clone();
            if let Err(e) = thread::Builder::new().spawn(|| register(new_worker)) {
                error!("Fail to spawn a new worker, cause {}", e);
            }
        }
    }
}

fn register(w: Worker) {
    loop {
        match w.receiver.recv() {
            Ok(task) => task(),
            Err(_) => debug!("Destroyed!"),
        }
    }
}
