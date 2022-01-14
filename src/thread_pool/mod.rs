use crate::Result;

pub use self::naive::NaiveThreadPool;
pub use self::rayon::RayonThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;

mod naive;
mod rayon;
mod shared_queue;

pub trait ThreadPool {
    fn new(n_thread: usize) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;
}
