use rayon;

use super::ThreadPool;
use crate::Result;

pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(n_thread: usize) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonThreadPool(
            rayon::ThreadPoolBuilder::new()
                .num_threads(n_thread)
                .build()?,
        ))
    }

    fn spawn<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(task)
    }
}
