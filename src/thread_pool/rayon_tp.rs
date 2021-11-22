use crate::common::Result;
use crate::thread_pool::ThreadPool;

pub struct RayonThreadPool {
    rayon: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(num_threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonThreadPool {
            rayon: rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads as usize)
                .build()
                .unwrap(),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        rayon::scope(|_| self.rayon.spawn(job));
    }
}
