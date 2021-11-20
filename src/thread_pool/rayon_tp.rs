use crate::thread_pool::ThreadPool;
use crate::common::Result;

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
        self.rayon.spawn(job);
    }
}
