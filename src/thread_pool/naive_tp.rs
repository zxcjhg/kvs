use crate::thread_pool::ThreadPool;
use crate::common::Result;
use std::thread;

pub struct NaiveThreadPool {}

impl ThreadPool for NaiveThreadPool {
    fn new(_: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(NaiveThreadPool{})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let t = thread::spawn(|| {
            job();
        });
        t.join().unwrap();
    }
}
