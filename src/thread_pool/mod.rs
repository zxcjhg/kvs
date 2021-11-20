use crate::common::Result;

use clap::ArgEnum;
use serde::{Deserialize, Serialize};

mod naive_tp;
mod rayon_tp;
mod sharedq_tp;
pub use naive_tp::NaiveThreadPool;
pub use rayon_tp::RayonThreadPool;
pub use sharedq_tp::SharedQueueThreadPool;

pub trait ThreadPool {
    fn new(num_threads: u32) -> Result<Self>
    where
        Self: Sized;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

#[derive(ArgEnum, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ThreadPoolType {
    #[clap(alias = "rayon")]
    Rayon,
    #[clap(alias = "sharedq")]
    SharedQ,
}
