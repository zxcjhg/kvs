use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::client::KvsClient;
use std::process;

use kvs::common::Command;
use kvs::common::{EngineType, Result};
use kvs::engine::*;
use kvs::server::KvsServer;
use kvs::thread_pool::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand_pcg::Pcg64;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

struct ThreadPoolHolder {
    sharedq: Option<SharedQueueThreadPool>,
    rayon: Option<rayon::ThreadPool>,
    tp_type: ThreadPoolType,
}

impl ThreadPoolHolder {
    fn new(tp_type: ThreadPoolType, num_threads: u32) -> Result<ThreadPoolHolder> {
        Ok(match tp_type {
            ThreadPoolType::SharedQ => ThreadPoolHolder {
                sharedq: Some(SharedQueueThreadPool::new(num_threads)?),
                rayon: None,
                tp_type,
            },
            ThreadPoolType::Rayon => ThreadPoolHolder {
                sharedq: None,
                rayon: Some(
                    rayon::ThreadPoolBuilder::new()
                        .num_threads(num_threads as usize)
                        .build()
                        .unwrap(),
                ),
                tp_type,
            },
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self.tp_type {
            ThreadPoolType::SharedQ => self.sharedq.as_ref().unwrap().spawn(job),
            ThreadPoolType::Rayon => self.rayon.as_ref().unwrap().spawn(job),
        }
    }
}

#[derive(Clone)]
struct EngineHolder {
    lkvs: Option<LogStructKVStore>,
    sled: Option<SledStore>,
    engine_type: EngineType,
}

impl EngineHolder {
    fn new(engine: &EngineType, path: &Path) -> Result<EngineHolder> {
        Ok(match engine {
            EngineType::Kvs => EngineHolder {
                lkvs: Some(LogStructKVStore::open(path).unwrap()),
                sled: None,
                engine_type: EngineType::Kvs,
            },
            EngineType::Sled => EngineHolder {
                lkvs: None,
                sled: Some(SledStore::open(path).unwrap()),
                engine_type: EngineType::Sled,
            },
        })
    }
    fn set(&self, key: String, value: String) -> Result<()> {
        match self.engine_type {
            EngineType::Kvs => self.lkvs.as_ref().unwrap().set(key, value),
            EngineType::Sled => self.sled.as_ref().unwrap().set(key, value),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        match self.engine_type {
            EngineType::Kvs => self.lkvs.as_ref().unwrap().remove(key),
            EngineType::Sled => self.sled.as_ref().unwrap().remove(key),
        }
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.engine_type {
            EngineType::Kvs => self.lkvs.as_ref().unwrap().get(key),
            EngineType::Sled => self.sled.as_ref().unwrap().get(key),
        }
    }
}

fn generate_random_string(seed: u64) -> String {
    let mut rng = Pcg64::seed_from_u64(seed);

    let len: usize = rng.gen_range(1..10000);
    let s: String = rng
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect();

    return s;
}

fn pool_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set");
    group
        .measurement_time(Duration::from_millis(6000))
        .warm_up_time(Duration::from_millis(1));

    for engine_type in [EngineType::Sled, EngineType::Kvs] {
        for pool_type in [ThreadPoolType::Rayon, ThreadPoolType::SharedQ] {
            for i in [1, 2, 4, 6, 8] {
                let temp_dir = TempDir::new().unwrap();
                let mut kv_store = EngineHolder::new(&engine_type, temp_dir.path()).unwrap();
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!(
                        "Engine: {}, Pool: {:?}, Num cpus: #{}",
                        &engine_type, &pool_type, i
                    )),
                    &(i, kv_store),
                    |b, (i, kv_store)| {
                        b.iter_batched(
                            || {
                                let mut keys = Vec::new();
                                let mut values = Vec::new();
                                let mut rng = thread_rng();

                                for _ in 0..100 {
                                    keys.push(rng.gen_range(0..100).to_string());
                                    values.push(rng.gen_range(0..100).to_string());
                                }
                                let pool =
                                    ThreadPoolHolder::new(pool_type.clone(), i.clone() as u32)
                                        .unwrap();

                                (keys, values, pool, pool_type.clone())
                            },
                            |(mut keys, mut values, pool, pool_type)| match pool_type {
                                ThreadPoolType::SharedQ => {
                                    for _ in 0..keys.len() {
                                        let key = keys.pop().unwrap();
                                        let value = values.pop().unwrap();
                                        let kv_store = kv_store.clone();
                                        pool.spawn(move || {
                                            kv_store.set(key, value).unwrap();
                                        });
                                    }
                                }
                                ThreadPoolType::Rayon => rayon::scope(|x| {
                                    for _ in 0..keys.len() {
                                        let key = keys.pop().unwrap();
                                        let value = values.pop().unwrap();
                                        let kv_store = kv_store.clone();
                                        x.spawn(move |_| {
                                            kv_store.set(key, value).unwrap();
                                        });
                                    }
                                }),
                            },
                            BatchSize::SmallInput,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

fn pool_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    group
        .measurement_time(Duration::from_millis(6000))
        .warm_up_time(Duration::from_millis(1));

    for engine_type in [EngineType::Kvs, EngineType::Sled] {
        let temp_dir = TempDir::new().unwrap();
        let mut kv_store = EngineHolder::new(&engine_type, temp_dir.path()).unwrap();
        for i in 0..10000 {
            kv_store.set(i.to_string(), i.to_string());
        }
        for pool_type in [ThreadPoolType::Rayon, ThreadPoolType::SharedQ] {
            for i in [1, 2, 4, 6, 8] {
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!(
                        "Engine: {}, Pool: {:?}, Num cpus: #{}",
                        &engine_type, &pool_type, i
                    )),
                    &i,
                    |b, i| {
                        b.iter_batched(
                            || {
                                let mut rng = thread_rng();

                                let mut data = Vec::new();
                                for _ in 0..100 {
                                    data.push(rng.gen_range(0..10000).to_string());
                                }

                                let pool =
                                    ThreadPoolHolder::new(pool_type.clone(), i.clone() as u32)
                                        .unwrap();

                                (data, pool, pool_type.clone())
                            },
                            |(mut data, pool, pool_type)| {
                                for key in data {
                                    let kv_store = kv_store.clone();
                                    pool.spawn(move || {
                                        assert_eq!(
                                            key.clone(),
                                            kv_store.get(key).unwrap().unwrap()
                                        );
                                    });
                                }
                            },
                            BatchSize::SmallInput,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, pool_set);
criterion_main!(benches);
