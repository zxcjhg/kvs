use assert_cmd::prelude::*;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::client::KvsClient;
use std::process;

use kvs::common::Command;
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
use std::rc::Rc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
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
    let mut group = c.benchmark_group("pool_set");
    group
        .measurement_time(Duration::from_millis(6000))
        .warm_up_time(Duration::from_millis(1));

    for i in [1, 2, 4, 6, 8] {
        let temp_dir = TempDir::new().unwrap();
        let mut kv_store = LogStructKVStore::open(temp_dir.path()).unwrap();
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("Num cpus: #{}", i)),
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
                        let mut pool = SharedQueueThreadPool::new(i.clone() as u32).unwrap();

                        (keys, values, pool)
                    },
                    |(mut keys, mut values, mut pool)| {
                        for i in 0..keys.len() {
                            let key = keys.pop().unwrap();
                            let value = values.pop().unwrap();
                            let kv_store = kv_store.clone();
                            pool.spawn(move || {
                                kv_store.set(key, value).unwrap();
                            });
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, pool_set);
criterion_main!(benches);
