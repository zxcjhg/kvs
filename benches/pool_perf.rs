use assert_cmd::prelude::*;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, black_box};
use kvs::client::KvsClient;
use std::process;

use kvs::common::Command;
use kvs::engine::*;
use kvs::server::KvsServer;
use kvs::thread_pool::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand_pcg::Pcg64;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
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
        .measurement_time(Duration::from_millis(4000))
        .warm_up_time(Duration::from_millis(1));

    for i in [4, 8].iter() {
        let temp_dir = TempDir::new().unwrap();
        let mut pool = RayonThreadPool::new(i.clone() as u32).unwrap();
        let mut kv_store = SledStore::open(temp_dir.path()).unwrap();

        let mut keys = Vec::new();
        let mut values = Vec::new();

        for i in (0..1000).step_by(2) {
            keys.push(generate_random_string(i));
            values.push(generate_random_string(i + 1));
        }

        group.bench_function(
            BenchmarkId::from_parameter(format!("Num cpus: #{}", i)),
            |b| {
                b.iter(black_box(|| {

                    for i in 0..keys.len() {
                        let key = keys.pop().unwrap();
                        let value = values.pop().unwrap();
                        let kv_store = kv_store.clone();
                        pool.spawn(move || {
                            kv_store.set(key, value);
                        });
                    }
                }));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, pool_set);
criterion_main!(benches);
