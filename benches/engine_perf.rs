use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::common::EngineType;
use kvs::engine::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand_pcg::Pcg64;
use std::path::PathBuf;
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

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    for engine in [EngineType::Sled, EngineType::Kvs].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(engine), engine, |b, engine| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let mut kv_store: Box<dyn KvsEngine> = match engine {
                        EngineType::Sled => Box::new(SledStore::open(&temp_dir.path()).unwrap()),
                        EngineType::Kvs => {
                            Box::new(LogStructKVStore::open(&temp_dir.path()).unwrap())
                        }
                    };
                    (kv_store, temp_dir)
                },
                |(mut kv_store, temp_dir)| {
                    for seed in (0..200).step_by(2) {
                        let key = generate_random_string(seed);
                        let value = generate_random_string(seed + 1);
                        kv_store.set(key, value);
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    for engine in [EngineType::Sled, EngineType::Kvs].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(engine), engine, |b, engine| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let mut kv_store: Box<dyn KvsEngine> = match engine {
                        EngineType::Sled => Box::new(SledStore::open(&temp_dir.path()).unwrap()),
                        EngineType::Kvs => {
                            Box::new(LogStructKVStore::open(&temp_dir.path()).unwrap())
                        }
                    };
                    (kv_store, temp_dir)
                },
                |(mut kv_store, temp_dir)| {
                    for seed in (200..400).step_by(2) {
                        let key = generate_random_string(seed);
                        let value = generate_random_string(seed + 1);
                        kv_store.set(key, value);
                    }
                    for seed in (200..400).step_by(2) {
                        let key = generate_random_string(seed);
                        let value = generate_random_string(seed + 1);
                        assert_eq!(value, kv_store.get(key).unwrap().unwrap());
                    }
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}
criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
