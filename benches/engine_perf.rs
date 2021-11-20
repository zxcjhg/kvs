use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::common::{Result, EngineType};
use kvs::engine::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand_pcg::Pcg64;
use std::path::PathBuf;
use tempfile::TempDir;


#[derive(Clone)]
struct EngineHolder {
    lkvs: Option<LogStructKVStore>,
    sled: Option<SledStore>,
    engine_type: EngineType,
}

impl EngineHolder {
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
    let len: usize = rng.gen_range(1..10);
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
                    let mut kv_store = match engine {
                        EngineType::Kvs => EngineHolder {
                            lkvs: Some(LogStructKVStore::open(&temp_dir.path()).unwrap()),
                            sled: None,
                            engine_type: EngineType::Kvs,
                        },
                        EngineType::Sled => EngineHolder {
                            lkvs: None,
                            sled: Some(SledStore::open(&temp_dir.path()).unwrap()),
                            engine_type: EngineType::Sled,
                        },
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
                    let mut kv_store = match engine {
                        EngineType::Kvs => EngineHolder {
                            lkvs: Some(LogStructKVStore::open(&temp_dir.path()).unwrap()),
                            sled: None,
                            engine_type: EngineType::Kvs,
                        },
                        EngineType::Sled => EngineHolder {
                            lkvs: None,
                            sled: Some(SledStore::open(&temp_dir.path()).unwrap()),
                            engine_type: EngineType::Sled,
                        },
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
