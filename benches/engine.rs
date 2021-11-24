use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::common::{EngineType, Result};
use kvs::engine::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand_pcg::Pcg64;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;
#[derive(Clone)]
struct EngineHolder {
    lkvs: Option<OptLogStructKvs>,
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
        let temp_dir = TempDir::new().unwrap();
        let mut kv_store = match engine {
            EngineType::Kvs => EngineHolder {
                lkvs: Some(OptLogStructKvs::open(temp_dir.path()).unwrap()),
                sled: None,
                engine_type: EngineType::Kvs,
            },
            EngineType::Sled => EngineHolder {
                lkvs: None,
                sled: Some(SledStore::open(temp_dir.path()).unwrap()),
                engine_type: EngineType::Sled,
            },
        };

        group.bench_with_input(
            BenchmarkId::from_parameter(engine),
            &kv_store,
            |b, kv_store| {
                b.iter_batched(
                    || {
                        let mut keys = Vec::new();
                        let mut values = Vec::new();

                        let mut rng = Pcg64::seed_from_u64(1);

                        for _ in 0..2000 {
                            keys.push(rng.gen_range(0..100).to_string());
                            values.push(rng.gen_range(0..100).to_string());
                        }

                        (kv_store, keys, values)
                    },
                    |(mut kv_store, mut keys, mut values)| {
                        for _ in 0..keys.len() {
                            kv_store.set(keys.pop().unwrap(), values.pop().unwrap());
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    for engine in [EngineType::Sled, EngineType::Kvs].iter() {
        let temp_dir = TempDir::new().unwrap();
        let mut kv_store = match engine {
            EngineType::Kvs => EngineHolder {
                lkvs: Some(OptLogStructKvs::open(&temp_dir.path()).unwrap()),
                sled: None,
                engine_type: EngineType::Kvs,
            },
            EngineType::Sled => EngineHolder {
                lkvs: None,
                sled: Some(SledStore::open(&temp_dir.path()).unwrap()),
                engine_type: EngineType::Sled,
            },
        };
        group.bench_with_input(
            BenchmarkId::from_parameter(engine),
            &kv_store,
            |b, kv_store| {
                b.iter_batched(
                    || {
                        let mut index = HashMap::<String, String>::new();
                        let mut rng = Pcg64::seed_from_u64(1);

                        for _ in 0..2000 {
                            let key = rng.gen_range(0..100).to_string();
                            let value = rng.gen_range(0..100).to_string();
                            index.insert(key.clone(), value.clone());

                            kv_store.set(key, value);
                        }

                        (kv_store, index)
                    },
                    |(mut kv_store, mut index)| {
                        for (key, value) in index.iter() {
                            assert_eq!(value.clone(), kv_store.get(key.clone()).unwrap().unwrap());
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}
criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
