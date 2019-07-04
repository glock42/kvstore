#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion, ParameterizedBenchmark};
use kvs::engine::kv::KvStore;
use kvs::engine::sled::SledKvsEngine;
use kvs::engine::KvsEngine;
use rand::prelude::*;
use sled::Db;
use std::iter;
use std::sync::Arc;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let temp_dir = Arc::new(TempDir::new().expect("unable to create temporary working directory"));
    let thread_dir1 = temp_dir.clone();
    let thread_dir2 = temp_dir.clone();
    let bench = ParameterizedBenchmark::new(
        "kvs",
        move |b, _| {
            b.iter_batched(
                || KvStore::open(thread_dir1.path()).expect("unable to open kvstore"),
                |mut store| {
                    for i in 1..(1 << 12) {
                        store.set(format!("key{}", i), "value".to_string()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
    .with_function("sled", move |b, _| {
        b.iter_batched(
            || SledKvsEngine::open(thread_dir2.path()).unwrap(),
            |mut db| {
                for i in 1..(1 << 12) {
                    db.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    c.bench("set_bench", bench);
}

fn get_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            })
        },
        vec![8, 12, 16, 20],
    )
    .with_function("sled", |b, i| {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SledKvsEngine::open(temp_dir.path()).unwrap();
        for key_i in 1..(1 << i) {
            db.set(format!("key{}", key_i), "value".to_string())
                .unwrap();
        }
        let mut rng = SmallRng::from_seed([0; 16]);
        b.iter(|| {
            db.get(format!("key{}", rng.gen_range(1, 1 << i))).unwrap();
        })
    });
    c.bench("get_bench", bench);
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
