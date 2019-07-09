#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion, ParameterizedBenchmark};
use kvs::engine::kv::KvStore;
use kvs::engine::sled::SledKvsEngine;
use kvs::engine::KvsEngine;
use rand::prelude::*;
use sled::Db;
use std::iter;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

fn kv(store: &mut dyn KvsEngine) {
    for j in 1..10 {
        for i in 1..1024 {
            store.set(format!("key{}", i), "value".to_string()).unwrap();
        }
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let temp_dir = Path::new("/tmp/db");
    let mut store = KvStore::open(temp_dir.to_path_buf()).expect("unable to open kvstore");
    c.bench_function("kvstore", move |b| b.iter(|| kv(&mut store)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
