use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use kip_db::core::{KVStore, hash_kv::HashKvStore};

/// 基于Hash持久化内核的bench测试
fn kv_benchmark(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = HashKvStore::open(temp_dir.path()).unwrap();
    store.set("key1".to_string(), "value1".to_string()).unwrap();

    c.bench_function("get exist", |b|
        b.iter(|| {
            store.get("key1".to_string())
                .unwrap()
        }));

    c.bench_function("get not exist", |b|
        b.iter(|| {
            store.get("key2".to_string())
                .unwrap()
        }));

    c.bench_function("set value", |b|
        b.iter(|| {
            store.set("key3".to_string(), "value3".to_string())
                .unwrap();
        }));

    c.bench_function("remove not exist value", |b|
        b.iter(|| {
            match store.remove("key4".to_string()) {
                Ok(_) => {}
                Err(_) => {}
            };
        }));
}

criterion_group!(benches, kv_benchmark);
criterion_main!(benches);



