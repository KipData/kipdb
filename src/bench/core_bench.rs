use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use kip_db::core::{KVStore, hash_kv::HashStore};
use kip_db::core::sled_kv::SledStore;

/// 基于Hash持久化内核的bench测试
fn kv_benchmark_with_store<T: KVStore>(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = T::open(temp_dir.path()).unwrap();
    store.set("key1".to_string(), "value1".to_string()).unwrap();

    c.bench_function(&*store_name_with_test::<T>("get exist"), |b|
        b.iter(|| {
            store.get("key1".to_string())
                .unwrap()
        }));

    c.bench_function(&*store_name_with_test::<T>("get not exist"), |b|
        b.iter(|| {
            store.get("key2".to_string())
                .unwrap()
        }));

    c.bench_function(&*store_name_with_test::<T>("set value"), |b|
        b.iter(|| {
            store.set("key3".to_string(), "value3".to_string())
                .unwrap();
        }));

    c.bench_function(&*store_name_with_test::<T>("remove not exist value"), |b|
        b.iter(|| {
            match store.remove("key4".to_string()) {
                Ok(_) => {}
                Err(_) => {}
            };
        }));
}

fn kv_benchmark(c: &mut Criterion) {
    kv_benchmark_with_store::<HashStore>(c);
    kv_benchmark_with_store::<SledStore>(c);
}

fn store_name_with_test<T: KVStore>(test_name :& str) -> String {
    format!("{}: {}",T::name(), test_name)
}

criterion_group!(benches, kv_benchmark);
criterion_main!(benches);



