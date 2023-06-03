use std::sync::atomic::{AtomicU64, Ordering};
use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;
use kip_db::kernel::KVStore;
use kip_db::kernel::lsm::lsm_kv::LsmStore;
use kip_db::kernel::sled_kv::SledStore;
use kip_db::kernel::Result;

/// 持久化内核的简易bench测试
fn kv_benchmark_with_store<T: KVStore>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let key1: Vec<u8> = encode_key("key1").unwrap();
    let key2: Vec<u8> = encode_key("key2").unwrap();
    let value1: Vec<u8> = encode_key("value1").unwrap();

    let store = rt.block_on(async {
        let store = T::open(temp_dir.path()).await.unwrap();
        // 用于get exist测试获取数据
        store.set(&key1, Bytes::copy_from_slice(value1.as_slice())).await.unwrap();
        store
    });

    c.bench_function(&store_name_with_test::<T>("get exist"), |b| {
        b.to_async(&rt).iter(|| {
            async {
                store.get(&key1).await
                    .unwrap()
            }
        })
    });

    c.bench_function(&store_name_with_test::<T>("get not exist"), |b|
        b.to_async(&rt).iter(|| {
            async {
                store.get(&key2).await
                    .unwrap()
            }
        }));

    // 使用count进行动态的数据名变更防止数据重复而导致不写入
    let seq_buf_set = AtomicU64::new(0);
    let value = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ
                            abcdefghijklmnopqrstuvwxyz
                            0123456789)(*&^%$#@!~".to_vec();
    c.bench_function(&store_name_with_test::<T>("set value"), |b|
        b.to_async(&rt).iter(|| {
            async {
                store.set(&seq_buf_set.fetch_add(1, Ordering::SeqCst).to_be_bytes(), Bytes::copy_from_slice(value.as_slice())).await
                    .unwrap();
            }
        }));

    let seq_buf_remove = AtomicU64::new(0);
    c.bench_function(&store_name_with_test::<T>("remove not exist value"), |b|
        b.to_async(&rt).iter(|| {
            async {
                let _ = store.remove(&seq_buf_remove.fetch_add(1, Ordering::SeqCst).to_be_bytes()).await;
            }
        }));
}

fn kv_benchmark(c: &mut Criterion) {
    kv_benchmark_with_store::<LsmStore>(c);
    kv_benchmark_with_store::<SledStore>(c);
}

fn store_name_with_test<T: KVStore>(test_name :& str) -> String {
    format!("{}: {}",T::name(), test_name)
}

criterion_group!(benches, kv_benchmark);
criterion_main!(benches);

// 测试用序列化方法
fn encode_key(key: &str) -> Result<Vec<u8>>{
    Ok(bincode::serialize(key)?)
}

