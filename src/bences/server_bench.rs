use std::sync::Arc;
use criterion::{Criterion, criterion_group, criterion_main};
use tokio::sync::RwLock;
use kip_db::KvStore;

fn kv_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = rt.block_on(async {
        let mut kv_store = KvStore::open("./tmp").await.unwrap();
        kv_store.set("key1".to_string(), "value1".to_string()).await.unwrap();
        Arc::new(RwLock::new(kv_store))
    });

    c.bench_function("get exist", |b| {
        b.to_async(&rt).iter(|| {
            async {
                store.read().await
                    .get("key1".to_string()).await
                    .unwrap();
            }
        })
    });

    c.bench_function("get not exist", |b| {
        b.to_async(&rt).iter(|| {
            async {
                store.read().await
                    .get("key2".to_string()).await
                    .unwrap();
            }
        })
    });

    c.bench_function("set value", |b| {
        b.to_async(&rt).iter(|| {
            async {
                store.write().await
                    .set("key3".to_string(), "value3".to_string()).await
                    .unwrap();
            }
        })
    });

    c.bench_function("remove not exist value", |b| {
        b.to_async(&rt).iter(|| {
            async {
                match store.write().await
                    .remove("key4".to_string()).await {
                    Ok(_) => {}
                    Err(_) => {}
                };
            }
        })
    });
}

criterion_group!(benches, kv_benchmark);
criterion_main!(benches);



