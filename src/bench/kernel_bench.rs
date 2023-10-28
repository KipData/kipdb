mod util;

use bytes::Bytes;
/// 参考Sled Benchmark
/// https://github.com/spacejam/sled/blob/main/benchmarks/criterion/benches/sled.rs
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

use crate::util::{counter, prepare_data, random, random_bytes};
use kip_db::kernel::lsm::storage::KipStorage;
use kip_db::kernel::Storage;

fn bulk_load<T: Storage>(c: &mut Criterion) {
    let count = AtomicU32::new(0_u32);
    let bytes = |len| -> Vec<u8> {
        count
            .fetch_add(1, Relaxed)
            .to_be_bytes()
            .into_iter()
            .cycle()
            .take(len)
            .collect()
    };

    let mut bench = |key_len, val_len| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();

        let db = rt.block_on(async {
            T::open(format!("{}_bulk_k{}_v{}", T::name(), key_len, val_len))
                .await
                .unwrap()
        });

        c.bench_function(
            &format!(
                "Store: {}, bulk load key/value lengths {}/{}",
                T::name(),
                key_len,
                val_len
            ),
            |b| {
                b.to_async(&rt).iter(|| async {
                    db.set(Bytes::from(bytes(key_len)), Bytes::from(bytes(val_len)))
                        .await
                        .unwrap();
                })
            },
        );
        rt.shutdown_background();
    };

    for key_len in &[10_usize, 128, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048] {
            bench(*key_len, *val_len);
        }
    }
}

fn monotonic_crud<T: Storage>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let db = T::open(format!("{}_monotonic_crud", T::name()))
            .await
            .unwrap();

        c.bench_function(&format!("Store: {}, monotonic inserts", T::name()), |b| {
            let count = AtomicU32::new(0_u32);
            b.iter(|| async {
                db.set(
                    Bytes::from(count.fetch_add(1, Relaxed).to_be_bytes().to_vec()),
                    Bytes::new(),
                )
                .await
                .unwrap();
            })
        });

        c.bench_function(&format!("Store: {}, monotonic gets", T::name()), |b| {
            let count = AtomicU32::new(0_u32);
            b.iter(|| async {
                db.get(&count.fetch_add(1, Relaxed).to_be_bytes())
                    .await
                    .unwrap();
            })
        });

        c.bench_function(&format!("Store: {}, monotonic removals", T::name()), |b| {
            let count = AtomicU32::new(0_u32);
            b.iter(|| async {
                db.remove(&count.fetch_add(1, Relaxed).to_be_bytes())
                    .await
                    .unwrap();
            })
        });
    });
}

fn random_read<T: Storage>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let db_path = format!("{}_random_read", T::name());
        let db = T::open(&db_path).await.unwrap();

        let key_size_range = 1usize..1025usize;
        let keys = prepare_data(&db, 100000, key_size_range.clone(), 1usize..1025usize).await;
        let keys = keys.into_iter().collect::<Vec<_>>();
        let key_count = keys.len();
        println!(
            "db size: {:?}, key count: {}",
            db.size_of_disk().await,
            key_count
        );

        c.bench_function(&format!("Store: {}, random read", T::name()), |b| {
            b.iter(|| async {
                let index = random(key_count as u32) as usize;
                let value = db.get(&keys[index]).await.unwrap();
                assert!(value.is_some());
            })
        });

        std::fs::remove_dir_all(db_path).unwrap();
    });
}

fn random_write<T: Storage>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let db_path = format!("{}_random_write", T::name());
        let db = T::open(&db_path).await.unwrap();

        c.bench_function(&format!("Store: {}, random write", T::name()), |b| {
            b.iter(|| async {
                db.set(
                    Bytes::from(random_bytes(1usize..1025usize)),
                    Bytes::from(random_bytes(1usize..1025usize)),
                )
                .await
                .unwrap();
            })
        });

        println!("db size: {:?}", db.size_of_disk().await);

        std::fs::remove_dir_all(db_path).unwrap();
    });
}

fn empty_opens<T: Storage>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let _ = std::fs::remove_dir_all("empty_opens");
    c.bench_function(&format!("Store: {}, empty opens", T::name()), |b| {
        b.to_async(&rt).iter(|| async {
            let _ = T::open(format!("empty_opens/{}.db", counter()))
                .await
                .unwrap();
        })
    });
    let _ = std::fs::remove_dir_all("empty_opens");
}

fn kv_bulk_load(c: &mut Criterion) {
    bulk_load::<KipStorage>(c);
    #[cfg(feature = "sled")]
    {
        use kip_db::kernel::sled_storage::SledStorage;
        bulk_load::<SledStorage>(c);
    }
    #[cfg(feature = "rocksdb")]
    {
        use kip_db::kernel::rocksdb_storage::RocksdbStorage;
        bulk_load::<RocksdbStorage>(c);
    }
}

fn kv_monotonic_crud(c: &mut Criterion) {
    monotonic_crud::<KipStorage>(c);
    #[cfg(feature = "sled")]
    {
        use kip_db::kernel::sled_storage::SledStorage;
        monotonic_crud::<SledStorage>(c);
    }
    #[cfg(feature = "rocksdb")]
    {
        use kip_db::kernel::rocksdb_storage::RocksdbStorage;
        monotonic_crud::<RocksdbStorage>(c);
    }
}

fn kv_random_read(c: &mut Criterion) {
    random_read::<KipStorage>(c);
    #[cfg(feature = "sled")]
    {
        use kip_db::kernel::sled_storage::SledStorage;
        random_read::<SledStorage>(c);
    }
    #[cfg(feature = "rocksdb")]
    {
        use kip_db::kernel::rocksdb_storage::RocksdbStorage;
        random_read::<RocksdbStorage>(c);
    }
}

fn kv_random_write(c: &mut Criterion) {
    random_write::<KipStorage>(c);
    #[cfg(feature = "sled")]
    {
        use kip_db::kernel::sled_storage::SledStorage;
        random_write::<SledStorage>(c);
    }
    #[cfg(feature = "rocksdb")]
    {
        use kip_db::kernel::rocksdb_storage::RocksdbStorage;
        random_write::<RocksdbStorage>(c);
    }
}

fn kv_empty_opens(c: &mut Criterion) {
    empty_opens::<KipStorage>(c);
    #[cfg(feature = "sled")]
    {
        use kip_db::kernel::sled_storage::SledStorage;
        empty_opens::<SledStorage>(c);
    }
    #[cfg(feature = "rocksdb")]
    {
        use kip_db::kernel::rocksdb_storage::RocksdbStorage;
        empty_opens::<RocksdbStorage>(c);
    }
}

criterion_group!(
    name = read_benches;
    config = Criterion::default().sample_size(1000);
    targets = kv_random_read,
);
criterion_group!(
    name = write_benches;
    config = Criterion::default().sample_size(100000);
    targets = kv_random_write,
);
criterion_group!(
    other_benches,
    kv_bulk_load,
    kv_monotonic_crud,
    kv_empty_opens
);

criterion_main!(read_benches, write_benches, other_benches,);
