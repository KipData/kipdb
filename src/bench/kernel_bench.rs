use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use bytes::Bytes;
/// 参考Sled Benchmark
/// https://github.com/spacejam/sled/blob/main/benchmarks/criterion/benches/sled.rs

use criterion::{criterion_group, criterion_main, Criterion};

use kip_db::kernel::KVStore;
use kip_db::kernel::lsm::lsm_kv::LsmStore;
use kip_db::kernel::sled_kv::SledStore;

fn counter() -> usize {
    use std::sync::atomic::AtomicUsize;

    static C: AtomicUsize = AtomicUsize::new(0);

    C.fetch_add(1, Relaxed)
}

/// Generates a random number in `0..n`.
fn random(n: u32) -> u32 {
    use std::cell::Cell;
    use std::num::Wrapping;

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1406868647));
    }

    RNG.with(|rng| {
        // This is the 32-bit variant of Xorshift.
        //
        // Source: https://en.wikipedia.org/wiki/Xorshift
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng.set(x);

        // This is a fast alternative to `x % n`.
        //
        // Author: Daniel Lemire
        // Source: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        ((x.0 as u64).wrapping_mul(n as u64) >> 32) as u32
    })
}

fn bulk_load<T: KVStore>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let count = AtomicU32::new(0_u32);
    let bytes = |len| -> Vec<u8> {
        count.fetch_add(1, Relaxed)
            .to_be_bytes()
            .into_iter()
            .cycle()
            .take(len)
            .collect()
    };

    let mut bench = |key_len, val_len| {
        let db = rt.block_on(async {
            T::open(format!("bulk_k{}_v{}", key_len, val_len)).await.unwrap()
        });

        c.bench_function(
            &format!("Store: {}, bulk load key/value lengths {}/{}", T::name(), key_len, val_len),
            |b| {
                b.to_async(&rt).iter(|| async {
                    db.set(
                        &bytes(key_len),
                        Bytes::from(bytes(val_len))
                    ).await.unwrap();
                })
            },
        );
    };

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(*key_len, *val_len);
        }
    }
}

fn monotonic_crud<T: KVStore>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let db = T::open("monotonic_crud").await.unwrap();

        c.bench_function(
            &format!("Store: {}, monotonic inserts", T::name()),
            |b| {

                let count = AtomicU32::new(0_u32);
                b.iter(|| async {
                    db.set(&count.fetch_add(1, Relaxed).to_be_bytes(), Bytes::new()).await.unwrap();
                })
            }
        );

        c.bench_function(
            &format!("Store: {}, monotonic gets", T::name()),
            |b| {
                let count = AtomicU32::new(0_u32);
                b.iter(|| async {
                    db.get(&count.fetch_add(1, Relaxed).to_be_bytes()).await.unwrap();
                })
            }
        );

        c.bench_function(
            &format!("Store: {}, monotonic removals", T::name()),

            |b| {
                let count = AtomicU32::new(0_u32);
                b.iter(|| async {
                    db.remove(&count.fetch_add(1, Relaxed).to_be_bytes()).await.unwrap();
                })
            }
        );
    });
}

fn random_crud<T: KVStore>(c: &mut Criterion) {
    const SIZE: u32 = 65536;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let db = T::open("random_crud").await.unwrap();

        c.bench_function(
            &format!("Store: {}, random inserts", T::name()),
            |b| {
                b.iter(|| async {
                    db.set(&random(SIZE).to_be_bytes(), Bytes::new()).await.unwrap();
                })
            }
        );

        c.bench_function(
            &format!("Store: {}, random gets", T::name()),
            |b| {
                b.iter(|| async {
                    db.get(&random(SIZE).to_be_bytes()).await.unwrap();
                })
            }
        );

        c.bench_function(
            &format!("Store: {}, random removals", T::name()),
            |b| {
                b.iter(|| async {
                    db.remove(&random(SIZE).to_be_bytes()).await.unwrap();
                })
            }
        );
    });
}

fn empty_opens<T: KVStore>(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let _ = std::fs::remove_dir_all("empty_opens");
    c.bench_function(
        &format!("Store: {}, empty opens", T::name()),
        |b| {
            b.to_async(&rt).iter(|| async {
                let _ = T::open(format!("empty_opens/{}.db", counter())).await.unwrap();
            })
        }
    );
    let _ = std::fs::remove_dir_all("empty_opens");
}

fn kv_bulk_load(c: &mut Criterion) {
    bulk_load::<LsmStore>(c);
    bulk_load::<SledStore>(c);
}

fn kv_monotonic_crud(c: &mut Criterion) {
    monotonic_crud::<LsmStore>(c);
    monotonic_crud::<SledStore>(c);
}

fn kv_random_crud(c: &mut Criterion) {
    random_crud::<LsmStore>(c);
    random_crud::<SledStore>(c);
}

fn kv_empty_opens(c: &mut Criterion) {
    empty_opens::<LsmStore>(c);
    empty_opens::<SledStore>(c);
}

criterion_group!(
    benches,
    kv_bulk_load,
    kv_monotonic_crud,
    kv_random_crud,
    kv_empty_opens
);
criterion_main!(benches);