use bytes::Bytes;
use futures::future::join_all;
use kip_db::kernel::Storage;
use rand::Rng;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::atomic::Ordering::Relaxed;

pub async fn prepare_data<T: Storage>(
    db: &T,
    data_size: u32,
    key_size_range: Range<usize>,
    value_size_range: Range<usize>,
) -> HashSet<Vec<u8>> {
    let mut tasks = Vec::new();
    let mut keys = HashSet::new();

    // there some dup keys, so final data size is probably less than data_size
    for _ in 0..data_size {
        let key = random_bytes(key_size_range.clone());
        let value = random_bytes(value_size_range.clone());

        keys.insert(key.clone());
        tasks.push(db.set(Bytes::from(key), Bytes::from(value)));
    }
    join_all(tasks).await;
    keys
}

pub fn random_bytes(size_range: Range<usize>) -> Vec<u8> {
    let mut rand = rand::thread_rng();

    let size = rand.gen_range(size_range);
    let mut bytes = Vec::with_capacity(size);

    for _ in 0..size {
        bytes.push(rand.gen_range(0..=255));
    }

    bytes
}

pub fn counter() -> usize {
    use std::sync::atomic::AtomicUsize;

    static C: AtomicUsize = AtomicUsize::new(0);

    C.fetch_add(1, Relaxed)
}

/// Generates a random number in `0..n`.
pub fn random(n: u32) -> u32 {
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
