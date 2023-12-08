use integer_encoding::FixedInt;
use itertools::Itertools;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::slice;

pub(crate) const DEFAULT_HASH_SEED_1: u64 = 31;

pub(crate) const DEFAULT_HASH_SEED_2: u64 = 37;

// https://rust-algo.club/collections/bloom_filter/
#[derive(Debug, Default)]
pub struct BloomFilter<T: ?Sized> {
    bits: BitVector,
    hash_fn_count: u64,
    hashers: [FixedHasher; 2],
    _phantom: PhantomData<T>,
}

impl<T: ?Sized> BloomFilter<T> {
    pub fn new(len: usize, err_rate: f64) -> Self {
        let bits_count = Self::optimal_bits_count(len, err_rate);
        let hash_fn_count = Self::optimal_hashers_count(err_rate);
        let hashers = [
            FixedHasher::new(DEFAULT_HASH_SEED_1),
            FixedHasher::new(DEFAULT_HASH_SEED_2),
        ];

        Self {
            bits: BitVector::new(bits_count),
            hash_fn_count,
            hashers,
            _phantom: PhantomData,
        }
    }

    pub fn insert(&mut self, elem: &T)
    where
        T: Hash,
    {
        // g_i(x) = h1(x) + i * h2(x)
        let hashes = self.make_hash(elem);
        for fn_i in 0..self.hash_fn_count {
            let index = self.get_index(hashes, fn_i as u64);
            self.bits.set_bit(index, true);
        }
    }

    pub fn contains(&self, elem: &T) -> bool
    where
        T: Hash,
    {
        let hashes = self.make_hash(elem);
        (0..self.hash_fn_count).all(|fn_i| {
            let index = self.get_index(hashes, fn_i as u64);
            self.bits.get_bit(index)
        })
    }

    fn get_index(&self, (h1, h2): (u64, u64), fn_i: u64) -> usize {
        (h1.wrapping_add(fn_i.wrapping_mul(h2)) % self.bits.len() as u64) as usize
    }

    fn make_hash(&self, elem: &T) -> (u64, u64)
    where
        T: Hash,
    {
        let hasher1 = &mut self.hashers[0].clone();
        let hasher2 = &mut self.hashers[1].clone();

        elem.hash(hasher1);
        elem.hash(hasher2);

        (hasher1.finish(), hasher2.finish())
    }

    /// m = -1 * (n * ln ε) / (ln 2)^2
    fn optimal_bits_count(len: usize, err_rate: f64) -> usize {
        let ln_2_2 = std::f64::consts::LN_2.powf(2f64);
        (-1f64 * len as f64 * err_rate.ln() / ln_2_2).ceil() as usize
    }

    /// k = -log_2 ε
    fn optimal_hashers_count(err_rate: f64) -> u64 {
        (-1f64 * err_rate.log2()).ceil() as u64
    }

    pub fn to_raw(&self) -> Vec<u8> {
        let mut bytes = u64::encode_fixed_vec(self.hash_fn_count);

        bytes.append(&mut self.bits.to_raw());
        bytes
    }

    pub fn from_raw(bytes: &[u8]) -> Self {
        let hash_fn_count = u64::decode_fixed(&bytes[0..8]);
        let bits = BitVector::from_raw(&bytes[8..]);
        let hashers = [
            FixedHasher::new(DEFAULT_HASH_SEED_1),
            FixedHasher::new(DEFAULT_HASH_SEED_2),
        ];

        BloomFilter {
            bits,
            hash_fn_count,
            hashers,
            _phantom: PhantomData,
        }
    }
}

#[derive(Debug, Default)]
pub struct BitVector {
    len: u64,
    bit_groups: Vec<i8>,
}

impl BitVector {
    pub fn new(len: usize) -> BitVector {
        BitVector {
            len: len as u64,
            bit_groups: vec![0; (len + 7) / 8],
        }
    }

    pub fn set_bit(&mut self, index: usize, value: bool) {
        let byte_index = index / 8;
        let bit_index = index % 8;

        if value {
            self.bit_groups[byte_index] |= 1 << bit_index;
        } else {
            self.bit_groups[byte_index] &= !(1 << bit_index);
        }
    }

    pub fn get_bit(&self, index: usize) -> bool {
        (self.bit_groups[index / 8] >> index % 8) & 1 != 0
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn to_raw(&self) -> Vec<u8> {
        let mut bytes = u64::encode_fixed_vec(self.len);

        for bits in &self.bit_groups {
            bytes.append(&mut bits.encode_fixed_vec());
        }
        bytes
    }

    pub fn from_raw(bytes: &[u8]) -> Self {
        let len = u64::decode_fixed(&bytes[0..8]);
        let bit_groups = bytes[8..]
            .iter()
            .map(|bit| i8::decode_fixed(slice::from_ref(bit)))
            .collect_vec();

        BitVector { len, bit_groups }
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct FixedHasher {
    seed: u64,
}

impl FixedHasher {
    pub fn new(seed: u64) -> Self {
        FixedHasher { seed }
    }
}

impl Hasher for FixedHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut hasher = DefaultHasher::new();
        self.seed.hash(&mut hasher);
        bytes.hash(&mut hasher);
        self.seed = hasher.finish();
    }

    fn finish(&self) -> u64 {
        self.seed
    }
}

#[cfg(test)]
mod tests {
    use crate::kernel::utils::bloom_filter::{BitVector, BloomFilter};
    use rand::Rng;
    use std::collections::HashSet;

    #[test]
    fn bit_vector_serialization() {
        let mut vector = BitVector::new(100);

        vector.set_bit(99, true);

        let bytes = vector.to_raw();
        let vector = BitVector::from_raw(&bytes);

        for i in 0..98 {
            assert!(!vector.get_bit(i));
        }
        assert!(vector.get_bit(99));
    }

    #[test]
    fn bit_vector_simple() {
        let mut vector = BitVector::new(100);

        vector.set_bit(99, true);

        for i in 0..98 {
            assert!(!vector.get_bit(i));
        }
        assert!(vector.get_bit(99));
    }

    #[test]
    fn bloom_filter_serialization() {
        let mut bf = BloomFilter::new(100, 0.01);

        bf.insert(&1);

        let bytes = bf.to_raw();
        let bf = BloomFilter::from_raw(&bytes);

        assert!(bf.contains(&1));
        assert!(!bf.contains(&2));
    }

    #[test]
    fn bloom_filter_simple() {
        let mut bf = BloomFilter::new(100, 0.01);

        bf.insert(&1);

        assert!(bf.contains(&1));
        assert!(!bf.contains(&2));
    }

    #[test]
    fn bloom_filter_fpr_test() {
        let cnt = 500000;
        let rate = 0.01;

        let mut bf = BloomFilter::new(cnt, rate);
        let mut set: HashSet<i32> = HashSet::new();
        let mut rng = rand::thread_rng();

        let mut i = 0;

        while i < cnt {
            let v = rng.gen::<i32>();
            set.insert(v);
            bf.insert(&v);
            i += 1;
        }

        i = 0;
        let mut false_positives = 0;
        while i < cnt {
            let v = rng.gen::<i32>();
            match (bf.contains(&v), set.contains(&v)) {
                (true, false) => {
                    false_positives += 1;
                }
                (false, true) => {
                    assert!(false);
                } // should never happen
                _ => {}
            }
            i += 1;
        }

        // make sure we're not too far off
        let actual_rate = false_positives as f64 / cnt as f64;
        assert!(actual_rate > (rate - 0.001));
        assert!(actual_rate < (rate + 0.001));
    }
}
