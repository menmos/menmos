use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

// See https://preshing.com/20110504/hash-collision-probabilities/
fn optimal_bucket_count(concurrent_calls: usize, mut collision_probability: f64) -> usize {
    if collision_probability == 0.0 {
        collision_probability = f64::EPSILON; // use the smallest possible float that is greater than zero - this will generate a huge amount of filters.
    }

    let bucket_count_float =
        ((concurrent_calls as f64 - 1.0) * concurrent_calls as f64) / (2.0 * collision_probability);

    (bucket_count_float.ceil() as usize).max(1)
}

pub struct ShardedMutex {
    buf: Vec<RwLock<()>>,
}

impl ShardedMutex {
    pub fn new(concurrent_calls: usize, collision_probability: f64) -> Self {
        let bucket_count = optimal_bucket_count(concurrent_calls, collision_probability);

        let mut buf = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            buf.push(RwLock::new(()));
        }

        Self { buf }
    }

    fn get_lock_id<H: Hash>(&self, key: &H) -> usize {
        // TODO: Faster hashing algo? Look into what hashmap does.
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();

        let mod_val = hash_value % (self.buf.len() as u64);

        debug_assert!(
            mod_val <= (self.buf.len() as u64),
            "mod of length should give a value withing length bounds"
        );

        mod_val as usize
    }

    #[tracing::instrument(skip(self, key))]
    pub async fn read<'a, H: Hash>(&'a self, key: &H) -> RwLockReadGuard<'a, ()> {
        let lock_id = self.get_lock_id(key);
        self.buf[lock_id].read().await
    }

    #[tracing::instrument(skip(self, key))]
    pub async fn write<'a, H: Hash>(&'a self, key: &H) -> RwLockWriteGuard<'a, ()> {
        let lock_id = self.get_lock_id(key);
        self.buf[lock_id].write().await
    }
}

#[cfg(test)]
mod tests {
    use super::optimal_bucket_count;

    #[test]
    fn optimal_bucket_count_basic() {
        let actual = optimal_bucket_count(2, 0.5);
        assert_eq!(actual, 2);
    }
}
