use bustle::*;
use flurry::*;
use std::sync::Arc;

#[derive(Clone)]
struct Table<K: 'static + Send + Sync>(Arc<HashMap<K, ()>>);

impl<K> BenchmarkTarget for Table<K>
where
    K: Sync + Send + From<u64> + Copy + 'static + std::hash::Hash + Eq + std::fmt::Debug,
{
    type Key = K;

    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(HashMap::with_capacity(capacity)))
    }

    fn get(&mut self, key: &Self::Key) -> bool {
        self.0.pin().get(key).is_some()
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        self.0.pin().insert(*key, ()).is_some()
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        self.0.pin().remove(key).is_some()
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        self.0
            .pin()
            .compute_if_present(key, |_, _| Some(()))
            .is_some()
    }
}

fn main() {
    Workload::new(1, Mix::read_heavy()).run::<Table<u64>>();
}
