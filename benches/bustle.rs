use bustle::*;
use flurry::*;
use std::hash::Hash;
use std::sync::Arc;

const REPIN_EVERY: usize = 1024;

struct Table<K>(Arc<HashMap<K, ()>>);

impl<K> Collection for Table<K>
where
    K: Sync + Send + 'static + From<u64> + Copy + Hash + Eq,
{
    type Handle = TableHandle<K>;

    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(HashMap::with_capacity(capacity)))
    }

    fn pin(&self) -> Self::Handle {
        let map = Arc::clone(&self.0);
        let guard = map.guard();
        TableHandle {
            map,
            guard,
            i: REPIN_EVERY,
        }
    }
}

struct TableHandle<K> {
    map: Arc<HashMap<K, ()>>,
    guard: epoch::Guard,
    i: usize,
}

impl<K> TableHandle<K> {
    #[inline(always)]
    fn maybe_repin(&mut self) {
        self.i -= 1;
        if self.i == 0 {
            self.i = REPIN_EVERY;
            self.guard.repin();
        }
    }
}

impl<K> CollectionHandle for TableHandle<K>
where
    K: Sync + Send + 'static + From<u64> + Copy + Hash + Eq,
{
    type Key = K;

    fn get(&mut self, key: &Self::Key) -> bool {
        self.maybe_repin();
        self.map.with_guard(&self.guard).get(key).is_some()
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        self.maybe_repin();
        self.map.with_guard(&self.guard).insert(*key, ()).is_some()
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        self.maybe_repin();
        self.map.with_guard(&self.guard).remove(key).is_some()
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        self.maybe_repin();
        self.map
            .with_guard(&self.guard)
            .compute_if_present(key, |_, _| Some(()))
            .is_some()
    }
}

fn main() {
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::read_heavy()).run::<Table<u64>>();
    }
}
