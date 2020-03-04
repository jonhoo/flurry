use flurry::HashMap;
use rand::Rng;
use std::sync::Arc;

/// Number of entries for each thread to place in the map.
const NUM_ENTRIES: usize = 128;

/// Number of iterations for each test
const ITERATIONS: usize = 64;

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
struct KeyVal {
    _data: usize,
}

impl KeyVal {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        Self { _data: rng.gen() }
    }
}

fn insert(map: Arc<HashMap<KeyVal, KeyVal>>, k: KeyVal) {
    map.insert(k, k, &map.guard());
}

#[test]
fn test_concurrent_insert<'g>() {
    test(insert);
}

fn test<F>(associator: F)
where
    F: Fn(Arc<HashMap<KeyVal, KeyVal>>, KeyVal) + Send + Copy + 'static,
{
    for _ in 0..ITERATIONS {
        test_once(associator);
    }
}

fn test_once<F>(associator: F)
where
    F: Fn(Arc<HashMap<KeyVal, KeyVal>>, KeyVal) + Send + Copy + 'static,
{
    let map = Arc::new(HashMap::new());
    let mut threads = Vec::new();
    for _ in 0..num_cpus::get().min(8) {
        let map = map.clone();
        let handle = std::thread::spawn(move || {
            for _ in 0..NUM_ENTRIES {
                let key = KeyVal::new();
                associator(map.clone(), key);
                assert!(map.contains_key(&key, &map.guard()));
            }
        });
        threads.push(handle);
    }
    for t in threads {
        t.join().expect("failed to join thread");
    }
}
