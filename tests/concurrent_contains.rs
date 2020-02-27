use crossbeam_epoch as epoch;
use flurry::HashMap;
use std::{sync::Arc, thread};

/// Number of entries for each thread to place in the map.
const NUM_ENTRIES: usize = 16;

/// Number of iterations for each test
const ITERATIONS: usize = 256;

/// Number of rounds every thread perfoms per entry.
const ROUNDS: usize = 32;

#[test]
fn test_concurrent_contains_key() {
    let guard = epoch::pin();
    let map = HashMap::new();
    let mut content = [0; NUM_ENTRIES];
    for k in 0..NUM_ENTRIES {
        map.insert(k, k, &guard);
        content[k] = k;
    }
    test(content, Arc::new(map));
}

fn test(content: [usize; NUM_ENTRIES], map: Arc<HashMap<usize, usize>>) {
    for _ in 0..ITERATIONS {
        test_once(content, map.clone());
    }
}

fn test_once(content: [usize; NUM_ENTRIES], map: Arc<HashMap<usize, usize>>) {
    let mut threads = Vec::new();
    for _ in 0..num_cpus::get().min(8) {
        let map = map.clone();
        let content = content.clone();
        let handle = thread::spawn(move || {
            let guard = &epoch::pin();
            let map = map.clone();
            for i in 0..NUM_ENTRIES * ROUNDS {
                let key = content[i % content.len()];
                assert!(map.contains_key(&key, guard));
            }
        });
        threads.push(handle);
    }
    for t in threads {
        t.join().expect("failed to join thread");
    }
}
