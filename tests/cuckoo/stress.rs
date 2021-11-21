// https://github.com/efficient/libcuckoo/tree/master/tests/stress-tests

use flurry::*;
use parking_lot::Mutex;
use rand::distributions::{Distribution, Uniform};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;

/// Number of keys and values to work with.
const NUM_KEYS: usize = 1 << 12;
/// Number of threads that should be started.
const NUM_THREADS: usize = 4;
/// How long the stress test will run (in milliseconds).
const TEST_LEN: u64 = 5000;

type Key = usize;
type Value = usize;

struct Environment {
    table1: HashMap<Key, Value>,
    table2: HashMap<Key, Value>,
    keys: Vec<Key>,
    vals1: Mutex<Vec<Value>>,
    vals2: Mutex<Vec<Value>>,
    ind_dist: Uniform<usize>,
    val_dist1: Uniform<Value>,
    val_dist2: Uniform<Value>,
    in_table: Mutex<Vec<bool>>,
    in_use: Mutex<Vec<AtomicBool>>,
    finished: AtomicBool,
}

impl Environment {
    pub fn new() -> Self {
        let mut keys = Vec::with_capacity(NUM_KEYS);
        let mut in_use = Vec::with_capacity(NUM_KEYS);

        for i in 0..NUM_KEYS {
            keys.push(i);
            in_use.push(AtomicBool::new(false));
        }

        Self {
            table1: HashMap::new(),
            table2: HashMap::new(),
            keys,
            vals1: Mutex::new(vec![0usize; NUM_KEYS]),
            vals2: Mutex::new(vec![0usize; NUM_KEYS]),
            ind_dist: Uniform::from(0..NUM_KEYS - 1),
            val_dist1: Uniform::from(Value::MIN..Value::MAX),
            val_dist2: Uniform::from(Value::MIN..Value::MAX),
            in_table: Mutex::new(vec![false; NUM_KEYS]),
            in_use: Mutex::new(in_use),
            finished: AtomicBool::new(false),
        }
    }
}

fn stress_insert_thread(env: Arc<Environment>) {
    let mut rng = rand::thread_rng();
    let guard = epoch::pin();
    while !env.finished.load(Ordering::SeqCst) {
        let idx = env.ind_dist.sample(&mut rng);
        let in_use = env.in_use.lock();
        if (*in_use)[idx]
            .compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let key = env.keys[idx];
            let val1 = env.val_dist1.sample(&mut rng);
            let val2 = env.val_dist2.sample(&mut rng);
            let res1 = if !env.table1.contains_key(&key, &guard) {
                env.table1.insert(key, val1, &guard).map_or(true, |_| false)
            } else {
                false
            };
            let res2 = if !env.table2.contains_key(&key, &guard) {
                env.table2.insert(key, val2, &guard).map_or(true, |_| false)
            } else {
                false
            };
            let mut in_table = env.in_table.lock();
            assert_ne!(res1, (*in_table)[idx]);
            assert_ne!(res2, (*in_table)[idx]);
            if res1 {
                assert_eq!(Some(&val1), env.table1.get(&key, &guard));
                assert_eq!(Some(&val2), env.table2.get(&key, &guard));
                let mut vals1 = env.vals1.lock();
                let mut vals2 = env.vals2.lock();
                (*vals1)[idx] = val1;
                (*vals2)[idx] = val2;
                (*in_table)[idx] = true;
            }
            (*in_use)[idx].swap(false, Ordering::SeqCst);
        }
    }
}

fn stress_delete_thread(env: Arc<Environment>) {
    let mut rng = rand::thread_rng();
    let guard = epoch::pin();
    while !env.finished.load(Ordering::SeqCst) {
        let idx = env.ind_dist.sample(&mut rng);
        let in_use = env.in_use.lock();
        if (*in_use)[idx]
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            let key = env.keys[idx];
            let res1 = env.table1.remove(&key, &guard).map_or(false, |_| true);
            let res2 = env.table2.remove(&key, &guard).map_or(false, |_| true);
            let mut in_table = env.in_table.lock();
            assert_eq!(res1, (*in_table)[idx]);
            assert_eq!(res2, (*in_table)[idx]);
            if res1 {
                assert!(env.table1.get(&key, &guard).is_none());
                assert!(env.table2.get(&key, &guard).is_none());
                (*in_table)[idx] = false;
            }
            (*in_use)[idx].swap(false, Ordering::SeqCst);
        }
    }
}

fn stress_find_thread(env: Arc<Environment>) {
    let mut rng = rand::thread_rng();
    let guard = epoch::pin();
    while !env.finished.load(Ordering::SeqCst) {
        let idx = env.ind_dist.sample(&mut rng);
        let in_use = env.in_use.lock();
        if (*in_use)[idx]
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            let key = env.keys[idx];
            let in_table = env.in_table.lock();
            let val1 = (*env.vals1.lock())[idx];
            let val2 = (*env.vals2.lock())[idx];

            let value = env.table1.get(&key, &guard);
            if value.is_some() {
                assert_eq!(&val1, value.unwrap());
                assert!((*in_table)[idx]);
            }
            let value = env.table2.get(&key, &guard);
            if value.is_some() {
                assert_eq!(&val2, value.unwrap());
                assert!((*in_table)[idx]);
            }
            (*in_use)[idx].swap(false, Ordering::SeqCst);
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn stress_test() {
    let root = Arc::new(Environment::new());
    let mut threads = Vec::new();
    for _ in 0..NUM_THREADS {
        let env = Arc::clone(&root);
        threads.push(thread::spawn(move || stress_insert_thread(env)));
        let env = Arc::clone(&root);
        threads.push(thread::spawn(move || stress_delete_thread(env)));
        let env = Arc::clone(&root);
        threads.push(thread::spawn(move || stress_find_thread(env)));
    }
    thread::sleep(std::time::Duration::from_millis(TEST_LEN));
    root.finished.swap(true, Ordering::SeqCst);
    for t in threads {
        t.join().expect("failed to join thread");
    }
    let in_table = &*root.in_table.lock();
    let num_filled = in_table.iter().filter(|b| **b).count();
    assert_eq!(num_filled, root.table1.len());
    assert_eq!(num_filled, root.table2.len());
}
