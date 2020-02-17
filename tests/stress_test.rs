use flurry::*;
use rand::distributions::Uniform;
use std::sync::atomic::AtomicBool;
use std::thread;

/// Number of keys and values to work with.
const NUM_KEYS: usize = 1 << 8;

type Key = usize;
type Value = usize;

struct Environment {
    table1: HashMap<Key, Value>,
    table2: HashMap<Key, Value>,
    keys: Vec<Key>,
    vals1: Vec<Value>,
    vals2: Vec<Value>,
    ind_dist: Uniform<usize>,
    val_dist1: Uniform<Value>,
    val_dist2: Uniform<Value>,
    in_table: Vec<bool>,
    in_use: Vec<AtomicBool>,
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
            vals1: vec![0usize; NUM_KEYS],
            vals2: vec![0usize; NUM_KEYS],
            ind_dist: Uniform::from(Value::min_value()..Value::max_value()),
            val_dist1: Uniform::from(Value::min_value()..Value::max_value()),
            val_dist2: Uniform::from(Value::min_value()..Value::max_value()),
            in_table: vec![false; NUM_KEYS],
            in_use,
            finished: AtomicBool::new(false),
        }
    }
}

fn stress_insert_thread(env: Arc<Mutex<Environment>>) -> JoinHandle<()> {}

#[test]
fn stress_test() {
    todo!("Implement this");
}
