use flurry::*;
use rand::prelude::*;
use std::hash::Hash;

#[cfg(not(miri))]
const SIZE: usize = 50_000;
#[cfg(miri)]
const SIZE: usize = 12;

// there must be more things absent than present!
#[cfg(not(miri))]
const ABSENT_SIZE: usize = 1 << 17;
#[cfg(miri)]
const ABSENT_SIZE: usize = 1 << 5;

const ABSENT_MASK: usize = ABSENT_SIZE - 1;

fn t1<K, V>(map: &HashMap<K, V>, keys: &[K], expect: usize)
where
    K: Sync + Send + Clone + Hash + Ord,
    V: Sync + Send,
{
    let mut sum = 0;
    let iters = 4;
    let guard = map.guard();
    for _ in 0..iters {
        for key in keys {
            if map.get(key, &guard).is_some() {
                sum += 1;
            }
        }
    }
    assert_eq!(sum, expect * iters);
}

fn t2<K>(map: &HashMap<K, usize>, keys: &[K], expect: usize)
where
    K: Sync + Send + Copy + Hash + Ord + std::fmt::Display,
{
    let mut sum = 0;
    let guard = map.guard();
    for key in keys {
        if map.remove(key, &guard).is_some() {
            sum += 1;
        }
    }
    assert_eq!(sum, expect);
}

fn t3<K>(map: &HashMap<K, usize>, keys: &[K], expect: usize)
where
    K: Sync + Send + Copy + Hash + Ord,
{
    let mut sum = 0;
    let guard = map.guard();
    for i in 0..keys.len() {
        if map.insert(keys[i], 0, &guard).is_none() {
            sum += 1;
        }
    }
    assert_eq!(sum, expect);
}

fn t4<K>(map: &HashMap<K, usize>, keys: &[K], expect: usize)
where
    K: Sync + Send + Copy + Hash + Ord,
{
    let mut sum = 0;
    let guard = map.guard();
    for i in 0..keys.len() {
        if map.contains_key(&keys[i], &guard) {
            sum += 1;
        }
    }
    assert_eq!(sum, expect);
}

fn t5<K>(map: &HashMap<K, usize>, keys: &[K], expect: usize)
where
    K: Sync + Send + Copy + Hash + Ord,
{
    let mut sum = 0;
    let guard = map.guard();
    let mut i = keys.len() as isize - 2;
    while i >= 0 {
        if map.remove(&keys[i as usize], &guard).is_some() {
            sum += 1;
        }
        i -= 2;
    }
    assert_eq!(sum, expect);
}

fn t6<K, V>(map: &HashMap<K, V>, keys1: &[K], keys2: &[K], expect: usize)
where
    K: Sync + Send + Clone + Hash + Ord,
    V: Sync + Send,
{
    let mut sum = 0;
    let guard = map.guard();
    for i in 0..expect {
        if map.get(&keys1[i], &guard).is_some() {
            sum += 1;
        }
        if map.get(&keys2[i & ABSENT_MASK], &guard).is_some() {
            sum += 1;
        }
    }
    assert_eq!(sum, expect);
}

fn t7<K>(map: &HashMap<K, usize>, k1: &[K], k2: &[K])
where
    K: Sync + Send + Copy + Hash + Ord,
{
    let mut sum = 0;
    let guard = map.guard();
    for i in 0..k1.len() {
        if map.contains_key(&k1[i], &guard) {
            sum += 1;
        }
        if map.contains_key(&k2[i], &guard) {
            sum += 1;
        }
    }
    assert_eq!(sum, k1.len());
}

fn ittest1<K>(map: &HashMap<K, usize>, expect: usize)
where
    K: Sync + Send + Copy + Hash + Eq,
{
    let mut sum = 0;
    let guard = map.guard();
    for _ in map.keys(&guard) {
        sum += 1;
    }
    assert_eq!(sum, expect);
}

fn ittest2<K>(map: &HashMap<K, usize>, expect: usize)
where
    K: Sync + Send + Copy + Hash + Eq,
{
    let mut sum = 0;
    let guard = map.guard();
    for _ in map.values(&guard) {
        sum += 1;
    }
    assert_eq!(sum, expect);
}

fn ittest3<K>(map: &HashMap<K, usize>, expect: usize)
where
    K: Sync + Send + Copy + Hash + Eq,
{
    let mut sum = 0;
    let guard = map.guard();
    for _ in map.iter(&guard) {
        sum += 1;
    }
    assert_eq!(sum, expect);
}

#[test]
fn everything() {
    let mut rng = rand::thread_rng();

    let map = HashMap::new();
    let mut keys: Vec<_> = (0..ABSENT_SIZE + SIZE).collect();
    keys.shuffle(&mut rng);
    let absent_keys = &keys[0..ABSENT_SIZE];
    let keys = &keys[ABSENT_SIZE..];

    // put (absent)
    t3(&map, keys, SIZE);
    // put (present)
    t3(&map, keys, 0);
    // contains_key (present & absent)
    t7(&map, keys, absent_keys);
    // contains_key (present)
    t4(&map, keys, SIZE);
    // contains_key (absent)
    t4(&map, absent_keys, 0);
    // get
    t6(&map, keys, absent_keys, SIZE);
    // get (present)
    t1(&map, keys, SIZE);
    // get (absent)
    t1(&map, absent_keys, 0);
    // remove (absent)
    t2(&map, absent_keys, 0);
    // remove (present)
    t5(&map, keys, SIZE / 2);
    // put (half present)
    t3(&map, keys, SIZE / 2);
    // iter, keys, values (present)
    ittest1(&map, SIZE);
    ittest2(&map, SIZE);
    ittest3(&map, SIZE);
}
