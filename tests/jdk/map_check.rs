use crossbeam::epoch;
use flurry::*;
use rand::prelude::*;
use std::hash::Hash;

const SIZE: usize = 50_000;
const ABSENT_SIZE: usize = 1 << 17;

fn t1<K, V>(map: &FlurryHashMap<K, V>, keys: &[K], expect: usize)
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
{
    let mut sum = 0;
    let iters = 4;
    let guard = epoch::pin();
    for _ in 0..iters {
        for key in keys {
            if map.get(key, &guard).is_some() {
                sum += 1;
            }
        }
    }
    assert_eq!(sum, expect * iters);
}

fn t3<K>(map: &FlurryHashMap<K, usize>, keys: &[K], expect: usize)
where
    K: Sync + Send + Copy + Hash + Eq,
{
    let mut sum = 0;
    let guard = epoch::pin();
    for i in 0..keys.len() {
        if map.insert(keys[i], 0, &guard).is_none() {
            sum += 1;
        }
    }
    assert_eq!(sum, expect);
}

fn t4<K>(map: &FlurryHashMap<K, usize>, keys: &[K], expect: usize)
where
    K: Sync + Send + Copy + Hash + Eq,
{
    let mut sum = 0;
    for i in 0..keys.len() {
        if map.contains_key(&keys[i]) {
            sum += 1;
        }
    }
    assert_eq!(sum, expect);
}

fn t7<K>(map: &FlurryHashMap<K, usize>, k1: &[K], k2: &[K])
where
    K: Sync + Send + Copy + Hash + Eq,
{
    let mut sum = 0;
    for i in 0..k1.len() {
        if map.contains_key(&k1[i]) {
            sum += 1;
        }
        if map.contains_key(&k2[i]) {
            sum += 1;
        }
    }
    assert_eq!(sum, k1.len());
}

fn ittest1<K>(map: &FlurryHashMap<K, usize>, expect: usize)
where
    K: Sync + Send + Copy + Hash + Eq,
{
    let mut sum = 0;
    let guard = epoch::pin();
    for _ in map.keys(&guard) {
        sum += 1;
    }
    assert_eq!(sum, expect);
}

fn ittest2<K>(map: &FlurryHashMap<K, usize>, expect: usize)
where
    K: Sync + Send + Copy + Hash + Eq,
{
    let mut sum = 0;
    let guard = epoch::pin();
    for _ in map.values(&guard) {
        sum += 1;
    }
    assert_eq!(sum, expect);
}

fn ittest3<K>(map: &FlurryHashMap<K, usize>, expect: usize)
where
    K: Sync + Send + Copy + Hash + Eq,
{
    let mut sum = 0;
    let guard = epoch::pin();
    for _ in map.iter(&guard) {
        sum += 1;
    }
    assert_eq!(sum, expect);
}

#[test]
fn everything() {
    let mut rng = rand::thread_rng();

    let map = FlurryHashMap::new();
    let mut keys: Vec<_> = (0..ABSENT_SIZE + SIZE).collect();
    keys.shuffle(&mut rng);
    let absent_keys = &keys[0..ABSENT_SIZE];
    let keys = &keys[ABSENT_SIZE..];

    // put (absent)
    t3(&map, &keys[..], SIZE);
    // put (present)
    t3(&map, &keys[..], 0);
    // contains_key (present & absent)
    t7(&map, &keys[..], &absent_keys[..]);
    // contains_key (present)
    t4(&map, &keys[..], SIZE);
    // contains_key (absent)
    t4(&map, &absent_keys[..], 0);
    // get (present)
    t1(&map, &keys[..], SIZE);
    // get (absent)
    t1(&map, &absent_keys[..], 0);

    // iter, keys, values (present)
    ittest1(&map, SIZE);
    ittest2(&map, SIZE);
    ittest3(&map, SIZE);
}
