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
    for i in 0..keys.len() {
        if map.insert(keys[i], 0).is_none() {
            sum += 1;
        }
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
    // get (present)
    t1(&map, &keys[..], SIZE);
    // get (absent)
    t1(&map, &absent_keys[..], 0);
}
