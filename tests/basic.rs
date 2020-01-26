use crossbeam_epoch as epoch;
use flurry::*;
use std::sync::Arc;

#[test]
fn new() {
    let _map = HashMap::<usize, usize>::new();
}

#[test]
fn insert() {
    let map = HashMap::<usize, usize>::new();
    let guard = epoch::pin();
    let old = map.insert(42, 0, &guard);
    assert!(old.is_none());
}

#[test]
fn get_empty() {
    let map = HashMap::<usize, usize>::new();

    {
        let guard = epoch::pin();
        let e = map.get(&42, &guard);
        assert!(e.is_none());
    }
}

#[test]
fn get_key_value_empty() {
    let map = HashMap::<usize, usize>::new();

    {
        let guard = epoch::pin();
        let e = map.get_key_value(&42, &guard);
        assert!(e.is_none());
    }
}

#[test]
fn remove_empty() {
    let map = HashMap::<usize, usize>::new();

    {
        let guard = epoch::pin();
        let old = map.remove(&42, &guard);
        assert!(old.is_none());
    }
}

#[test]
fn insert_and_remove() {
    let map = HashMap::<usize, usize>::new();

    {
        let guard = epoch::pin();
        map.insert(42, 0, &guard);
        let old = map.remove(&42, &guard).unwrap();
        assert_eq!(old, &0);
        assert!(map.get(&42, &guard).is_none());
    }
}

#[test]
fn insert_and_get() {
    let map = HashMap::<usize, usize>::new();

    map.insert(42, 0, &epoch::pin());
    {
        let guard = epoch::pin();
        let e = map.get(&42, &guard).unwrap();
        assert_eq!(e, &0);
    }
}

#[test]
fn insert_and_get_key_value() {
    let map = HashMap::<usize, usize>::new();

    map.insert(42, 0, &epoch::pin());
    {
        let guard = epoch::pin();
        let e = map.get_key_value(&42, &guard).unwrap();
        assert_eq!(e, (&42, &0));
    }
}

use std::hash::{BuildHasher, Hasher};

struct OneBucketState;
struct OneBucketHasher;
impl BuildHasher for OneBucketState {
    type Hasher = OneBucketHasher;

    fn build_hasher(&self) -> Self::Hasher {
        OneBucketHasher
    }
}
impl Hasher for OneBucketHasher {
    fn write(&mut self, _bytes: &[u8]) {}
    fn finish(&self) -> u64 {
        0
    }
}

#[test]
fn one_bucket() {
    let guard = epoch::pin();
    let map = HashMap::<&'static str, usize, _>::with_hasher(OneBucketState);

    // we want to check that all operations work regardless on whether
    // we are operating on the head of a bucket, the tail of the bucket,
    // or somewhere in the middle.
    let v = map.insert("head", 0, &guard);
    assert_eq!(v, None);
    let v = map.insert("middle", 10, &guard);
    assert_eq!(v, None);
    let v = map.insert("tail", 100, &guard);
    assert_eq!(v, None);
    let e = map.get("head", &guard).unwrap();
    assert_eq!(e, &0);
    let e = map.get("middle", &guard).unwrap();
    assert_eq!(e, &10);
    let e = map.get("tail", &guard).unwrap();
    assert_eq!(e, &100);

    // check that replacing the keys returns the correct old value
    let v = map.insert("head", 1, &guard);
    assert_eq!(v, Some(&0));
    let v = map.insert("middle", 11, &guard);
    assert_eq!(v, Some(&10));
    let v = map.insert("tail", 101, &guard);
    assert_eq!(v, Some(&100));
    // and updated the right value
    let e = map.get("head", &guard).unwrap();
    assert_eq!(e, &1);
    let e = map.get("middle", &guard).unwrap();
    assert_eq!(e, &11);
    let e = map.get("tail", &guard).unwrap();
    assert_eq!(e, &101);
    // and that remove produces the right value
    // note that we must remove them in a particular order
    // so that we test all three node positions
    let v = map.remove("middle", &guard);
    assert_eq!(v, Some(&11));
    let v = map.remove("tail", &guard);
    assert_eq!(v, Some(&101));
    let v = map.remove("head", &guard);
    assert_eq!(v, Some(&1));
}

#[test]
fn update() {
    let map = HashMap::<usize, usize>::new();

    let guard = epoch::pin();
    map.insert(42, 0, &guard);
    let old = map.insert(42, 1, &guard);
    assert_eq!(old, Some(&0));
    {
        let guard = epoch::pin();
        let e = map.get(&42, &guard).unwrap();
        assert_eq!(e, &1);
    }
}

#[test]
fn concurrent_insert() {
    let map = Arc::new(HashMap::<usize, usize>::new());

    let map1 = map.clone();
    let t1 = std::thread::spawn(move || {
        for i in 0..64 {
            map1.insert(i, 0, &epoch::pin());
        }
    });
    let map2 = map.clone();
    let t2 = std::thread::spawn(move || {
        for i in 0..64 {
            map2.insert(i, 1, &epoch::pin());
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    let guard = epoch::pin();
    for i in 0..64 {
        let v = map.get(&i, &guard).unwrap();
        assert!(v == &0 || v == &1);

        let kv = map.get_key_value(&i, &guard).unwrap();
        assert!(kv == (&i, &0) || kv == (&i, &1));
    }
}

#[test]
fn concurrent_remove() {
    let map = Arc::new(HashMap::<usize, usize>::new());

    {
        let guard = epoch::pin();
        for i in 0..64 {
            map.insert(i, i, &guard);
        }
    }

    let map1 = map.clone();
    let t1 = std::thread::spawn(move || {
        let guard = epoch::pin();
        for i in 0..64 {
            if let Some(v) = map1.remove(&i, &guard) {
                assert_eq!(v, &i);
            }
        }
    });
    let map2 = map.clone();
    let t2 = std::thread::spawn(move || {
        let guard = epoch::pin();
        for i in 0..64 {
            if let Some(v) = map2.remove(&i, &guard) {
                assert_eq!(v, &i);
            }
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // after joining the threads, the map should be empty
    let guard = epoch::pin();
    for i in 0..64 {
        assert!(map.get(&i, &guard).is_none());
    }
}

#[test]
fn current_kv_dropped() {
    let dropped1 = Arc::new(0);
    let dropped2 = Arc::new(0);

    let map = HashMap::<Arc<usize>, Arc<usize>>::new();

    map.insert(dropped1.clone(), dropped2.clone(), &epoch::pin());
    assert_eq!(Arc::strong_count(&dropped1), 2);
    assert_eq!(Arc::strong_count(&dropped2), 2);

    drop(map);

    // dropping the map should immediately drop (not deferred) all keys and values
    assert_eq!(Arc::strong_count(&dropped1), 1);
    assert_eq!(Arc::strong_count(&dropped2), 1);
}

#[test]
fn empty_maps_equal() {
    let map1 = HashMap::<usize, usize>::new();
    let map2 = HashMap::<usize, usize>::new();
    assert_eq!(map1, map2);
    assert_eq!(map2, map1);
}

#[test]
fn different_size_maps_not_equal() {
    let map1 = HashMap::<usize, usize>::new();
    let map2 = HashMap::<usize, usize>::new();
    {
        let guard = epoch::pin();
        map1.insert(1, 0, &guard);
        map1.insert(2, 0, &guard);
        map2.insert(1, 0, &guard);
    }

    assert_ne!(map1, map2);
    assert_ne!(map2, map1);
}

#[test]
fn same_values_equal() {
    let map1 = HashMap::<usize, usize>::new();
    let map2 = HashMap::<usize, usize>::new();
    {
        let guard = epoch::pin();
        map1.insert(1, 0, &guard);
        map2.insert(1, 0, &guard);
    }

    assert_eq!(map1, map2);
    assert_eq!(map2, map1);
}

#[test]
fn different_values_not_equal() {
    let map1 = HashMap::<usize, usize>::new();
    let map2 = HashMap::<usize, usize>::new();
    {
        let guard = epoch::pin();
        map1.insert(1, 0, &guard);
        map2.insert(1, 1, &guard);
    }

    assert_ne!(map1, map2);
    assert_ne!(map2, map1);
}

#[test]
#[ignore]
// ignored because we cannot control when destructors run
fn drop_value() {
    let dropped1 = Arc::new(0);
    let dropped2 = Arc::new(1);

    let map = HashMap::<usize, Arc<usize>>::new();

    map.insert(42, dropped1.clone(), &epoch::pin());
    assert_eq!(Arc::strong_count(&dropped1), 2);
    assert_eq!(Arc::strong_count(&dropped2), 1);

    map.insert(42, dropped2.clone(), &epoch::pin());
    assert_eq!(Arc::strong_count(&dropped2), 2);

    drop(map);

    // First NotifyOnDrop was dropped when it was replaced by the second
    assert_eq!(Arc::strong_count(&dropped1), 1);
    // Second NotifyOnDrop was dropped when the map was dropped
    assert_eq!(Arc::strong_count(&dropped2), 1);
}

#[test]
fn clone_map_empty() {
    let map = HashMap::<&'static str, u32>::new();
    let cloned_map = map.clone();
    assert_eq!(map.len(), cloned_map.len());
    assert_eq!(&map, &cloned_map);
    assert_eq!(cloned_map.len(), 0);
}

#[test]
// Test that same values exists in both maps (original and cloned)
fn clone_map_filled() {
    let map = HashMap::<&'static str, u32>::new();
    map.insert("FooKey", 0, &epoch::pin());
    map.insert("BarKey", 10, &epoch::pin());
    let cloned_map = map.clone();
    assert_eq!(map.len(), cloned_map.len());
    assert_eq!(&map, &cloned_map);

    // test that we are not mapping the same tables
    map.insert("NewItem", 100, &epoch::pin());
    assert_ne!(&map, &cloned_map);
}

#[test]
fn default() {
    let map: HashMap<usize, usize> = Default::default();

    let guard = epoch::pin();
    map.insert(42, 0, &guard);

    assert_eq!(map.get(&42, &guard), Some(&0));
}

#[test]
fn get_and() {
    let map: HashMap<usize, usize> = HashMap::new();

    let guard = epoch::pin();
    map.insert(42, 32, &guard);

    assert_eq!(map.get_and(&42, |value| *value + 10), Some(42));
}

#[test]
fn debug() {
    let map: HashMap<usize, usize> = HashMap::new();

    let guard = epoch::pin();
    map.insert(42, 0, &guard);
    map.insert(16, 8, &guard);

    let formatted = format!("{:?}", map);

    assert!(formatted == "{42: 0, 16: 8}" || formatted == "{16: 8, 42: 0}");
}

#[test]
fn extend() {
    let map: HashMap<usize, usize> = HashMap::new();

    let guard = epoch::pin();

    let mut entries: Vec<(usize, usize)> = vec![(42, 0), (16, 6), (38, 42)];
    entries.sort();

    (&map).extend(entries.clone().into_iter());

    let mut collected: Vec<(usize, usize)> = map
        .iter(&guard)
        .map(|(key, value)| (*key, *value))
        .collect();
    collected.sort();

    assert_eq!(entries, collected);
}

#[test]
fn extend_ref() {
    let map: HashMap<usize, usize> = HashMap::new();

    let mut entries: Vec<(&usize, &usize)> = vec![(&42, &0), (&16, &6), (&38, &42)];
    entries.sort();

    (&map).extend(entries.clone().into_iter());

    let guard = epoch::pin();
    let mut collected: Vec<(&usize, &usize)> = map.iter(&guard).collect();
    collected.sort();

    assert_eq!(entries, collected);
}

#[test]
fn from_iter_ref() {
    use std::iter::FromIterator;

    let mut entries: Vec<(&usize, &usize)> = vec![(&42, &0), (&16, &6), (&38, &42)];
    entries.sort();

    let map: HashMap<usize, usize> = HashMap::from_iter(entries.clone().into_iter());

    let guard = epoch::pin();
    let mut collected: Vec<(&usize, &usize)> = map.iter(&guard).collect();
    collected.sort();

    assert_eq!(entries, entries)
}

#[test]
fn from_iter_empty() {
    use std::iter::FromIterator;

    let entries: Vec<(usize, usize)> = Vec::new();
    let map: HashMap<usize, usize> = HashMap::from_iter(entries.into_iter());

    assert_eq!(map.len(), 0)
}
