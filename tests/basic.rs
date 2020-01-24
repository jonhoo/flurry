use crossbeam::epoch;
use flurry::*;
use std::sync::Arc;

#[test]
fn new() {
    let _map = FlurryHashMap::<usize, usize>::new();
}

#[test]
fn insert() {
    let map = FlurryHashMap::<usize, usize>::new();
    let guard = epoch::pin();
    let old = map.insert(42, 0, &guard);
    assert!(old.is_none());
}

#[test]
fn get_empty() {
    let map = FlurryHashMap::<usize, usize>::new();

    {
        let guard = epoch::pin();
        let e = map.get(&42, &guard);
        assert!(e.is_none());
    }
}

#[test]
fn get_key_value_empty() {
    let map = FlurryHashMap::<usize, usize>::new();

    {
        let guard = epoch::pin();
        let e = map.get_key_value(&42, &guard);
        assert!(e.is_none());
    }
}

#[test]
fn remove_empty() {
    let map = FlurryHashMap::<usize, usize>::new();

    {
        let guard = epoch::pin();
        let old = map.remove(&42, &guard);
        assert!(old.is_none());
    }
}

#[test]
fn insert_and_remove() {
    let map = FlurryHashMap::<usize, usize>::new();

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
    let map = FlurryHashMap::<usize, usize>::new();

    map.insert(42, 0, &epoch::pin());
    {
        let guard = epoch::pin();
        let e = map.get(&42, &guard).unwrap();
        assert_eq!(e, &0);
    }
}

#[test]
fn insert_and_get_key_value() {
    let map = FlurryHashMap::<usize, usize>::new();

    map.insert(42, 0, &epoch::pin());
    {
        let guard = epoch::pin();
        let e = map.get_key_value(&42, &guard).unwrap();
        assert_eq!(e, (&42, &0));
    }
}

#[test]
fn insert_in_same_bucket_and_get_distinct_entries() {
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

    let map = FlurryHashMap::<usize, usize, _>::with_hasher(OneBucketState);

    map.insert(42, 0, &epoch::pin());
    map.insert(50, 20, &epoch::pin());
    {
        let guard = epoch::pin();
        let e = map.get(&42, &guard).unwrap();
        assert_eq!(e, &0);
        let e = map.get(&50, &guard).unwrap();
        assert_eq!(e, &20);
    }
}

#[test]
fn update() {
    let map = FlurryHashMap::<usize, usize>::new();

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
    let map = Arc::new(FlurryHashMap::<usize, usize>::new());

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
    let map = Arc::new(FlurryHashMap::<usize, usize>::new());

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

    let map = FlurryHashMap::<Arc<usize>, Arc<usize>>::new();

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
    let map1 = FlurryHashMap::<usize, usize>::new();
    let map2 = FlurryHashMap::<usize, usize>::new();
    assert_eq!(map1, map2);
    assert_eq!(map2, map1);
}

#[test]
fn different_size_maps_not_equal() {
    let map1 = FlurryHashMap::<usize, usize>::new();
    let map2 = FlurryHashMap::<usize, usize>::new();
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
    let map1 = FlurryHashMap::<usize, usize>::new();
    let map2 = FlurryHashMap::<usize, usize>::new();
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
    let map1 = FlurryHashMap::<usize, usize>::new();
    let map2 = FlurryHashMap::<usize, usize>::new();
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

    let map = FlurryHashMap::<usize, Arc<usize>>::new();

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
