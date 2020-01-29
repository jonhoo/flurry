use crossbeam_epoch as epoch;
use flurry::*;
use std::sync::Arc;

// These tests all use `K = String` and `Q = str` for `Borrow`-based lookups

#[test]
fn get_empty() {
    let map = HashMap::<String, usize>::new();

    {
        let guard = epoch::pin();
        let e = map.get("foo", &guard);
        assert!(e.is_none());
    }
}

#[test]
fn remove_empty() {
    let map = HashMap::<String, usize>::new();

    {
        let guard = epoch::pin();
        let old = map.remove("foo", &guard);
        assert!(old.is_none());
    }
}

#[test]
fn insert_and_remove() {
    let map = HashMap::<String, usize>::new();

    {
        let guard = epoch::pin();
        map.insert("foo".to_string(), 0, &guard);
        let old = map.remove("foo", &guard).unwrap();
        assert_eq!(old, &0);
        assert!(map.get("foo", &guard).is_none());
    }
}

#[test]
fn insert_and_get() {
    let map = HashMap::<String, usize>::new();

    map.insert("foo".to_string(), 0, &epoch::pin());
    {
        let guard = epoch::pin();
        let e = map.get("foo", &guard).unwrap();
        assert_eq!(e, &0);
    }
}

#[test]
fn update() {
    let map = HashMap::<String, usize>::new();

    let guard = epoch::pin();
    map.insert("foo".to_string(), 0, &guard);
    let old = map.insert("foo".to_string(), 1, &guard);
    assert_eq!(old, Some(&0));
    {
        let guard = epoch::pin();
        let e = map.get("foo", &guard).unwrap();
        assert_eq!(e, &1);
    }
}

#[test]
fn concurrent_insert() {
    let map = Arc::new(HashMap::<String, usize>::new());
    let keys = Arc::new((0..64).map(|i| i.to_string()).collect::<Vec<_>>());

    let map1 = map.clone();
    let keys1 = keys.clone();
    let t1 = std::thread::spawn(move || {
        for key in keys1.iter() {
            map1.insert(key.clone(), 0, &epoch::pin());
        }
    });
    let map2 = map.clone();
    let keys2 = keys.clone();
    let t2 = std::thread::spawn(move || {
        for key in keys2.iter() {
            map2.insert(key.clone(), 1, &epoch::pin());
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    let guard = epoch::pin();
    for key in keys.iter() {
        let v = map.get(key.as_str(), &guard).unwrap();
        assert!(v == &0 || v == &1);
    }
}

#[test]
fn concurrent_remove() {
    let map = Arc::new(HashMap::<String, usize>::new());
    let keys = Arc::new((0..64).map(|i| i.to_string()).collect::<Vec<_>>());

    {
        let guard = epoch::pin();
        for (i, key) in keys.iter().enumerate() {
            map.insert(key.clone(), i, &guard);
        }
    }

    let map1 = map.clone();
    let keys1 = keys.clone();
    let t1 = std::thread::spawn(move || {
        let guard = epoch::pin();
        for (i, key) in keys1.iter().enumerate() {
            if let Some(v) = map1.remove(key.as_str(), &guard) {
                assert_eq!(v, &i);
            }
        }
    });
    let map2 = map.clone();
    let keys2 = keys.clone();
    let t2 = std::thread::spawn(move || {
        let guard = epoch::pin();
        for (i, key) in keys2.iter().enumerate() {
            if let Some(v) = map2.remove(key.as_str(), &guard) {
                assert_eq!(v, &i);
            }
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // after joining the threads, the map should be empty
    let guard = epoch::pin();
    for key in keys.iter() {
        assert!(map.get(key.as_str(), &guard).is_none());
    }
}
