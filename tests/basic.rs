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
    let old = map.insert(42, 0);
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
fn insert_and_get() {
    let map = FlurryHashMap::<usize, usize>::new();

    map.insert(42, 0);
    {
        let guard = epoch::pin();
        let e = map.get(&42, &guard).unwrap();
        // safety: the map guarantees that it will not free something there is a Shared to
        assert_eq!(unsafe { e.deref() }, &0);
    }
}

#[test]
fn update() {
    let map = FlurryHashMap::<usize, usize>::new();

    map.insert(42, 0);
    let old = map.insert(42, 1);
    assert!(old.is_some());
    {
        let guard = epoch::pin();
        let e = map.get(&42, &guard).unwrap();
        // safety: the map guarantees that it will not free something there is a Shared to
        assert_eq!(unsafe { e.deref() }, &1);
    }
}

#[test]
fn current_kv_dropped() {
    let dropped1 = Arc::new(0);
    let dropped2 = Arc::new(0);

    let map = FlurryHashMap::<Arc<usize>, Arc<usize>>::new();

    map.insert(dropped1.clone(), dropped2.clone());
    assert_eq!(Arc::strong_count(&dropped1), 2);
    assert_eq!(Arc::strong_count(&dropped2), 2);

    drop(map);

    // dropping the map should immediately drop (not deferred) all keys and values
    assert_eq!(Arc::strong_count(&dropped1), 1);
    assert_eq!(Arc::strong_count(&dropped2), 1);
}

#[test]
#[ignore]
// ignored because we cannot control when destructors run
fn drop_value() {
    let dropped1 = Arc::new(0);
    let dropped2 = Arc::new(1);

    let map = FlurryHashMap::<usize, Arc<usize>>::new();

    map.insert(42, dropped1.clone());
    assert_eq!(Arc::strong_count(&dropped1), 2);
    assert_eq!(Arc::strong_count(&dropped2), 1);

    map.insert(42, dropped2.clone());
    assert_eq!(Arc::strong_count(&dropped2), 2);

    drop(map);

    // First NotifyOnDrop was dropped when it was replaced by the second
    assert_eq!(Arc::strong_count(&dropped1), 1);
    // Second NotifyOnDrop was dropped when the map was dropped
    assert_eq!(Arc::strong_count(&dropped2), 1);
}
