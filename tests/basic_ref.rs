use flurry::HashMap;
use std::sync::Arc;

#[test]
fn clear() {
    let map = HashMap::<usize, usize>::new();
    let map = map.pin();
    {
        map.insert(0, 1);
        map.insert(1, 1);
        map.insert(2, 1);
        map.insert(3, 1);
        map.insert(4, 1);
    }
    map.clear();
    assert!(map.is_empty());
}

#[test]
fn insert() {
    let map = HashMap::<usize, usize>::new();
    let map = map.pin();

    let old = map.insert(42, 0);
    assert!(old.is_none());
}

#[test]
fn get_empty() {
    let map = HashMap::<usize, usize>::new();

    {
        let map = map.pin();
        let e = map.get(&42);
        assert!(e.is_none());
    }
}

#[test]
fn get_key_value_empty() {
    let map = HashMap::<usize, usize>::new();

    {
        let map = map.pin();
        let e = map.get_key_value(&42);
        assert!(e.is_none());
    }
}

#[test]
fn remove_empty() {
    let map = HashMap::<usize, usize>::new();

    {
        let map = map.pin();
        let old = map.remove(&42);
        assert!(old.is_none());
    }
}

#[test]
fn insert_and_remove() {
    let map = HashMap::<usize, usize>::new();

    {
        let map = map.pin();
        map.insert(42, 0);
        let old = map.remove(&42).unwrap();
        assert_eq!(old, &0);
        assert!(map.get(&42).is_none());
    }
}

#[test]
fn insert_and_get() {
    let map = HashMap::<usize, usize>::new();

    map.pin().insert(42, 0);
    {
        let map = map.pin();
        let e = map.get(&42).unwrap();
        assert_eq!(e, &0);
    }
}

#[test]
fn insert_and_get_key_value() {
    let map = HashMap::<usize, usize>::new();

    map.pin().insert(42, 0);
    {
        let map = map.pin();
        let e = map.get_key_value(&42).unwrap();
        assert_eq!(e, (&42, &0));
    }
}

#[test]
fn update() {
    let map = HashMap::<usize, usize>::new();

    let map1 = map.pin();
    map1.insert(42, 0);
    let old = map1.insert(42, 1);
    assert_eq!(old, Some(&0));
    {
        let map2 = map.pin();
        let e = map2.get(&42).unwrap();
        assert_eq!(e, &1);
    }
}

#[test]
fn compute_if_present() {
    let map = HashMap::<usize, usize>::new();

    let map1 = map.pin();
    map1.insert(42, 0);
    let new = map1.compute_if_present(&42, |_, v| Some(v + 1));
    assert_eq!(new, Some(&1));
    {
        let map2 = map.pin();
        let e = map2.get(&42).unwrap();
        assert_eq!(e, &1);
    }
}

#[test]
fn compute_if_present_empty() {
    let map = HashMap::<usize, usize>::new();

    let map1 = map.pin();
    let new = map1.compute_if_present(&42, |_, v| Some(v + 1));
    assert!(new.is_none());
    {
        let map2 = map.pin();
        assert!(map2.get(&42).is_none());
    }
}

#[test]
fn compute_if_present_remove() {
    let map = HashMap::<usize, usize>::new();

    let map1 = map.pin();
    map1.insert(42, 0);
    let new = map1.compute_if_present(&42, |_, _| None);
    assert!(new.is_none());
    {
        let map2 = map.pin();
        assert!(map2.get(&42).is_none());
    }
}

#[test]
fn concurrent_insert() {
    let map = Arc::new(HashMap::<usize, usize>::new());

    let map1 = map.clone();
    let t1 = std::thread::spawn(move || {
        for i in 0..64 {
            map1.pin().insert(i, 0);
        }
    });
    let map2 = map.clone();
    let t2 = std::thread::spawn(move || {
        for i in 0..64 {
            map2.pin().insert(i, 0);
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    let map = map.pin();
    for i in 0..64 {
        let v = map.get(&i).unwrap();
        assert!(v == &0 || v == &1);

        let kv = map.get_key_value(&i).unwrap();
        assert!(kv == (&i, &0) || kv == (&i, &1));
    }
}

#[test]
fn concurrent_remove() {
    let map = Arc::new(HashMap::<usize, usize>::new());

    {
        let map = map.pin();
        for i in 0..64 {
            map.insert(i, i);
        }
    }

    let map1 = map.clone();
    let t1 = std::thread::spawn(move || {
        let map1 = map1.pin();
        for i in 0..64 {
            if let Some(v) = map1.remove(&i) {
                assert_eq!(v, &i);
            }
        }
    });
    let map2 = map.clone();
    let t2 = std::thread::spawn(move || {
        let map2 = map2.pin();
        for i in 0..64 {
            if let Some(v) = map2.remove(&i) {
                assert_eq!(v, &i);
            }
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // after joining the threads, the map should be empty
    let map = map.pin();
    for i in 0..64 {
        assert!(map.get(&i).is_none());
    }
}

#[test]
fn concurrent_compute_if_present() {
    let map = Arc::new(HashMap::<usize, usize>::new());

    {
        let map = map.pin();
        for i in 0..64 {
            map.insert(i, i);
        }
    }

    let map1 = map.clone();
    let t1 = std::thread::spawn(move || {
        let map1 = map1.pin();
        for i in 0..64 {
            let new = map1.compute_if_present(&i, |_, _| None);
            assert!(new.is_none());
        }
    });
    let map2 = map.clone();
    let t2 = std::thread::spawn(move || {
        let map2 = map2.pin();
        for i in 0..64 {
            let new = map2.compute_if_present(&i, |_, _| None);
            assert!(new.is_none());
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // after joining the threads, the map should be empty
    let map = map.pin();
    for i in 0..64 {
        assert!(map.get(&i).is_none());
    }
}

#[test]
fn current_kv_dropped() {
    let dropped1 = Arc::new(0);
    let dropped2 = Arc::new(0);

    let map = HashMap::<Arc<usize>, Arc<usize>>::new();

    map.pin().insert(dropped1.clone(), dropped2.clone());
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
    assert_eq!(map1, map2.pin());
    assert_eq!(map1.pin(), map2);
    assert_eq!(map1.pin(), map2.pin());
    assert_eq!(map2.pin(), map1.pin());
}

#[test]
fn different_size_maps_not_equal() {
    let map1 = HashMap::<usize, usize>::new();
    let map2 = HashMap::<usize, usize>::new();
    {
        let map1 = map1.pin();
        let map2 = map2.pin();
        map1.insert(1, 0);
        map1.insert(2, 0);
        map2.insert(1, 0);
    }

    assert_ne!(map1, map2.pin());
    assert_ne!(map1.pin(), map2);
    assert_ne!(map1.pin(), map2.pin());
    assert_ne!(map2.pin(), map1.pin());
}

#[test]
fn same_values_equal() {
    let map1 = HashMap::<usize, usize>::new();
    let map2 = HashMap::<usize, usize>::new();
    {
        let map1 = map1.pin();
        let map2 = map2.pin();
        map1.insert(1, 0);
        map2.insert(1, 0);
    }

    assert_eq!(map1, map2.pin());
    assert_eq!(map1.pin(), map2);
    assert_eq!(map1.pin(), map2.pin());
    assert_eq!(map2.pin(), map1.pin());
}

#[test]
fn different_values_not_equal() {
    let map1 = HashMap::<usize, usize>::new();
    let map2 = HashMap::<usize, usize>::new();
    {
        let map1 = map1.pin();
        let map2 = map2.pin();
        map1.insert(1, 0);
        map2.insert(1, 1);
    }

    assert_ne!(map1, map2.pin());
    assert_ne!(map1.pin(), map2);
    assert_ne!(map1.pin(), map2.pin());
    assert_ne!(map2.pin(), map1.pin());
}

#[test]
#[ignore]
// ignored because we cannot control when destructors run
fn drop_value() {
    let dropped1 = Arc::new(0);
    let dropped2 = Arc::new(1);

    let map = HashMap::<usize, Arc<usize>>::new();

    map.pin().insert(42, dropped1.clone());
    assert_eq!(Arc::strong_count(&dropped1), 2);
    assert_eq!(Arc::strong_count(&dropped2), 1);

    map.pin().insert(42, dropped2.clone());
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
    let map = map.pin();
    let cloned_map = map.clone(); // another ref to the same map
    assert_eq!(map.len(), cloned_map.len());
    assert_eq!(map, cloned_map);
    assert_eq!(cloned_map.len(), 0);
}

#[test]
// Test that same values exists in both refs (original and cloned)
fn clone_map_filled() {
    let map = HashMap::<&'static str, u32>::new();
    let map = map.pin();
    map.insert("FooKey", 0);
    map.insert("BarKey", 10);
    let cloned_map = map.clone(); // another ref to the same map
    assert_eq!(map.len(), cloned_map.len());
    assert_eq!(map, cloned_map);

    // test that we are mapping the same tables
    map.insert("NewItem", 100);
    assert_eq!(map, cloned_map);
}

#[test]
fn debug() {
    let map: HashMap<usize, usize> = HashMap::new();
    let map = map.pin();

    map.insert(42, 0);
    map.insert(16, 8);

    let formatted = format!("{:?}", map);

    assert!(formatted == "{42: 0, 16: 8}" || formatted == "{16: 8, 42: 0}");
}

#[test]
fn retain_empty() {
    let map = HashMap::<&'static str, u32>::new();
    let map = map.pin();
    map.retain(|_, _| false);
    assert_eq!(map.len(), 0);
}

#[test]
fn retain_all_false() {
    let map: HashMap<u32, u32> = (0..10 as u32).map(|x| (x, x)).collect();
    let map = map.pin();
    map.retain(|_, _| false);
    assert_eq!(map.len(), 0);
}

#[test]
fn retain_all_true() {
    let size = 10usize;
    let map: HashMap<usize, usize> = (0..size).map(|x| (x, x)).collect();
    let map = map.pin();
    map.retain(|_, _| true);
    assert_eq!(map.len(), size);
}

#[test]
fn retain_some() {
    let map: HashMap<u32, u32> = (0..10).map(|x| (x, x)).collect();
    let map = map.pin();
    let expected_map: HashMap<u32, u32> = (5..10).map(|x| (x, x)).collect();
    map.retain(|_, v| *v >= 5);
    assert_eq!(map.len(), 5);
    assert_eq!(map, expected_map);
}

#[test]
fn retain_force_empty() {
    let map = HashMap::<&'static str, u32>::new();
    let map = map.pin();
    map.retain_force(|_, _| false);
    assert_eq!(map.len(), 0);
}

#[test]
fn retain_force_some() {
    let map: HashMap<u32, u32> = (0..10).map(|x| (x, x)).collect();
    let map = map.pin();
    let expected_map: HashMap<u32, u32> = (5..10).map(|x| (x, x)).collect();
    map.retain_force(|_, v| *v >= 5);
    assert_eq!(map.len(), 5);
    assert_eq!(map, expected_map);
}
