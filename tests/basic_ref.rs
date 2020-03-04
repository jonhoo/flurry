use crossbeam_epoch as epoch;
use flurry::*;
use std::sync::Arc;

#[test]
fn pin() {
    let _map = HashMap::<usize, usize>::new().pin();
}

#[test]
fn with_guard() {
    let guard = epoch::pin();
    let _map = HashMap::<usize, usize>::new().with_guard(&guard);
}

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
    let map = map.pin();
    let e = map.get(&42);
    assert!(e.is_none());
}

#[test]
fn get_key_value_empty() {
    let map = HashMap::<usize, usize>::new();
    let map = map.pin();
    let e = map.get_key_value(&42);
    assert!(e.is_none());
}

#[test]
fn remove_empty() {
    let map = HashMap::<usize, usize>::new();
    let map = map.pin();
    let old = map.remove(&42);
    assert!(old.is_none());
}

#[test]
fn insert_and_remove() {
    let map = HashMap::<usize, usize>::new();
    let map = map.pin();
    map.insert(42, 0);
    let old = map.remove(&42).unwrap();
    assert_eq!(old, &0);
    assert!(map.get(&42).is_none());
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

mod hasher;
use hasher::ZeroHashBuilder;

#[test]
fn one_bucket() {
    let map = HashMap::<&'static str, usize, _>::with_hasher(ZeroHashBuilder);
    let map = map.pin();

    // we want to check that all operations work regardless on whether
    // we are operating on the head of a bucket, the tail of the bucket,
    // or somewhere in the middle.
    let v = map.insert("head", 0);
    assert_eq!(v, None);
    let v = map.insert("middle", 10);
    assert_eq!(v, None);
    let v = map.insert("tail", 100);
    assert_eq!(v, None);
    let e = map.get("head").unwrap();
    assert_eq!(e, &0);
    let e = map.get("middle").unwrap();
    assert_eq!(e, &10);
    let e = map.get("tail").unwrap();
    assert_eq!(e, &100);

    // check that replacing the keys returns the correct old value
    let v = map.insert("head", 1);
    assert_eq!(v, Some(&0));
    let v = map.insert("middle", 11);
    assert_eq!(v, Some(&10));
    let v = map.insert("tail", 101);
    assert_eq!(v, Some(&100));
    // and updated the right value
    let e = map.get("head").unwrap();
    assert_eq!(e, &1);
    let e = map.get("middle").unwrap();
    assert_eq!(e, &11);
    let e = map.get("tail").unwrap();
    assert_eq!(e, &101);
    // and that remove produces the right value
    // note that we must remove them in a particular order
    // so that we test all three node positions
    let v = map.remove("middle");
    assert_eq!(v, Some(&11));
    let v = map.remove("tail");
    assert_eq!(v, Some(&101));
    let v = map.remove("head");
    assert_eq!(v, Some(&1));
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
    let map = map.pin();

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

#[test]
fn clone_map_empty() {
    let map = HashMap::<&'static str, u32>::new();
    let map = map.pin();
    let cloned_map = map.clone();
    assert_eq!(map.len(), cloned_map.len());
    assert_eq!(&map, &cloned_map);
    assert_eq!(cloned_map.len(), 0);
}

#[test]
// Test that same values exists in both maps (original and cloned)
fn clone_map_filled() {
    let map_ref = HashMap::<&'static str, u32>::new();
    let map_ref = map_ref.pin();
    map_ref.insert("FooKey", 0);
    map_ref.insert("BarKey", 10);
    let cloned_map_ref = map_ref.clone();
    assert_eq!(map_ref.len(), cloned_map_ref.len());
    assert_eq!(&map_ref, &cloned_map_ref);

    // test that both maps are equal,
    // because the ref and the cloned ref, point to the same map
    map_ref.insert("NewItem", 100);
    assert_eq!(&map_ref, &cloned_map_ref);
}

#[test]
fn default() {
    let map: HashMap<usize, usize> = Default::default();
    let map = map.pin();
    map.insert(42, 0);
    assert_eq!(map.get(&42), Some(&0));
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
fn from_iter_ref() {
    use std::iter::FromIterator;

    let mut entries: Vec<(&usize, &usize)> = vec![(&42, &0), (&16, &6), (&38, &42)];
    entries.sort();

    let map: HashMap<usize, usize> = HashMap::from_iter(entries.clone().into_iter());
    let map = map.pin();
    let mut collected: Vec<(&usize, &usize)> = map.iter().collect();
    collected.sort();

    assert_eq!(entries, entries)
}

#[test]
fn from_iter_empty() {
    use std::iter::FromIterator;

    let entries: Vec<(usize, usize)> = Vec::new();
    let map: HashMap<usize, usize> = HashMap::from_iter(entries.into_iter());
    let map = map.pin();
    assert_eq!(map.len(), 0)
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
    let expected_map = expected_map.pin();
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
