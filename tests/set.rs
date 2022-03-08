use flurry::HashSet;
use std::sync::Arc;

#[test]
fn new() {
    let _set = HashSet::<usize>::new();
}

#[test]
fn insert() {
    let set = HashSet::new();
    let guard = set.guard();
    let did_set = set.insert(42, &guard);
    assert!(did_set);

    let did_set = set.insert(42, &guard);
    assert!(!did_set);
}

#[test]
fn no_contains() {
    let set = HashSet::<usize>::new();

    {
        let guard = set.guard();
        let contained = set.contains(&42, &guard);
        assert!(!contained);
    }
}

#[test]
fn get_no_contains() {
    let set = HashSet::<usize>::new();

    {
        let guard = set.guard();
        let e = set.get(&42, &guard);
        assert!(e.is_none());
    }
}

#[test]
fn remove_empty() {
    let set = HashSet::<usize>::new();

    {
        let guard = set.guard();
        let removed = set.remove(&42, &guard);
        assert!(!removed);
    }
}

#[test]
fn insert_and_remove() {
    let set = HashSet::new();

    {
        let guard = set.guard();
        set.insert(42, &guard);
        let removed = set.remove(&42, &guard);
        assert!(removed);
        assert!(!set.contains(&42, &guard));
    }
}

#[test]
fn insert_and_contains() {
    let set = HashSet::new();

    set.insert(42, &set.guard());
    {
        let guard = set.guard();
        let got = set.contains(&42, &guard);
        assert!(got);
    }
}

#[test]
fn insert_and_get() {
    let set = HashSet::new();

    set.insert(42, &set.guard());
    {
        let guard = set.guard();
        let e = set.get(&42, &guard).unwrap();
        assert_eq!(e, &42);
    }
}

#[test]
fn update() {
    let set = HashSet::new();

    let guard = set.guard();
    set.insert(42, &guard);
    let was_new = set.insert(42, &guard);
    assert!(!was_new);
    assert!(set.contains(&42, &guard));
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_insert() {
    let set = Arc::new(HashSet::<usize>::new());

    let set1 = set.clone();
    let t1 = std::thread::spawn(move || {
        for i in 0..64 {
            set1.insert(i, &set1.guard());
        }
    });
    let set2 = set.clone();
    let t2 = std::thread::spawn(move || {
        for i in 0..64 {
            set2.insert(i, &set2.guard());
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    let guard = set.guard();
    for i in 0..64 {
        assert!(set.contains(&i, &guard));

        let key = set.get(&i, &guard).unwrap();
        assert_eq!(key, &i);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_remove() {
    let set = Arc::new(HashSet::<usize>::new());

    {
        let guard = set.guard();
        for i in 0..64 {
            set.insert(i, &guard);
        }
    }

    let set1 = set.clone();
    let t1 = std::thread::spawn(move || {
        let guard = set1.guard();
        for i in 0..64 {
            set1.remove(&i, &guard);
        }
    });
    let set2 = set.clone();
    let t2 = std::thread::spawn(move || {
        let guard = set2.guard();
        for i in 0..64 {
            set2.remove(&i, &guard);
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // after joining the threads, the map should be empty
    let guard = set.guard();
    for i in 0..64 {
        assert!(!set.contains(&i, &guard));
    }
}

#[test]
fn empty_sets_equal() {
    let set1 = HashSet::<usize>::new();
    let set2 = HashSet::<usize>::new();
    assert_eq!(set1, set2);
    assert_eq!(set2, set1);
}

#[test]
fn different_size_maps_not_equal() {
    let set1 = HashSet::<usize>::new();
    let set2 = HashSet::<usize>::new();
    {
        set1.pin().insert(1);
        set1.pin().insert(2);
        set2.pin().insert(1);
    }

    assert_ne!(set1, set2);
    assert_ne!(set2, set1);
}

#[test]
fn same_values_equal() {
    let set1 = HashSet::<usize>::new();
    let set2 = HashSet::<usize>::new();
    {
        set1.pin().insert(1);
        set2.pin().insert(1);
    }

    assert_eq!(set1, set2);
    assert_eq!(set2, set1);
}

#[test]
fn different_values_not_equal() {
    let set1 = HashSet::<usize>::new();
    let set2 = HashSet::<usize>::new();
    {
        set1.pin().insert(1);
        set2.pin().insert(2);
    }

    assert_ne!(set1, set2);
    assert_ne!(set2, set1);
}

#[test]
fn clone_set_empty() {
    let set = HashSet::<&'static str>::new();
    let cloned_set = set.clone();
    assert_eq!(set.len(), cloned_set.len());
    assert_eq!(&set, &cloned_set);
    assert_eq!(cloned_set.len(), 0);
}

#[test]
// Test that same values exists in both maps (original and cloned)
fn clone_set_filled() {
    let set = HashSet::<&'static str>::new();
    set.insert("FooKey", &set.guard());
    set.insert("BarKey", &set.guard());
    let cloned_set = set.clone();
    assert_eq!(set.len(), cloned_set.len());
    assert_eq!(&set, &cloned_set);

    // test that we are not mapping the same tables
    set.insert("NewItem", &set.guard());
    assert_ne!(&set, &cloned_set);
}

#[test]
fn default() {
    let set: HashSet<usize> = Default::default();

    let guard = set.guard();
    set.insert(42, &guard);

    assert!(set.contains(&42, &guard));
}

#[test]
fn debug() {
    let set: HashSet<usize> = HashSet::new();

    let guard = set.guard();
    set.insert(42, &guard);
    set.insert(16, &guard);

    let formatted = format!("{:?}", set);

    assert!(formatted == "{42, 16}" || formatted == "{16, 42}");
}

#[test]
fn extend() {
    let set: HashSet<usize> = HashSet::new();

    let guard = set.guard();

    let mut entries = vec![42, 16, 38];
    entries.sort();

    (&set).extend(entries.clone().into_iter());

    let mut collected: Vec<_> = set.iter(&guard).map(|value| *value).collect();
    collected.sort();

    assert_eq!(entries, collected);
}

#[test]
fn extend_ref() {
    let set: HashSet<usize> = HashSet::new();

    let mut entries = vec![&42, &16, &38];
    entries.sort();

    (&set).extend(entries.clone().into_iter());

    let guard = set.guard();
    let mut collected: Vec<_> = set.iter(&guard).collect();
    collected.sort();

    assert_eq!(entries, collected);
}

#[test]
fn from_iter_ref() {
    use std::iter::FromIterator;

    let mut entries: Vec<_> = vec![&42, &16, &38];
    entries.sort();

    let set: HashSet<usize> = HashSet::from_iter(entries.clone().into_iter());

    let guard = set.guard();
    let mut collected: Vec<_> = set.iter(&guard).collect();
    collected.sort();

    assert_eq!(entries, entries)
}

#[test]
fn from_iter_empty() {
    use std::iter::FromIterator;

    let set: HashSet<_> = HashSet::from_iter(std::iter::empty::<usize>());

    assert_eq!(set.len(), 0)
}
