use flurry::*;
use std::iter::FromIterator;

const ITER: [(usize, &'static str); 5] = [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")];

#[test]
fn test_from_iter() {
    let guard = unsafe { crossbeam_epoch::unprotected() };
    let map1 = from_iter_contron();
    let map2: HashMap<_, _> = HashMap::from_iter(ITER.iter());

    // TODO: improve when `Map: Eq`
    let mut fst: Vec<_> = map1.iter(&guard).collect();
    let mut snd: Vec<_> = map2.iter(&guard).collect();
    fst.sort();
    snd.sort();

    assert_eq!(fst, snd);
}

fn from_iter_contron() -> HashMap<usize, &'static str> {
    let guard = unsafe { crossbeam_epoch::unprotected() };
    let map = HashMap::with_capacity(5);
    assert!(map.is_empty());

    for (key, value) in &ITER {
        map.insert(*key, *value, &guard);
    }

    assert!(!map.is_empty());
    assert_eq!(ITER.len(), map.len());
    map
}

fn map5() -> HashMap<isize, String> {
    let map = HashMap::new();
    // TODO: add is_empty check once method exists
    // assert!(map.is_empty());
    let guard = epoch::pin();
    map.insert(1, "A".to_owned(), &guard);
    map.insert(2, "B".to_owned(), &guard);
    map.insert(3, "C".to_owned(), &guard);
    map.insert(4, "D".to_owned(), &guard);
    map.insert(5, "E".to_owned(), &guard);
    // TODO: add is_empty and len check once methods exist
    // assert!(!map.is_empty());
    // assert_eq!(map.len(), 5);
    map
}

// remove removes the correct key-value pair from the map
#[test]
fn test_remove() {
    let map = map5();
    let guard = epoch::pin();
    map.remove(&5, &guard);
    // TODO: add len check once method exists
    // assert_eq!(map.len(), 4);
    assert!(!map.contains_key(&5));
}
