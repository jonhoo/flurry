use flurry::*;
use std::iter::FromIterator;

const ITER: [(usize, &'static str); 5] = [
    (1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")
];

#[test]
fn test_from_iter() {
    let guard = unsafe { crossbeam::epoch::unprotected() };
    let map1 = from_iter_contron();
    let map2 = FlurryHashMap::from_iter(ITER.iter());

    // TODO: improve when `Map: Eq`
    let mut fst: Vec<_> = map1.iter(&guard).collect();
    let mut snd: Vec<_> = map2.iter(&guard).collect();
    fst.sort();
    snd.sort();
 
    assert_eq!(fst, snd);
}

fn from_iter_contron() -> FlurryHashMap<usize, &'static str> {
    let guard = unsafe { crossbeam::epoch::unprotected() };
    let map = FlurryHashMap::with_capacity(5);
    assert!(map.is_empty());

    for (key, value) in &ITER {
        map.insert(*key, *value, &guard);
    }

    assert!(!map.is_empty());
    assert_eq!(ITER.len(), map.len());
    map
}

