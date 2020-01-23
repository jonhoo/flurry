use flurry::*;
use std::iter::FromIterator;

#[test]
fn test_from_iter() {
    let gurad = unsafe { crossbeam::epoch::unprotected() };
    let map1 = map5();
    let map2 = FlurryHashMap::from_iter(map1.iter(&guard));

    assert_eq!(map2, map1);

    map2.insert(1, "F");
    assert!(!(map2 == map1));
}

fn map5() -> FlurryHashMap<isize, &'static str> {
    let map = FlurryHashMap::with_capacity(5);
    assert!(map.is_empty());

    map.insert(1, "A");
    map.insert(2, "B");
    map.insert(3, "C");
    map.insert(4, "D");
    map.insert(5, "E");

    assert!(!map.is_empty());
    assert_eq!(5, map.len());
    map
}
