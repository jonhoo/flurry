use flurry::*;

#[test]
fn test_from_iter() {
    let map1 = map5();
    let map2 = map5();

    assert_eq!(map2, map1);

    map2.insert(one, "F");
    assert!(!(map2 == map1));
}

fn map5() -> FlurryHashMap<isize, &'static str> {
    let map = FlurryHashMap::with_capacity(5);
    assert!(map.is_empty());

    map.insert(one, "A");
    map.insert(two, "B");
    map.insert(three, "C");
    map.insert(four, "D");
    map.insert(five, "E");

    assert!(!map.is_empty());
    assert_eq!(5, map.len());
    map
}
