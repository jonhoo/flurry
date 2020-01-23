use flurry::*;

fn map5() -> FlurryHashMap<isize, String> {
    let map = FlurryHashMap::new();
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
