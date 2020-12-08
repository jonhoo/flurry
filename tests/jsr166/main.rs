use flurry::*;
use std::iter::FromIterator;

const ITER: [(usize, &'static str); 5] = [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")];

#[test]
fn test_from_iter() {
    let map1 = from_iter_contron();
    let map2: HashMap<_, _> = HashMap::from_iter(ITER.iter());
    assert_eq!(map1, map2);
}

fn from_iter_contron() -> HashMap<usize, &'static str> {
    let map = HashMap::with_capacity(5);
    {
        let guard = map.guard();
        assert!(map.is_empty());

        for (key, value) in &ITER {
            map.insert(*key, *value, &guard);
        }
    }

    assert!(!map.is_empty());
    assert_eq!(ITER.len(), map.len());
    map
}

fn map5() -> HashMap<isize, String> {
    let map = HashMap::new();
    assert!(map.is_empty());
    {
        let guard = map.guard();
        map.insert(1, "A".to_owned(), &guard);
        map.insert(2, "B".to_owned(), &guard);
        map.insert(3, "C".to_owned(), &guard);
        map.insert(4, "D".to_owned(), &guard);
        map.insert(5, "E".to_owned(), &guard);
    }
    assert!(!map.is_empty());
    assert_eq!(map.len(), 5);
    map
}

// remove removes the correct key-value pair from the map
#[test]
fn test_remove() {
    let map = map5();
    let guard = map.guard();
    map.remove(&5, &guard);
    assert_eq!(map.len(), 4);
    assert!(!map.contains_key(&5, &guard));
}
