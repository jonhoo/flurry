use flurry::HashSet;

#[test]
fn new() {
    let _set = HashSet::<usize, _>::new();
}

#[test]
fn insert() {
    let set = HashSet::new();
    let did_set = set.insert(42);
    assert!(did_set);

    let did_set = set.insert(42);
    assert!(!did_set);
}

#[test]
fn insert_contains() {
    let set = HashSet::new();
    set.insert(42);

    assert!(set.contains(&42));
    assert!(!set.contains(&43));
}
