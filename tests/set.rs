use flurry::HashSet;

#[test]
fn new() {
    let _set = HashSet::<usize>::new();
}

#[test]
fn insert() {
    let set = HashSet::<usize>::new();
    let did_set = set.insert(42);
    assert!(did_set);

    let did_set = set.insert(42);
    assert!(!did_set);
}

#[test]
fn insert_contains() {
    let set = HashSet::<usize>::new();
    set.insert(42);

    assert!(set.contains(&42));
    assert!(!set.contains(&43));
}
