use flurry::HashSet;

#[test]
fn new() {
    let _set = HashSet::<usize, _>::new();
}

#[test]
fn insert() {
    let set = HashSet::new();
    let guard = flurry::epoch::pin();
    let did_set = set.insert(42, &guard);
    assert!(did_set);

    let did_set = set.insert(42, &guard);
    assert!(!did_set);
}

#[test]
fn insert_contains() {
    let set = HashSet::new();
    let guard = flurry::epoch::pin();
    set.insert(42, &guard);

    assert!(set.contains(&42, &guard));
    assert!(!set.contains(&43, &guard));
}
