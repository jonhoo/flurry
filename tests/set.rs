use flurry::FlurryHashSet;

#[test]
fn new() {
    let _set = FlurryHashSet::<usize>::new();
}

#[test]
fn insert() {
    let set = FlurryHashSet::<usize>::new();
    let did_set = set.insert(42);
    assert!(did_set);

    let did_set = set.insert(42);
    assert!(!did_set);
}

#[test]
fn insert_contains() {
    let set = FlurryHashSet::<usize>::new();
    set.insert(42);

    assert!(set.contains(&42));
    assert!(!set.contains(&43));
}
