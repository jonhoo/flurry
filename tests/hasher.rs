use flurry::{DefaultHashBuilder, HashMap};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};

#[derive(Default)]
pub struct ZeroHasher;

pub struct ZeroHashBuilder;

impl Hasher for ZeroHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}

impl BuildHasher for ZeroHashBuilder {
    type Hasher = ZeroHasher;

    fn build_hasher(&self) -> ZeroHasher {
        ZeroHasher
    }
}

fn check<S: BuildHasher + Default>() {
    let range = if cfg!(miri) { 0..16 } else { 0..1000 };
    let map = HashMap::<i32, i32, S>::default();
    let guard = map.guard();
    for i in range.clone() {
        map.insert(i, i, &guard);
    }

    assert!(!map.contains_key(&i32::min_value(), &guard));
    assert!(!map.contains_key(&(range.start - 1), &guard));
    for i in range.clone() {
        assert!(map.contains_key(&i, &guard));
    }
    assert!(!map.contains_key(&range.end, &guard));
    assert!(!map.contains_key(&i32::max_value(), &guard));
}

#[test]
fn test_default_hasher() {
    check::<DefaultHashBuilder>();
}

#[test]
fn test_zero_hasher() {
    check::<BuildHasherDefault<ZeroHasher>>();
}

#[test]
fn test_max_hasher() {
    #[derive(Default)]
    struct MaxHasher;

    impl Hasher for MaxHasher {
        fn finish(&self) -> u64 {
            u64::max_value()
        }
        fn write(&mut self, _: &[u8]) {}
    }

    check::<BuildHasherDefault<MaxHasher>>();
}
