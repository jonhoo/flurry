//! A concurrent hash set backed by HashMap.

use std::hash::Hash;

use crate::epoch;
use crate::HashMap;

/// A concurrent hash set.
#[derive(Debug)]
pub struct FlurryHashSet<T: 'static, S = crate::DefaultHashBuilder>
where
    T: Sync + Send + Clone + Hash + Eq,
    S: std::hash::BuildHasher,
{
    map: HashMap<T, (), S>,
}

impl<T> FlurryHashSet<T, crate::DefaultHashBuilder>
where
    T: Sync + Send + Clone + Hash + Eq,
{
    /// Creates a new, empty set with the default initial table size (16).
    pub fn new() -> Self {
        Self {
            map: HashMap::<T, ()>::new(),
        }
    }

    /// Adds a value to the set.
    ///
    /// If the set did not have this value present, true is returned.
    ///
    /// If the set did have this value present, false is returned.
    pub fn insert(&self, value: T) -> bool {
        let guard = epoch::pin();
        let old = self.map.insert(value, (), &guard);
        old.is_none()
    }

    /// Returns true if the set contains a value.
    pub fn contains(&self, value: &T) -> bool {
        let guard = epoch::pin();
        self.map.contains_key(value, &guard)
    }
}
