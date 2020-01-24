//! A concurrent hash set backed by HashMap.

use std::hash::Hash;

use crate::epoch::{self, Guard};
use crate::iter::Keys;
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
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::FlurryHashSet;
    ///
    /// let set: FlurryHashSet<i32> = FlurryHashSet::new();
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::FlurryHashSet;
    ///
    /// let set = FlurryHashSet::new();
    ///
    /// assert_eq!(set.insert(2), true);
    /// assert_eq!(set.insert(2), false);
    /// assert!(set.contains(&2));
    /// ```
    pub fn insert(&self, value: T) -> bool {
        let guard = epoch::pin();
        let old = self.map.insert(value, (), &guard);
        old.is_none()
    }

    /// Returns true if the set contains a value.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::FlurryHashSet;
    ///
    /// let set = FlurryHashSet::new();
    /// set.insert(2);
    ///
    /// assert!(set.contains(&2));
    /// assert!(!set.contains(&1));
    /// ```
    pub fn contains(&self, value: &T) -> bool {
        let guard = epoch::pin();
        self.map.contains_key(value, &guard)
    }

    /// Removes a value from the set.
    ///
    /// If the set did not have this value present, false is returned.
    ///
    /// If the set did have this value present, true is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::FlurryHashSet;
    ///
    /// let set = FlurryHashSet::new();
    /// set.insert(2);
    ///
    /// assert_eq!(set.remove(&2), true);
    /// assert!(!set.contains(&2));
    /// assert_eq!(set.remove(&2), false);
    /// ```
    pub fn remove(&self, value: &T) -> bool {
        let guard = epoch::pin();
        let removed = self.map.remove(value, &guard);
        removed.is_some()
    }

    /// An iterator over the set's values.
    ///
    /// See [`FlurryHashMap::keys`] for details.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::FlurryHashSet;
    ///
    /// let set = FlurryHashSet::new();
    /// set.insert(1);
    /// set.insert(2);
    ///
    /// let guard = flurry::epoch::pin();
    /// for x in set.iter(&guard) {
    ///     println!("{}", x);
    /// }
    /// ```
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> Keys<'g, T, ()> {
        self.map.keys(guard)
    }
}
