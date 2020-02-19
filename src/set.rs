//! A concurrent hash set.
//!
//! See `HashSet` for details.

use std::borrow::Borrow;
use std::hash::Hash;

use crate::epoch::{self, Guard};
use crate::iter::Keys;
use crate::HashMap;

/// A concurrent hash set implemented as a `FlurryHashMap` where the value is `()`.
///
/// # Examples
///
/// ```
/// use flurry::HashSet;
///
/// // Initialize a new hash set.
/// let books = HashSet::new();
///
/// // Add some books
/// books.insert("Fight Club");
/// books.insert("Three Men In A Raft");
/// books.insert("The Book of Dust");
/// books.insert("The Dry");
///
/// // Check for a specific one.
/// if !books.contains(&"The Drunken Botanist") {
///     println!("We don't have The Drunken Botanist.");
/// }
///
/// // Remove a book.
/// books.remove(&"Three Men In A Raft");
///
/// // Iterate over everything.
/// let guard = flurry::epoch::pin();
/// for book in books.iter(&guard) {
///     println!("{}", book);
/// }
/// ```
#[derive(Debug)]
pub struct HashSet<T: 'static, S = crate::DefaultHashBuilder>
where
    T: Sync + Send + Clone + Hash + Eq,
    S: std::hash::BuildHasher,
{
    map: HashMap<T, (), S>,
}

impl<T> HashSet<T, crate::DefaultHashBuilder>
where
    T: Sync + Send + Clone + Hash + Eq,
{
    /// Creates a new, empty set with the default initial table size (16).
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    ///
    /// let set: HashSet<i32> = HashSet::new();
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
    /// use flurry::HashSet;
    ///
    /// let set = HashSet::new();
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
    /// The value may be any borrowed form of the set's type, but `Hash` and `Eq` on the borrowed
    /// form must match those for the type.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    ///
    /// let set = HashSet::new();
    /// set.insert(2);
    ///
    /// assert!(set.contains(&2));
    /// assert!(!set.contains(&1));
    /// ```
    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let guard = epoch::pin();
        self.map.contains_key(value, &guard)
    }

    /// Removes a value from the set.
    ///
    /// The value may be any borrowed form of the set's type, but `Hash` and `Eq` on the borrowed
    /// form must match those for the type.
    ///
    /// If the set did not have this value present, false is returned.
    ///
    /// If the set did have this value present, true is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    ///
    /// let set = HashSet::new();
    /// set.insert(2);
    ///
    /// assert_eq!(set.remove(&2), true);
    /// assert!(!set.contains(&2));
    /// assert_eq!(set.remove(&2), false);
    /// ```
    pub fn remove<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
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
    /// use flurry::HashSet;
    ///
    /// let set = HashSet::new();
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
