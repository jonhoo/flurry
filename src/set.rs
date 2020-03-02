//! A concurrent hash set.
//!
//! See `HashSet` for details.

use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};

use crate::epoch::Guard;
use crate::iter::Keys;
use crate::HashMap;

/// A concurrent hash set implemented as a `HashMap` where the value is `()`.
///
/// # Examples
///
/// ```
/// use flurry::HashSet;
///
/// // Initialize a new hash set.
/// let books = HashSet::new();
/// let guard = flurry::epoch::pin();
///
/// // Add some books
/// books.insert("Fight Club", &guard);
/// books.insert("Three Men In A Raft", &guard);
/// books.insert("The Book of Dust", &guard);
/// books.insert("The Dry", &guard);
///
/// // Check for a specific one.
/// if !books.contains(&"The Drunken Botanist", &guard) {
///     println!("We don't have The Drunken Botanist.");
/// }
///
/// // Remove a book.
/// books.remove(&"Three Men In A Raft", &guard);
///
/// // Iterate over everything.
/// for book in books.iter(&guard) {
///     println!("{}", book);
/// }
/// ```
#[derive(Debug)]
pub struct HashSet<T, S = crate::DefaultHashBuilder> {
    map: HashMap<T, (), S>,
}

impl<T> HashSet<T, crate::DefaultHashBuilder> {
    /// Creates an empty `HashSet`.
    ///
    /// The hash set is initially created with a capacity of 0, so it will not allocate until it
    /// is first inserted into.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    /// let set: HashSet<i32> = HashSet::new();
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty `HashSet` with the specified capacity.
    ///
    /// The hash map will be able to hold at least `capacity` elements without
    /// reallocating. If `capacity` is 0, the hash map will not allocate.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    /// let map: HashSet<&str, _> = HashSet::with_capacity(10);
    /// ```
    ///
    /// # Notes
    ///
    /// There is no guarantee that the HashSet will not resize if `capacity`
    /// elements are inserted. The set will resize based on key collision, so
    /// bad key distribution may cause a resize before `capacity` is reached.
    /// For more information see the [`resizing behavior`] of HashMap.
    ///
    /// [`resizing behavior`]: index.html#resizing-behavior
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, crate::DefaultHashBuilder::default())
    }
}

impl<T, S> Default for HashSet<T, S>
where
    S: Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<T, S> HashSet<T, S> {
    /// Creates an empty set which will use `hash_builder` to hash values.
    ///
    /// The created set has the default initial capacity.
    ///
    /// Warning: `hash_builder` is normally randomly generated, and is designed to
    /// allow the set to be resistant to attacks that cause many collisions and
    /// very poor performance. Setting it manually using this
    /// function can expose a DoS attack vector.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::{HashSet, DefaultHashBuilder};
    ///
    /// let set = HashSet::with_hasher(DefaultHashBuilder::default());
    /// let guard = flurry::epoch::pin();
    /// set.insert(1, &guard);
    /// ```
    pub fn with_hasher(hash_builder: S) -> Self {
        Self {
            map: HashMap::with_hasher(hash_builder),
        }
    }

    /// Creates an empty set with the specified `capacity`, using `hash_builder` to hash the
    /// values.
    ///
    /// The set will be sized to accommodate `capacity` elements with a low chance of reallocating
    /// (assuming uniformly distributed hashes). If `capacity` is 0, the call will not allocate,
    /// and is equivalent to [`HashSet::new`].
    ///
    /// Warning: `hash_builder` is normally randomly generated, and is designed to allow the set
    /// to be resistant to attacks that cause many collisions and very poor performance.
    /// Setting it manually using this function can expose a DoS attack vector.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let s = RandomState::new();
    /// let set = HashSet::with_capacity_and_hasher(10, s);
    /// let guard = flurry::epoch::pin();
    /// set.insert(1, &guard);
    /// ```
    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self {
        Self {
            map: HashMap::with_capacity_and_hasher(capacity, hash_builder),
        }
    }

    /// An iterator visiting all values in arbitrary order.
    ///
    /// The iterator element type is `(&'g K, &'g V)`.
    ///
    /// See [`HashMap::keys`] for details.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    ///
    /// let set = HashSet::new();
    /// let guard = flurry::epoch::pin();
    /// set.insert(1, &guard);
    /// set.insert(2, &guard);
    ///
    /// for x in set.iter(&guard) {
    ///     println!("{}", x);
    /// }
    /// ```
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> Keys<'g, T, ()> {
        self.map.keys(guard)
    }
}

impl<T, S> HashSet<T, S>
where
    T: Hash + Eq,
    S: BuildHasher,
{
    /// Returns `true` if the set contains the specified value.
    ///
    /// The value may be any borrowed form of the set's value type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the value type.
    ///
    /// [`Eq`]: std::cmp::Eq
    /// [`Hash`]: std::hash::Hash
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    ///
    /// let set = HashSet::new();
    /// let guard = flurry::epoch::pin();
    /// set.insert(2, &guard);
    ///
    /// assert!(set.contains(&2, &guard));
    /// assert!(!set.contains(&1, &guard));
    /// ```
    pub fn contains<'g, Q>(&self, value: &Q, guard: &'g Guard) -> bool
    where
        T: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        self.map.contains_key(value, guard)
    }
}

impl<T, S> HashSet<T, S>
where
    T: 'static + Sync + Send + Clone + Hash + Eq,
    S: BuildHasher,
{
    /// Adds a value to the set.
    ///
    /// If the set did not have this value present, `true` is returned.
    ///
    /// If the set did have this value present, `false` is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    ///
    /// let set = HashSet::new();
    /// let guard = flurry::epoch::pin();
    ///
    /// assert_eq!(set.insert(2, &guard), true);
    /// assert_eq!(set.insert(2, &guard), false);
    /// assert!(set.contains(&2, &guard));
    /// ```
    pub fn insert<'g>(&'g self, value: T, guard: &'g Guard) -> bool {
        let old = self.map.insert(value, (), guard);
        old.is_none()
    }

    /// Removes a value from the set.
    ///
    /// If the set did not have this value present, `false` is returned.
    ///
    /// If the set did have this value present, `true` is returned.
    ///
    /// The value may be any borrowed form of the set's value type, but
    /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
    /// the value type.
    ///
    /// [`Eq`]: std::cmp::Eq
    /// [`Hash`]: std::hash::Hash
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashSet;
    ///
    /// let set = HashSet::new();
    /// let guard = flurry::epoch::pin();
    /// set.insert(2, &guard);
    ///
    /// assert_eq!(set.remove(&2, &guard), true);
    /// assert!(!set.contains(&2, &guard));
    /// assert_eq!(set.remove(&2, &guard), false);
    /// ```
    pub fn remove<'g, Q>(&'g self, value: &Q, guard: &'g Guard) -> bool
    where
        T: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let removed = self.map.remove(value, guard);
        removed.is_some()
    }
}
