use fast_counter::ConcurrentCounter;
use seize::Linked;

use crate::iter::*;
use crate::node::*;
use crate::raw::*;
use crate::reclaim::{Atomic, Collector, Guard, RetireShared, Shared};
use std::borrow::Borrow;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicIsize, Ordering};

const ISIZE_BITS: usize = core::mem::size_of::<isize>() * 8;

/// The largest possible table capacity.  This value must be
/// exactly 1<<30 to stay within Java array allocation and indexing
/// bounds for power of two table sizes, and is further required
/// because the top two bits of 32bit hash fields are used for
/// control purposes.
const MAXIMUM_CAPACITY: usize = 1 << 30; // TODO: use ISIZE_BITS

/// The default initial table capacity.  Must be a power of 2
/// (i.e., at least 1) and at most `MAXIMUM_CAPACITY`.
const DEFAULT_CAPACITY: usize = 16;

/// The bin count threshold for using a tree rather than list for a bin. Bins are
/// converted to trees when adding an element to a bin with at least this many
/// nodes. The value must be greater than 2, and should be at least 8 to mesh
/// with assumptions in tree removal about conversion back to plain bins upon
/// shrinkage.
const TREEIFY_THRESHOLD: usize = 8;

/// The bin count threshold for untreeifying a (split) bin during a resize
/// operation. Should be less than TREEIFY_THRESHOLD, and at most 6 to mesh with
/// shrinkage detection under removal.
const UNTREEIFY_THRESHOLD: usize = 6;

/// The smallest table capacity for which bins may be treeified. (Otherwise the
/// table is resized if too many nodes in a bin.) The value should be at least 4
/// * TREEIFY_THRESHOLD to avoid conflicts between resizing and treeification
/// thresholds.
const MIN_TREEIFY_CAPACITY: usize = 64;

/// Minimum number of rebinnings per transfer step. Ranges are
/// subdivided to allow multiple resizer threads.  This value
/// serves as a lower bound to avoid resizers encountering
/// excessive memory contention.  The value should be at least
/// `DEFAULT_CAPACITY`.
const MIN_TRANSFER_STRIDE: isize = 16;

/// The number of bits used for generation stamp in `size_ctl`.
/// Must be at least 6 for 32bit arrays.
const RESIZE_STAMP_BITS: usize = ISIZE_BITS / 2;

/// The maximum number of threads that can help resize.
/// Must fit in `32 - RESIZE_STAMP_BITS` bits for 32 bit architectures
/// and `64 - RESIZE_STAMP_BITS` bits for 64 bit architectures
const MAX_RESIZERS: isize = (1 << (ISIZE_BITS - RESIZE_STAMP_BITS)) - 1;

/// The bit shift for recording size stamp in `size_ctl`.
const RESIZE_STAMP_SHIFT: usize = ISIZE_BITS - RESIZE_STAMP_BITS;

#[cfg(not(miri))]
static NCPU_INITIALIZER: std::sync::Once = std::sync::Once::new();
#[cfg(not(miri))]
static NCPU: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

macro_rules! load_factor {
    ($n: expr) => {
        // ¾ n = n - n/4 = n - (n >> 2)
        $n - ($n >> 2)
    };
}

/// A concurrent hash table.
///
/// Flurry uses [`Guards`] to control the lifetime of the resources that get stored and
/// extracted from the map. [`Guards`] are acquired through the [`HashMap::pin`] and
/// [`HashMap::guard`] functions. For more information, see the [notes in the crate-level
/// documentation].
///
/// [notes in the crate-level documentation]: index.html#a-note-on-guard-and-memory-use
/// [`Guards`]: index.html#a-note-on-guard-and-memory-use
pub struct HashMap<K, V, S = crate::DefaultHashBuilder> {
    /// The array of bins. Lazily initialized upon first insertion.
    /// Size is always a power of two. Accessed directly by iterators.
    table: Atomic<Table<K, V>>,

    /// The next table to use; non-null only while resizing.
    next_table: Atomic<Table<K, V>>,

    /// The next table index (plus one) to split while resizing.
    transfer_index: AtomicIsize,

    counter: ConcurrentCounter,

    /// Table initialization and resizing control.  When negative, the
    /// table is being initialized or resized: -1 for initialization,
    /// else -(1 + the number of active resizing threads).  Otherwise,
    /// when table is null, holds the initial table size to use upon
    /// creation, or 0 for default. After initialization, holds the
    /// next element count value upon which to resize the table.
    size_ctl: AtomicIsize,

    /// Collector that all `Guard` references used for operations on this map must be tied to. It
    /// is important that they all assocate with the _same_ `Collector`, otherwise you end up with
    /// unsoundness as described in https://github.com/jonhoo/flurry/issues/46. Specifically, a
    /// user can do:
    ///
    /// ```rust,should_panic
    /// # use flurry::HashMap;
    /// let map: HashMap<_, _> = HashMap::default();
    /// map.insert(42, String::from("hello"), &map.guard());
    ///
    /// let evil = seize::Collector::new();
    /// let guard = evil.enter();
    /// let oops = map.get(&42, &guard);
    ///
    /// map.remove(&42, &map.guard());
    /// // at this point, the default collector is allowed to free `"hello"`
    /// // since no guard from `map`s collector is active.
    /// // `oops` is tied to the lifetime of a Guard that is not a part of
    /// // the same collector, and so can now be dangling.
    /// // but we can still access it!
    /// assert_eq!(oops.unwrap(), "hello");
    /// ```
    ///
    /// We avoid that by checking that every external guard that is passed in is associated with
    /// the `Collector` that was specified when the map was created (which may be the global
    /// collector).
    collector: Collector,

    build_hasher: S,
}

#[derive(Eq, PartialEq, Debug)]
enum PutResult<'a, T> {
    Inserted {
        new: &'a T,
    },
    Replaced {
        old: &'a T,
        new: &'a T,
    },
    Exists {
        current: &'a T,
        not_inserted: Box<Linked<T>>,
    },
}

impl<'a, T> PutResult<'a, T> {
    fn before(&self) -> Option<&'a T> {
        match *self {
            PutResult::Inserted { .. } => None,
            PutResult::Replaced { old, .. } => Some(old),
            PutResult::Exists { current, .. } => Some(current),
        }
    }

    #[allow(dead_code)]
    fn after(&self) -> Option<&'a T> {
        match *self {
            PutResult::Inserted { new } => Some(new),
            PutResult::Replaced { new, .. } => Some(new),
            PutResult::Exists { .. } => None,
        }
    }
}

/// The error type for the [`HashMap::try_insert`] method.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct TryInsertError<'a, V> {
    /// A reference to the current value mapped to the key.
    pub current: &'a V,
    /// The value that [`HashMap::try_insert`] failed to insert.
    pub not_inserted: V,
}

impl<'a, V> Display for TryInsertError<'a, V>
where
    V: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Insert of \"{:?}\" failed as key was already present with value \"{:?}\"",
            self.not_inserted, self.current
        )
    }
}

impl<'a, V> Error for TryInsertError<'a, V>
where
    V: Debug,
{
    #[inline]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

// ===
// the following methods only see Ks and Vs if there have been inserts.
// modifications to the map are all guarded by thread-safety bounds (Send + Sync ).
// but _these_ methods do not need to be, since they will never introduce keys or values, only give
// out ones that have already been inserted (which implies they must be thread-safe).
// ===

impl<K, V> HashMap<K, V, crate::DefaultHashBuilder> {
    /// Creates an empty `HashMap`.
    ///
    /// The hash map is initially created with a capacity of 0, so it will not allocate until it
    /// is first inserted into.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    /// let map: HashMap<&str, i32> = HashMap::new();
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty `HashMap` with the specified capacity.
    ///
    /// The hash map will be able to hold at least `capacity` elements without
    /// reallocating. If `capacity` is 0, the hash map will not allocate.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    /// let map: HashMap<&str, i32> = HashMap::with_capacity(10);
    /// ```
    ///
    /// # Notes
    ///
    /// There is no guarantee that the HashMap will not resize if `capacity`
    /// elements are inserted. The map will resize based on key collision, so
    /// bad key distribution may cause a resize before `capacity` is reached.
    /// For more information see the [`resizing behavior`]
    ///
    /// [`resizing behavior`]: index.html#resizing-behavior
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, crate::DefaultHashBuilder::default())
    }
}

impl<K, V, S> Default for HashMap<K, V, S>
where
    S: Default,
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K, V, S> HashMap<K, V, S> {
    /// Creates an empty map which will use `hash_builder` to hash keys.
    ///
    /// The created map has the default initial capacity.
    ///
    /// Warning: `hash_builder` is normally randomly generated, and is designed to
    /// allow the map to be resistant to attacks that cause many collisions and
    /// very poor performance. Setting it manually using this
    /// function can expose a DoS attack vector.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::{HashMap, DefaultHashBuilder};
    ///
    /// let map = HashMap::with_hasher(DefaultHashBuilder::default());
    /// map.pin().insert(1, 2);
    /// ```
    pub fn with_hasher(hash_builder: S) -> Self {
        Self {
            table: Atomic::null(),
            next_table: Atomic::null(),
            transfer_index: AtomicIsize::new(0),
            counter: ConcurrentCounter::new(num_cpus()),
            size_ctl: AtomicIsize::new(0),
            build_hasher: hash_builder,
            collector: Collector::new(),
        }
    }

    /// Creates an empty map with the specified `capacity`, using `hash_builder` to hash the keys.
    ///
    /// The map will be sized to accommodate `capacity` elements with a low chance of reallocating
    /// (assuming uniformly distributed hashes). If `capacity` is 0, the call will not allocate,
    /// and is equivalent to [`HashMap::new`].
    ///
    /// Warning: `hash_builder` is normally randomly generated, and is designed to allow the map
    /// to be resistant to attacks that cause many collisions and very poor performance.
    /// Setting it manually using this function can expose a DoS attack vector.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let s = RandomState::new();
    /// let map = HashMap::with_capacity_and_hasher(10, s);
    /// map.pin().insert(1, 2);
    /// ```
    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self {
        if capacity == 0 {
            return Self::with_hasher(hash_builder);
        }

        let mut map = Self::with_hasher(hash_builder);
        map.presize(capacity);
        map
    }

    /// Associate a custom [`seize::Collector`] with this map.
    ///
    /// By default, the global collector is used. With this method you can use a different
    /// collector instead. This may be desireable if you want more control over when and how memory
    /// reclamation happens.
    ///
    /// Note that _all_ `Guard` references provided to access the returned map _must_ be
    /// constructed using guards produced by `collector`.
    #[must_use]
    pub fn with_collector(mut self, collector: Collector) -> Self {
        self.collector = collector;
        self
    }

    /// Pin a `Guard` for use with this map.
    ///
    /// Keep in mind that for as long as you hold onto this `Guard`, you are preventing the
    /// collection of garbage generated by the map.
    pub fn guard(&self) -> Guard<'_> {
        self.collector.enter()
    }

    #[inline]
    fn check_guard(&self, guard: &Guard<'_>) {
        // guard.collector() may be `None` if it is unprotected
        if let Some(c) = guard.collector() {
            assert!(Collector::ptr_eq(c, &self.collector));
        }
    }

    /// Returns the number of entries in the map.
    ///
    /// Note that the returned value is _NOT_ an
    /// atomic snapshot; invocation in the absence of concurrent
    /// updates returns an accurate result, but concurrent updates that
    /// occur while the sum is being calculated might not be
    /// incorporated.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    ///
    /// map.pin().insert(1, "a");
    /// map.pin().insert(2, "b");
    /// assert!(map.pin().len() == 2);
    /// ```
    pub fn len(&self) -> usize {
        let n = self.counter.sum();
        if n < 0 {
            0
        } else {
            n as usize
        }
    }

    /// Returns `true` if the map is empty. Otherwise returns `false`.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    /// assert!(map.pin().is_empty());
    /// map.pin().insert("a", 1);
    /// assert!(!map.pin().is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    /// Returns the capacity of the map.
    fn capacity(&self, guard: &Guard<'_>) -> usize {
        self.check_guard(guard);
        let table = self.table.load(Ordering::Relaxed, guard);

        if table.is_null() {
            0
        } else {
            // Safety: we loaded `table` under the `guard`,
            // so it must still be valid here
            unsafe { table.deref() }.len()
        }
    }

    /// Returns the stamp bits for resizing a table of size n.
    /// Must be negative when shifted left by `RESIZE_STAMP_SHIFT`.
    fn resize_stamp(n: usize) -> isize {
        n.leading_zeros() as isize | (1_isize << (RESIZE_STAMP_BITS - 1))
    }

    /// An iterator visiting all key-value pairs in arbitrary order.
    ///
    /// The iterator element type is `(&'g K, &'g V)`.
    pub fn iter<'g>(&'g self, guard: &'g Guard<'_>) -> Iter<'g, K, V> {
        self.check_guard(guard);
        let table = self.table.load(Ordering::SeqCst, guard);
        let node_iter = NodeIter::new(table, guard);
        Iter { node_iter, guard }
    }

    /// An iterator visiting all keys in arbitrary order.
    ///
    /// The iterator element type is `&'g K`.
    pub fn keys<'g>(&'g self, guard: &'g Guard<'_>) -> Keys<'g, K, V> {
        self.check_guard(guard);
        let table = self.table.load(Ordering::SeqCst, guard);
        let node_iter = NodeIter::new(table, guard);
        Keys { node_iter }
    }

    /// An iterator visiting all values in arbitrary order.
    ///
    /// The iterator element type is `&'g V`.
    pub fn values<'g>(&'g self, guard: &'g Guard<'_>) -> Values<'g, K, V> {
        self.check_guard(guard);
        let table = self.table.load(Ordering::SeqCst, guard);
        let node_iter = NodeIter::new(table, guard);
        Values { node_iter, guard }
    }

    fn init_table<'g>(&'g self, guard: &'g Guard<'_>) -> Shared<'g, Table<K, V>> {
        loop {
            let table = self.table.load(Ordering::SeqCst, guard);
            // safety: we loaded the table while the thread was marked as active.
            // table won't be deallocated until the guard is dropped at the earliest.
            if !table.is_null() && !unsafe { table.deref() }.is_empty() {
                break table;
            }
            // try to allocate the table
            let mut sc = self.size_ctl.load(Ordering::SeqCst);
            if sc < 0 {
                // we lost the initialization race; just spin
                std::thread::yield_now();
                continue;
            }

            if self
                .size_ctl
                .compare_exchange(sc, -1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                // we get to do it!
                let mut table = self.table.load(Ordering::SeqCst, guard);

                // safety: we loaded the table while the thread was marked as active.
                // table won't be deallocated until the guard is dropped at the earliest.
                if table.is_null() || unsafe { table.deref() }.is_empty() {
                    let n = if sc > 0 {
                        sc as usize
                    } else {
                        DEFAULT_CAPACITY
                    };
                    table = Shared::boxed(Table::new(n, &self.collector), &self.collector);
                    self.table.store(table, Ordering::SeqCst);
                    sc = load_factor!(n as isize)
                }
                self.size_ctl.store(sc, Ordering::SeqCst);
                break table;
            }
        }
    }

    /// Presize the table to accommodate the given number of elements.
    fn presize(&mut self, size: usize) {
        // NOTE: this is a stripped-down version of try_presize for use only when we _know_ that
        // the table is new, and that therefore we won't have to help out with transfers or deal
        // with contending initializations.

        // safety: we are creating this map, so no other thread can access it,
        // while we are initializing it.
        let guard = unsafe { Guard::unprotected() };

        let requested_capacity = if size >= MAXIMUM_CAPACITY / 2 {
            MAXIMUM_CAPACITY
        } else {
            // round the requested_capacity to the next power of to from 1.5 * size + 1
            // TODO: find out if this is neccessary
            let size = size + (size >> 1) + 1;

            std::cmp::min(MAXIMUM_CAPACITY, size.next_power_of_two())
        } as usize;

        // sanity check that the map has indeed not been set up already
        assert_eq!(self.size_ctl.load(Ordering::SeqCst), 0);
        assert!(self.table.load(Ordering::SeqCst, &guard).is_null());

        // the table has not yet been initialized, so we can just create it
        // with as many bins as were requested

        // create a table with `new_capacity` empty bins
        let new_table = Shared::boxed(
            Table::new(requested_capacity, &self.collector),
            &self.collector,
        );

        // store the new table to `self.table`
        self.table.store(new_table, Ordering::SeqCst);

        // resize the table once it is 75% full
        let new_load_to_resize_at = load_factor!(requested_capacity as isize);

        // store the next load at which the table should resize to it's size_ctl field
        // and thus release the initialization "lock"
        self.size_ctl.store(new_load_to_resize_at, Ordering::SeqCst);
    }
}

// ===
// the following methods require Clone and Ord, since they ultimately call
// `transfer`, which needs to be able to clone keys and work with tree bins.
// however, they do _not_ need to require thread-safety bounds (Send + Sync +
// 'static) since if the bounds do not hold, the map is empty, so no keys or
// values will be transfered anyway.
// ===

impl<K, V, S> HashMap<K, V, S>
where
    K: Clone + Ord,
{
    /// Tries to presize table to accommodate the given number of elements.
    fn try_presize(&self, size: usize, guard: &Guard<'_>) {
        let requested_capacity = if size >= MAXIMUM_CAPACITY / 2 {
            MAXIMUM_CAPACITY
        } else {
            // round the requested_capacity to the next power of to from 1.5 * size + 1
            // TODO: find out if this is neccessary
            let size = size + (size >> 1) + 1;

            std::cmp::min(MAXIMUM_CAPACITY, size.next_power_of_two())
        } as isize;

        loop {
            let size_ctl = self.size_ctl.load(Ordering::SeqCst);
            if size_ctl < 0 {
                break;
            }

            let table = self.table.load(Ordering::SeqCst, guard);

            // The current capacity == the number of bins in the current table
            let current_capactity = if table.is_null() {
                0
            } else {
                unsafe { table.deref() }.len()
            };

            if current_capactity == 0 {
                // the table has not yet been initialized, so we can just create it
                // with as many bins as were requested

                // since the map is uninitialized, size_ctl describes the initial capacity
                let initial_capacity = size_ctl;

                // the new capacity is either the requested capacity or the initial capacity (size_ctl)
                let new_capacity = requested_capacity.max(initial_capacity) as usize;

                // try to aquire the initialization "lock" to indicate that we are initializing the table.
                if self
                    .size_ctl
                    .compare_exchange(size_ctl, -1, Ordering::SeqCst, Ordering::Relaxed)
                    .is_err()
                {
                    // somebody else is already initializing the table (or has already finished).
                    continue;
                }

                // we got the initialization `lock`; Make sure the table is still unitialized
                // (or is the same table with 0 bins we read earlier, althought that should not be the case)
                if self.table.load(Ordering::SeqCst, guard) != table {
                    // NOTE: this could probably be `!self.table.load(...).is_null()`
                    // if we decide that tables can never have 0 bins.

                    // the table is already initialized; Write the `size_ctl` value it had back to it's
                    // `size_ctl` field to release the initialization "lock"
                    self.size_ctl.store(size_ctl, Ordering::SeqCst);
                    continue;
                }

                // create a table with `new_capacity` empty bins
                let new_table =
                    Shared::boxed(Table::new(new_capacity, &self.collector), &self.collector);

                // store the new table to `self.table`
                let old_table = self.table.swap(new_table, Ordering::SeqCst, guard);

                // old_table should be `null`, since we don't ever initialize a table with 0 bins
                // and this branch only happens if table has not yet been initialized or it's length is 0.
                assert!(old_table.is_null());

                // TODO: if we allow tables with 0 bins. `defer_destroy` `old_table` if it's not `null`:
                // if !old_table.is_null() {
                //     // TODO: safety argument, for why this is okay
                //     unsafe { guard.defer_destroy(old_table) }
                // }

                // resize the table once it is 75% full
                let new_load_to_resize_at = load_factor!(new_capacity as isize);

                // store the next load at which the table should resize to it's size_ctl field
                // and thus release the initialization "lock"
                self.size_ctl.store(new_load_to_resize_at, Ordering::SeqCst);
            } else if requested_capacity <= size_ctl || current_capactity >= MAXIMUM_CAPACITY {
                // Either the `requested_capacity` was smaller than or equal to the load we would resize at (size_ctl)
                // and we don't need to resize, since our load factor will still be acceptable if we don't

                // Or it was larger than the `MAXIMUM_CAPACITY` of the map and we refuse
                // to resize to an invalid capacity
                break;
            } else if table == self.table.load(Ordering::SeqCst, guard) {
                // The table is initialized, try to resize it to the requested capacity

                let rs: isize = Self::resize_stamp(current_capactity) << RESIZE_STAMP_SHIFT;
                // TODO: see #29: `rs` is postive even though `resize_stamp` says:
                // "Must be negative when shifted left by RESIZE_STAMP_SHIFT"
                // and since our size_control field needs to be negative
                // to indicate a resize this needs to be addressed

                if self
                    .size_ctl
                    .compare_exchange(size_ctl, rs + 2, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    // someone else already started to resize the table
                    // TODO: can we `self.help_transfer`?
                    self.transfer(table, Shared::null(), guard);
                }
            }
        }
    }

    // NOTE: transfer requires that K and V are Send + Sync if it will actually transfer anything.
    // If K/V aren't Send + Sync, the map must be empty, and therefore calling tansfer is fine.
    #[inline(never)]
    fn transfer<'g>(
        &'g self,
        table: Shared<'g, Table<K, V>>,
        mut next_table_ptr: Shared<'g, Table<K, V>>,
        guard: &'g Guard<'_>,
    ) {
        // safety: table was read while `guard` was held. the code that drops table only drops it
        // after it is no longer reachable, and any outstanding references are no longer active.
        // this references is still active (marked by the guard), so the target of the references
        // won't be dropped while the guard remains active.
        let n = unsafe { table.deref() }.len();
        let ncpu = num_cpus();

        let stride = if ncpu > 1 { (n >> 3) / ncpu } else { n };
        let stride = std::cmp::max(stride as isize, MIN_TRANSFER_STRIDE);

        if next_table_ptr.is_null() {
            // we are initiating a resize
            let table = Shared::boxed(Table::new(n << 1, &self.collector), &self.collector);
            let now_garbage = self.next_table.swap(table, Ordering::SeqCst, guard);
            assert!(now_garbage.is_null());
            self.transfer_index.store(n as isize, Ordering::SeqCst);
            next_table_ptr = self.next_table.load(Ordering::Relaxed, guard);
        }

        // safety: same argument as for table above
        let next_n = unsafe { next_table_ptr.deref() }.len();

        let mut advance = true;
        let mut finishing = false;
        let mut i = 0;
        let mut bound = 0;
        loop {
            // try to claim a range of bins for us to transfer
            while advance {
                i -= 1;
                if i >= bound || finishing {
                    advance = false;
                    break;
                }

                let next_index = self.transfer_index.load(Ordering::SeqCst);
                if next_index <= 0 {
                    i = -1;
                    advance = false;
                    break;
                }

                let next_bound = if next_index > stride {
                    next_index - stride
                } else {
                    0
                };
                if self
                    .transfer_index
                    .compare_exchange(next_index, next_bound, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    bound = next_bound;
                    i = next_index;
                    advance = false;
                    break;
                }
            }

            if i < 0 || i as usize >= n || i as usize + n >= next_n {
                // the resize has finished

                if finishing {
                    // this branch is only taken for one thread partaking in the resize!
                    self.next_table.store(Shared::null(), Ordering::SeqCst);
                    let now_garbage = self.table.swap(next_table_ptr, Ordering::SeqCst, guard);
                    // safety: need to guarantee that now_garbage is no longer reachable. more
                    // specifically, no thread that executes _after_ this line can ever get a
                    // reference to now_garbage.
                    //
                    // first, we need to argue that there is no _other_ way to get to now_garbage.
                    //
                    //  - it is _not_ accessible through self.table any more
                    //  - it is _not_ accessible through self.next_table any more
                    //  - what about forwarding nodes (BinEntry::Moved)?
                    //    the only BinEntry::Moved that point to now_garbage, are the ones in
                    //    _previous_ tables. to get to those previous tables, one must ultimately
                    //    have arrived through self.table (because that's where all operations
                    //    start their search). since self.table has now changed, only "old" threads
                    //    can still be accessing them. no new thread can get to past tables, and
                    //    therefore they also cannot get to ::Moved that point to now_garbage, so
                    //    we're fine.
                    //
                    // this means that no _future_ thread (i.e., that was inactive before
                    // the swap may be freed) can get a reference to now_garbage.
                    //
                    // next, let's talk about threads with _existing_ references to now_garbage.
                    // such a thread must have gotten that reference before the call to swap.
                    // because of this, that thread must have been marked as active, and included
                    // in the reference count, meaning the garbage will not be freed until
                    // that thread drops its guard at the earliest.
                    unsafe { guard.retire_shared(now_garbage) };
                    self.size_ctl
                        .store(((n as isize) << 1) - ((n as isize) >> 1), Ordering::SeqCst);
                    return;
                }

                let sc = self.size_ctl.load(Ordering::SeqCst);
                if self
                    .size_ctl
                    .compare_exchange(sc, sc - 1, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    if (sc - 2) != Self::resize_stamp(n) << RESIZE_STAMP_SHIFT {
                        return;
                    }

                    // we are the chosen thread to finish the resize!
                    finishing = true;

                    // ???
                    advance = true;

                    // NOTE: the java code says "recheck before commit" here
                    i = n as isize;
                }

                continue;
            }
            let i = i as usize;

            // safety: these were read while `guard` was held. the code that drops these, only
            // drops them after a) they are no longer reachable, and b) any outstanding references
            // are no longer active. these references are still active (marked by the guard), so
            // the target of these references won't be dropped while the guard remains active.
            let table = unsafe { table.deref() };

            let bin = table.bin(i, guard);
            if bin.is_null() {
                advance = table
                    .cas_bin(
                        i,
                        Shared::null(),
                        table.get_moved(next_table_ptr, guard),
                        guard,
                    )
                    .is_ok();
                continue;
            }
            // safety: as for table above
            let next_table = unsafe { next_table_ptr.deref() };

            // safety: bin is a valid pointer.
            //
            // there are three cases when a bin pointer is invalidated:
            //
            //  1. if the table was resized, bin is a move entry, and the resize has completed. in
            //     that case, the table (and all its heads) have already been retired.
            //  2. if the table is being resized, bin may be swapped with a move entry. the old bin
            //     will only be retired after that happens.
            //  3. when elements are inserted into or removed from the map, bin may be changed into
            //     or from a TreeBin from or into a regular, linear bin. the old bin will be
            //     retired only once that happens.
            //
            // in all cases, we held the guard when we got the reference to the bin. if any such
            // swap happened, it must have happened _after_ we read. since we did the read while
            // the current thread was marked as active, we must be included in the reference count,
            // and the drop must happen _after_ we decrement the count (i.e drop our guard).
            match **unsafe { bin.deref() } {
                BinEntry::Moved => {
                    // already processed
                    advance = true;
                }
                BinEntry::Node(ref head) => {
                    // bin is non-empty, need to link into it, so we must take the lock
                    let head_lock = head.lock.lock();

                    // need to check that this is _still_ the head
                    let current_head = table.bin(i, guard);
                    if current_head != bin {
                        // nope -- try again from the start
                        continue;
                    }

                    // yes, it is still the head, so we can now "own" the bin
                    // note that there can still be readers in the bin!

                    // TODO: ReservationNode

                    let mut run_bit = head.hash & n as u64;
                    let mut last_run = bin;
                    let mut p = bin;
                    loop {
                        // safety: p is a valid pointer.
                        //
                        // p is only retired when its bin is replaced with a move node
                        // (see safety comment near table.store_bin below). we read the
                        // bin, and got to p, so its bin has not yet been swapped with a
                        // move node. and, our thread is marked as active, so any retirement
                        // must include us in the reference count. therefore, it can only be
                        // dropped _after_ we drop our guard, and is safe to use now.
                        let node = unsafe { p.deref() }.as_node().unwrap();
                        let next = node.next.load(Ordering::SeqCst, guard);

                        let b = node.hash & n as u64;
                        if b != run_bit {
                            run_bit = b;
                            last_run = p;
                        }

                        if next.is_null() {
                            break;
                        }
                        p = next;
                    }

                    let mut low_bin = Shared::null();
                    let mut high_bin = Shared::null();
                    if run_bit == 0 {
                        // last run is all in the low bin
                        low_bin = last_run;
                    } else {
                        // last run is all in the high bin
                        high_bin = last_run;
                    }

                    p = bin;
                    while p != last_run {
                        // safety: p is a valid pointer.
                        //
                        // p is only retired when its bin is replaced with a move node
                        // (see safety comment near table.store_bin below). we read the
                        // bin, and got to p, so its bin has not yet been swapped with a
                        // move node. and, our thread is marked as active, so any retirement
                        // must include us in the reference count. therefore, it can only be
                        // dropped _after_ we drop our guard, and is safe to use now.
                        let node = unsafe { p.deref() }.as_node().unwrap();

                        let link = if node.hash & n as u64 == 0 {
                            // to the low bin!
                            &mut low_bin
                        } else {
                            // to the high bin!
                            &mut high_bin
                        };

                        *link = Shared::boxed(
                            BinEntry::Node(Node::with_next(
                                node.hash,
                                node.key.clone(),
                                node.value.clone(),
                                Atomic::from(*link),
                            )),
                            &self.collector,
                        );

                        p = node.next.load(Ordering::SeqCst, guard);
                    }

                    next_table.store_bin(i, low_bin);
                    next_table.store_bin(i + n, high_bin);
                    table.store_bin(i, table.get_moved(next_table_ptr, guard));

                    // everything up to last_run in the _old_ bin linked list is now garbage.
                    // those nodes have all been re-allocated in the new bin linked list.
                    p = bin;
                    while p != last_run {
                        // safety:
                        //
                        // we need to argue that there is no longer a way to access p. the only way
                        // to get to p is through table[i]. since table[i] has been replaced by a
                        // BinEntry::Moved, p is no longer accessible.
                        //
                        // any existing reference to p must have been taken before table.store_bin.
                        // any threads that have such a reference must have been active before we
                        // retired p, so they are protected by the reference count.
                        let next = unsafe { p.deref() }
                            .as_node()
                            .unwrap()
                            .next
                            .load(Ordering::SeqCst, guard);
                        unsafe { guard.retire_shared(p) };
                        p = next;
                    }

                    advance = true;

                    drop(head_lock);
                }
                BinEntry::Tree(ref tree_bin) => {
                    let bin_lock = tree_bin.lock.lock();

                    // need to check that this is _still_ the correct bin
                    let current_head = table.bin(i, guard);
                    if current_head != bin {
                        // nope -- try again from the start
                        continue;
                    }

                    let mut low = Shared::null();
                    let mut low_tail = Shared::null();
                    let mut high = Shared::null();
                    let mut high_tail = Shared::null();
                    let mut low_count = 0;
                    let mut high_count = 0;
                    let mut e = tree_bin.first.load(Ordering::Relaxed, guard);
                    while !e.is_null() {
                        // safety: we read under our guard, at which point the tree
                        // structure was valid. Since our guard marks the current thread
                        // as active, the TreeNodes remain valid for at least as long as
                        // we hold onto the guard.
                        // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                        let tree_node = unsafe { TreeNode::get_tree_node(e) };
                        let hash = tree_node.node.hash;
                        let new_node = TreeNode::new(
                            hash,
                            tree_node.node.key.clone(),
                            tree_node.node.value.clone(),
                            Atomic::null(),
                            Atomic::null(),
                        );
                        let run_bit = hash & n as u64;
                        if run_bit == 0 {
                            new_node.prev.store(low_tail, Ordering::Relaxed);
                            let new_node =
                                Shared::boxed(BinEntry::TreeNode(new_node), &self.collector);
                            if low_tail.is_null() {
                                // this is the first element inserted into the low bin
                                low = new_node;
                            } else {
                                // safety: `low_tail` was just created by us and not shared.
                                // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                                unsafe { TreeNode::get_tree_node(low_tail) }
                                    .node
                                    .next
                                    .store(new_node, Ordering::Relaxed);
                            }
                            low_tail = new_node;
                            low_count += 1;
                        } else {
                            new_node.prev.store(high_tail, Ordering::Relaxed);
                            let new_node =
                                Shared::boxed(BinEntry::TreeNode(new_node), &self.collector);
                            if high_tail.is_null() {
                                // this is the first element inserted into the high bin
                                high = new_node;
                            } else {
                                // safety: `high_tail` was just created by us and not shared.
                                // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                                unsafe { TreeNode::get_tree_node(high_tail) }
                                    .node
                                    .next
                                    .store(new_node, Ordering::Relaxed);
                            }
                            high_tail = new_node;
                            high_count += 1;
                        }
                        e = tree_node.node.next.load(Ordering::Relaxed, guard);
                    }

                    let mut reused_bin = false;
                    let low_bin = if low_count <= UNTREEIFY_THRESHOLD {
                        // use a regular bin instead of a tree bin since the
                        // bin is too small. since the tree nodes are
                        // already behind shared references, we have to
                        // clean them up manually.
                        let low_linear = self.untreeify(low, guard);
                        // safety: we have just created `low` and its `next`
                        // nodes and have never shared them
                        unsafe { TreeBin::drop_tree_nodes(low, false, guard) };
                        low_linear
                    } else if high_count != 0 {
                        // the new bin will also be a tree bin. if both the high
                        // bin and the low bin are non-empty, we have to
                        // allocate a new TreeBin.
                        // safety: we have just created `low` and its `next` nodes using `Shared::boxed`
                        // and have never shared them
                        let low_bin = unsafe { BinEntry::Tree(TreeBin::new(low, guard)) };

                        Shared::boxed(low_bin, &self.collector)
                    } else {
                        // if not, we can re-use the old bin here, since it will
                        // be swapped for a Moved entry while we are still
                        // behind the bin lock.
                        reused_bin = true;
                        // since we also don't use the created low nodes here,
                        // we need to clean them up.
                        // safety: we have just created `low` and its `next`
                        // nodes and have never shared them
                        unsafe { TreeBin::drop_tree_nodes(low, false, guard) };
                        bin
                    };
                    let high_bin = if high_count <= UNTREEIFY_THRESHOLD {
                        let high_linear = self.untreeify(high, guard);
                        // safety: we have just created `high` and its `next`
                        // nodes and have never shared them
                        unsafe { TreeBin::drop_tree_nodes(high, false, guard) };
                        high_linear
                    } else if low_count != 0 {
                        // safety: we have just created `high` and its `next` nodes using `Shared::boxed`
                        // and have never shared them
                        let high_bin = unsafe { BinEntry::Tree(TreeBin::new(high, guard)) };

                        Shared::boxed(high_bin, &self.collector)
                    } else {
                        reused_bin = true;
                        // since we also don't use the created low nodes here,
                        // we need to clean them up.
                        // safety: we have just created `high` and its `next`
                        // nodes and have never shared them
                        unsafe { TreeBin::drop_tree_nodes(high, false, guard) };
                        bin
                    };

                    next_table.store_bin(i, low_bin);
                    next_table.store_bin(i + n, high_bin);
                    table.store_bin(i, table.get_moved(next_table_ptr, guard));

                    // if we did not re-use the old bin, it is now garbage,
                    // since all of its nodes have been reallocated. However,
                    // we always re-use the stored values, so we can't drop those.
                    if !reused_bin {
                        // safety: the entry for this bin in the old table was
                        // swapped for a Moved entry, so no thread can obtain a
                        // new reference to `bin` from there. we only defer
                        // dropping the old bin if it was not used in
                        // `next_table` so there is no other reference to it
                        // anyone could obtain.
                        unsafe { TreeBin::defer_drop_without_values(bin, guard) };
                    }

                    advance = true;
                    drop(bin_lock);
                }
                BinEntry::TreeNode(_) => unreachable!(
                    "The head of a bin cannot be a TreeNode directly without BinEntry::Tree"
                ),
            }
        }
    }

    fn help_transfer<'g>(
        &'g self,
        table: Shared<'g, Table<K, V>>,
        guard: &'g Guard<'_>,
    ) -> Shared<'g, Table<K, V>> {
        if table.is_null() {
            return table;
        }

        // safety: table is only retired after it is swapped to null.
        // we read it as not null while holding `guard`, so any thread
        // retiring the table must have seen us as active and included us
        // in the reference count. therefore our reference is valid until
        // we decrement the reference count (i.e drop our guard).
        let next_table = unsafe { table.deref() }.next_table(guard);
        if next_table.is_null() {
            return table;
        }

        // safety: same as above
        let rs = Self::resize_stamp(unsafe { table.deref() }.len()) << RESIZE_STAMP_SHIFT;

        while next_table == self.next_table.load(Ordering::SeqCst, guard)
            && table == self.table.load(Ordering::SeqCst, guard)
        {
            let sc = self.size_ctl.load(Ordering::SeqCst);
            if sc >= 0
                || sc == rs + MAX_RESIZERS
                || sc == rs + 1
                || self.transfer_index.load(Ordering::SeqCst) <= 0
            {
                break;
            }

            if self
                .size_ctl
                .compare_exchange(sc, sc + 1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                self.transfer(table, next_table, guard);
                break;
            }
        }
        next_table
    }

    fn add_count(&self, n: isize, resize_hint: Option<usize>, guard: &Guard<'_>) {
        if n != 0 {
            self.counter.add(n)
        }

        // if resize_hint is None, it means the caller does not want us to consider a resize.
        // if it is Some(n), the caller saw n entries in a bin
        // TODO: use the resize hint
        let Some(_saw_bin_length) = resize_hint else {
            return;
        };

        let mut count = self.counter.sum();

        loop {
            let sc = self.size_ctl.load(Ordering::SeqCst);
            if count < sc {
                // we're not at the next resize point yet
                break;
            }

            let table = self.table.load(Ordering::SeqCst, guard);
            if table.is_null() {
                // table will be initalized by another thread anyway
                break;
            }

            // safety: table is only retired after it is swapped to null.
            // we read it as not null while holding `guard`, so any thread
            // retiring the table must have seen us as active and included us
            // in the reference count. therefore our reference is valid until
            // we decrement the reference count (i.e drop our guard).
            let n = unsafe { table.deref() }.len();
            if n >= MAXIMUM_CAPACITY {
                // can't resize any more anyway
                break;
            }

            let rs = Self::resize_stamp(n) << RESIZE_STAMP_SHIFT;
            if sc < 0 {
                // ongoing resize! can we join the resize transfer?
                if sc == rs + MAX_RESIZERS || sc == rs + 1 {
                    break;
                }
                let nt = self.next_table.load(Ordering::SeqCst, guard);
                if nt.is_null() {
                    break;
                }
                if self.transfer_index.load(Ordering::SeqCst) <= 0 {
                    break;
                }

                // try to join!
                if self
                    .size_ctl
                    .compare_exchange(sc, sc + 1, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    self.transfer(table, nt, guard);
                }
            } else if self
                .size_ctl
                .compare_exchange(sc, rs + 2, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                // a resize is needed, but has not yet started
                // TODO: figure out why this is rs + 2, not just rs
                // NOTE: this also applies to `try_presize`
                self.transfer(table, Shared::null(), guard);
            }

            // another resize may be needed!
            count = self.counter.sum();
        }
    }

    /// Tries to reserve capacity for at least `additional` more elements to be inserted in the
    /// `HashMap`.
    ///
    /// The collection may reserve more space to avoid frequent reallocations.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map: HashMap<&str, i32> = HashMap::new();
    ///
    /// map.pin().reserve(10);
    /// ```
    ///
    /// # Notes
    ///
    /// Reserving does not panic in flurry. If the new size is invalid, no
    /// reallocation takes place.
    pub fn reserve(&self, additional: usize, guard: &Guard<'_>) {
        self.check_guard(guard);
        let absolute = self.len() + additional;
        self.try_presize(absolute, guard);
    }
}

// ===
// the following methods never introduce new items (so they do not need the thread-safety bounds),
// but they _do_ perform lookups, which require hashing and equality.
// ===

impl<K, V, S> HashMap<K, V, S>
where
    K: Hash + Ord,
    S: BuildHasher,
{
    #[inline]
    fn hash<Q: ?Sized + Hash>(&self, key: &Q) -> u64 {
        self.build_hasher.hash_one(key)
    }

    fn get_node<'g, Q>(&'g self, key: &Q, guard: &'g Guard<'_>) -> Option<&'g Node<K, V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        let table = self.table.load(Ordering::SeqCst, guard);
        if table.is_null() {
            return None;
        }

        // safety: we loaded the table while holding a guard.
        // table won't be deallocated until we drop our guard
        // at the earliest.
        let table = unsafe { table.deref() };
        if table.is_empty() {
            return None;
        }

        let h = self.hash(key);
        let bini = table.bini(h);
        let bin = table.bin(bini, guard);
        if bin.is_null() {
            return None;
        }

        // safety: bin is a valid pointer.
        //
        // there are three cases when a bin pointer is invalidated:
        //
        //  1. if the table was resized, bin is a move entry, and the resize has completed. in
        //     that case, the table (and all its heads) have already been retired.
        //  2. if the table is being resized, bin may be swapped with a move entry. the old bin
        //     will only be retired after that happens.
        //  3. when elements are inserted into or removed from the map, bin may be changed into
        //     or from a TreeBin from or into a regular, linear bin. the old bin will be
        //     retired only once that happens.
        //
        // in all cases, we held the guard when we got the reference to the bin. if any such
        // swap happened, it must have happened _after_ we read. since we did the read while
        // the current thread was marked as active, we must be included in the reference count,
        // and the drop must happen _after_ we decrement the count (i.e drop our guard).
        let node = table.find(unsafe { bin.deref() }, h, key, guard);
        if node.is_null() {
            return None;
        }

        // safety: we loaded the bin while holding a guard, so any retirements
        // must have seen us as active, and any future retirements must see us as active.
        // the bin and its nodes cannot be dropped until at least after we drop our guard.
        let node = unsafe { node.deref() };
        Some(match **node {
            BinEntry::Node(ref n) => n,
            BinEntry::TreeNode(ref tn) => &tn.node,
            _ => panic!("`Table::find` should always return a Node"),
        })
    }

    /// Returns `true` if the map contains a value for the specified key.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Ord`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Ord`]: std::cmp::Ord
    /// [`Hash`]: std::hash::Hash
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    /// let mref = map.pin();
    /// mref.insert(1, "a");
    /// assert_eq!(mref.contains_key(&1), true);
    /// assert_eq!(mref.contains_key(&2), false);
    /// ```
    pub fn contains_key<Q>(&self, key: &Q, guard: &Guard<'_>) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.check_guard(guard);
        self.get(key, guard).is_some()
    }

    /// Returns a reference to the value corresponding to the key.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Ord`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Ord`]: std::cmp::Ord
    /// [`Hash`]: std::hash::Hash
    ///
    /// To obtain a `Guard`, use [`HashMap::guard`].
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    /// let mref = map.pin();
    /// mref.insert(1, "a");
    /// assert_eq!(mref.get(&1), Some(&"a"));
    /// assert_eq!(mref.get(&2), None);
    /// ```
    #[inline]
    pub fn get<'g, Q>(&'g self, key: &Q, guard: &'g Guard<'_>) -> Option<&'g V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.check_guard(guard);
        let node = self.get_node(key, guard)?;

        let v = node.value.load(Ordering::SeqCst, guard);
        assert!(!v.is_null());
        // safety: the lifetime of the reference is bound to the guard
        // supplied which means that the memory will not be modified
        // until at least after the guard goes out of scope
        unsafe { v.as_ref().map(|linked| &**linked) }
    }

    /// Returns the key-value pair corresponding to `key`.
    ///
    /// Returns `None` if this map contains no mapping for `key`.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Ord`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Ord`]: std::cmp::Ord
    /// [`Hash`]: std::hash::Hash
    #[inline]
    pub fn get_key_value<'g, Q>(&'g self, key: &Q, guard: &'g Guard<'_>) -> Option<(&'g K, &'g V)>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.check_guard(guard);
        let node = self.get_node(key, guard)?;

        let v = node.value.load(Ordering::SeqCst, guard);
        assert!(!v.is_null());
        // safety: the lifetime of the reference is bound to the guard
        // supplied which means that the memory will not be modified
        // until at least after the guard goes out of scope
        unsafe { v.as_ref() }.map(|v| (&node.key, &**v))
    }

    pub(crate) fn guarded_eq(
        &self,
        other: &Self,
        our_guard: &Guard<'_>,
        their_guard: &Guard<'_>,
    ) -> bool
    where
        V: PartialEq,
    {
        if self.len() != other.len() {
            return false;
        }

        self.iter(our_guard)
            .all(|(key, value)| other.get(key, their_guard).map_or(false, |v| *value == *v))
    }
}

// ===
// the following methods only ever _remove_ items, but never introduce them, so they do not need
// the thread-safety bounds.
// ===

impl<K, V, S> HashMap<K, V, S>
where
    K: Clone + Ord,
{
    /// Clears the map, removing all key-value pairs.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    ///
    /// map.pin().insert(1, "a");
    /// map.pin().clear();
    /// assert!(map.pin().is_empty());
    /// ```
    pub fn clear(&self, guard: &Guard<'_>) {
        // Negative number of deletions
        let mut delta = 0;
        let mut idx = 0usize;

        let mut table = self.table.load(Ordering::SeqCst, guard);
        // Safety: self.table is a valid pointer because we checked it above.
        while !table.is_null() && idx < unsafe { table.deref() }.len() {
            let tab = unsafe { table.deref() };
            let raw_node = tab.bin(idx, guard);
            if raw_node.is_null() {
                idx += 1;
                continue;
            }
            // Safety: node is a valid pointer because we checked
            // it in the above if stmt.
            match **unsafe { raw_node.deref() } {
                BinEntry::Moved => {
                    table = self.help_transfer(table, guard);
                    // start from the first bin again in the new table
                    idx = 0;
                }
                BinEntry::Node(ref node) => {
                    let head_lock = node.lock.lock();
                    // need to check that this is _still_ the head
                    let current_head = tab.bin(idx, guard);
                    if current_head != raw_node {
                        // nope -- try the bin again
                        continue;
                    }
                    // we now own the bin
                    // unlink it from the map to prevent others from entering it
                    // NOTE: The Java code stores the null bin _after_ the loop, and thus also has
                    // to hold the lock until that point. However, after the store happens new
                    // threads and threads waiting on the lock will read the new bin, so we can
                    // drop the lock early and do the counting and garbage collection outside the
                    // critical section.
                    tab.store_bin(idx, Shared::null());
                    drop(head_lock);
                    // next, walk the nodes of the bin and free the nodes and their values as we go
                    // note that we do not free the head node yet, since we're holding the lock it contains
                    let mut p = node.next.load(Ordering::SeqCst, guard);
                    while !p.is_null() {
                        delta -= 1;
                        p = {
                            // safety: we loaded p under guard, and guard is still pinned, so p has not been dropped.
                            let node = unsafe { p.deref() }
                                .as_node()
                                .expect("entry following Node should always be a Node");
                            let next = node.next.load(Ordering::SeqCst, guard);
                            let value = node.value.load(Ordering::SeqCst, guard);
                            // NOTE: do not use the reference in `node` after this point!

                            // free the node's value
                            // safety: any thread that sees this p's value must have read the bin before we stored null
                            // into it above. it must also have already been marked as active. therefore, the
                            // defer_destroy below won't be executed until that thread's guard is dropped, at which
                            // point it holds no outstanding references to the value anyway.
                            unsafe { guard.retire_shared(value) };
                            // free the bin entry itself
                            // safety: same argument as for value above.
                            unsafe { guard.retire_shared(p) };
                            next
                        };
                    }
                    // finally, we can drop the head node and its value
                    let value = node.value.load(Ordering::SeqCst, guard);
                    // NOTE: do not use the reference in `node` after this point!
                    // safety: same as the argument for being allowed to free the nodes beyond the head above
                    unsafe { guard.retire_shared(value) };
                    unsafe { guard.retire_shared(raw_node) };
                    delta -= 1;
                    idx += 1;
                }
                BinEntry::Tree(ref tree_bin) => {
                    let bin_lock = tree_bin.lock.lock();
                    // need to check that this is _still_ the correct bin
                    let current_head = tab.bin(idx, guard);
                    if current_head != raw_node {
                        // nope -- try the bin again
                        continue;
                    }
                    // we now own the bin
                    // unlink it from the map to prevent others from entering it
                    // NOTE: The Java code stores the null bin _after_ the loop, and thus also has
                    // to hold the lock until that point. However, after the store happens new
                    // threads and threads waiting on the lock will read the new bin, so we can
                    // drop the lock early and do the counting and garbage collection outside the
                    // critical section.
                    tab.store_bin(idx, Shared::null());
                    drop(bin_lock);
                    // next, walk the nodes of the bin and count how many values we remove
                    let mut p = tree_bin.first.load(Ordering::SeqCst, guard);
                    while !p.is_null() {
                        delta -= 1;
                        p = {
                            // safety: we read under our guard, at which point the tree
                            // structure was valid. Since our guard marks the current thread
                            // as active, the TreeNodes remain valid for at least as long as
                            // we hold onto the guard.
                            // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                            let tree_node = unsafe { TreeNode::get_tree_node(p) };
                            // NOTE: we do not drop the TreeNodes or their
                            // values here, since they will be dropped together
                            // with the containing TreeBin (`tree_bin`) in its
                            // `drop`
                            tree_node.node.next.load(Ordering::SeqCst, guard)
                        };
                    }
                    // safety: same as in the BinEntry::Node case above
                    unsafe { guard.retire_shared(raw_node) };
                    idx += 1;
                }
                BinEntry::TreeNode(_) => unreachable!(
                    "The head of a bin cannot be a TreeNode directly without BinEntry::Tree"
                ),
            };
        }

        if delta != 0 {
            self.add_count(delta, None, guard);
        }
    }
}

// ===
// the following methods _do_ introduce items into the map, and so must require that the keys and
// values are thread safe, and can be garbage collected at a later time.
// ===

impl<K, V, S> HashMap<K, V, S>
where
    K: Sync + Send + Clone + Hash + Ord,
    V: Sync + Send,
    S: BuildHasher,
{
    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, [`None`] is returned.
    ///
    /// If the map did have this key present, the value is updated, and the old
    /// value is returned. The key is left unchanged. See the [std-collections
    /// documentation] for more.
    ///
    /// [`None`]: std::option::Option::None
    /// [std-collections documentation]: https://doc.rust-lang.org/std/collections/index.html#insert-and-complex-keys
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    /// assert_eq!(map.pin().insert(37, "a"), None);
    /// assert_eq!(map.pin().is_empty(), false);
    ///
    /// // you can also re-use a map pin like so:
    /// let mref = map.pin();
    ///
    /// mref.insert(37, "b");
    /// assert_eq!(mref.insert(37, "c"), Some(&"b"));
    /// assert_eq!(mref.get(&37), Some(&"c"));
    /// ```
    pub fn insert<'g>(&'g self, key: K, value: V, guard: &'g Guard<'_>) -> Option<&'g V> {
        self.check_guard(guard);
        self.put(key, value, false, guard).before()
    }

    /// Inserts a key-value pair into the map unless the key already exists.
    ///
    /// If the map does not contain the key, the key-value pair is inserted
    /// and this method returns `Ok`.
    ///
    /// If the map does contain the key, the map is left unchanged and this
    /// method returns `Err`.
    ///
    /// [std-collections documentation]: https://doc.rust-lang.org/std/collections/index.html#insert-and-complex-keys
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::{HashMap, TryInsertError};
    ///
    /// let map = HashMap::new();
    /// let mref = map.pin();
    ///
    /// mref.insert(37, "a");
    /// assert_eq!(
    ///     mref.try_insert(37, "b"),
    ///     Err(TryInsertError { current: &"a", not_inserted: &"b"})
    /// );
    /// assert_eq!(mref.try_insert(42, "c"), Ok(&"c"));
    /// assert_eq!(mref.get(&37), Some(&"a"));
    /// assert_eq!(mref.get(&42), Some(&"c"));
    /// ```
    #[inline]
    pub fn try_insert<'g>(
        &'g self,
        key: K,
        value: V,
        guard: &'g Guard<'_>,
    ) -> Result<&'g V, TryInsertError<'g, V>> {
        match self.put(key, value, true, guard) {
            PutResult::Exists {
                current,
                not_inserted,
            } => Err(TryInsertError {
                current,
                not_inserted: not_inserted.value,
            }),
            PutResult::Inserted { new } => Ok(new),
            PutResult::Replaced { .. } => {
                unreachable!("no_replacement cannot result in PutResult::Replaced")
            }
        }
    }

    fn put<'g>(
        &'g self,
        mut key: K,
        value: V,
        no_replacement: bool,
        guard: &'g Guard<'_>,
    ) -> PutResult<'g, V> {
        let hash = self.hash(&key);
        let mut table = self.table.load(Ordering::SeqCst, guard);
        let mut bin_count;
        let value = Shared::boxed(value, &self.collector);
        let mut old_val = None;
        loop {
            // safety: see argument below for !is_null case
            if table.is_null() || unsafe { table.deref() }.is_empty() {
                table = self.init_table(guard);
                continue;
            }

            // safety: table is a valid pointer.
            //
            // we are in one of three cases:
            //
            //  1. if table is the one we read before the loop, then we read it while holding the
            //     guard, so it won't be dropped until after we drop that guard b/c this thread must
            //     have been included in the reference count of a retirement.
            //
            //  2. if table is read by init_table, we did so while holding a guard, so the
            //     argument is as for point 1. or, we allocated the table while holding a guard,
            //     so the earliest it can be deallocated is after we drop our guard.
            //
            //  3. if table is set by a Moved node (below) through help_transfer, it will _either_
            //     keep using `table` (which is fine by 1. and 2.), or use the `next_table` raw
            //     pointer from inside the Moved. to see that if a Moved(t) is _read_, then t must
            //     still be valid, see the safety comment on Table.next_table.
            let t = unsafe { table.deref() };

            let bini = t.bini(hash);
            let mut bin = t.bin(bini, guard);
            if bin.is_null() {
                // fast path -- bin is empty so stick us at the front
                let node =
                    Shared::boxed(BinEntry::Node(Node::new(hash, key, value)), &self.collector);
                match t.cas_bin(bini, bin, node, guard) {
                    Ok(_old_null_ptr) => {
                        self.add_count(1, Some(0), guard);
                        // safety: we have not moved the node's value since we placed it into
                        // its `Atomic` in the very beginning of the method, so the ref is still
                        // valid. since the value is not currently marked as garbage, and since
                        // `value` was loaded under a guard, the returned reference will remain valid
                        // for the guard's lifetime.
                        return PutResult::Inserted {
                            new: unsafe { value.deref() },
                        };
                    }
                    Err(changed) => {
                        assert!(!changed.current.is_null());
                        bin = changed.current;
                        let BinEntry::Node(node) = unsafe { changed.new.into_box() }.value else {
                            unreachable!("we declared node and it is a BinEntry::Node");
                        };
                        key = node.key;
                    }
                }
            }

            // slow path -- bin is non-empty
            //
            // safety: bin is a valid pointer.
            //
            // there are three cases when a bin pointer is invalidated:
            //
            //  1. if the table was resized, bin is a move entry, and the resize has completed. in
            //     that case, the table (and all its heads) have already been retired.
            //  2. if the table is being resized, bin may be swapped with a move entry. the old bin
            //     will only be retired after that happens.
            //  3. when elements are inserted into or removed from the map, bin may be changed into
            //     or from a TreeBin from or into a regular, linear bin. the old bin will be
            //     retired only once that happens.
            //
            // in all cases, we held the guard when we got the reference to the bin. if any such
            // swap happened, it must have happened _after_ we read. since we did the read while
            // the current thread was marked as active, we must be included in the reference count,
            // and the drop must happen _after_ we decrement the count (i.e drop our guard).
            match **unsafe { bin.deref() } {
                BinEntry::Moved => {
                    table = self.help_transfer(table, guard);
                    continue;
                }
                BinEntry::Node(ref head)
                    if no_replacement && head.hash == hash && head.key == key =>
                {
                    // fast path if replacement is disallowed and first bin matches
                    let v = head.value.load(Ordering::SeqCst, guard);
                    // safety (for v): since the value is present now, and we've held a guard from
                    // the beginning of the search, the value cannot be dropped after we drop our guard.
                    // safety (for value): since we never inserted the value in the tree, `value`
                    // is the last remaining pointer to the initial value.
                    return PutResult::Exists {
                        current: unsafe { v.deref() },
                        not_inserted: unsafe { value.into_box() },
                    };
                }
                BinEntry::Node(ref head) => {
                    // bin is non-empty, need to link into it, so we must take the lock
                    let head_lock = head.lock.lock();

                    // need to check that this is _still_ the head
                    let current_head = t.bin(bini, guard);
                    if current_head != bin {
                        // nope -- try again from the start
                        continue;
                    }

                    // yes, it is still the head, so we can now "own" the bin
                    // note that there can still be readers in the bin!

                    // TODO: ReservationNode

                    bin_count = 1;
                    let mut p = bin;

                    old_val = loop {
                        // safety: we loaded the bin while holding a guard, so any retirements
                        // must have seen us as active. the bin and its nodes cannot be dropped
                        // until at least after we drop our guard.
                        let n = unsafe { p.deref() }.as_node().unwrap();
                        if n.hash == hash && n.key == key {
                            // the key already exists in the map!
                            let current_value = n.value.load(Ordering::SeqCst, guard);

                            // safety: since the value is present now, and we've held a guard from
                            // the beginning of the search, the value cannot be dropped until after
                            // we drop our guard.
                            let current_value = unsafe { current_value.deref() };

                            if no_replacement {
                                // the key is not absent, so don't update because of
                                // `no_replacement`, we don't use the new value, so we need to clean
                                // it up and return it back to the caller
                                // safety: we own value and did not share it
                                return PutResult::Exists {
                                    current: current_value,
                                    not_inserted: unsafe { value.into_box() },
                                };
                            } else {
                                // update the value in the existing node
                                let now_garbage = n.value.swap(value, Ordering::SeqCst, guard);
                                // NOTE: now_garbage == current_value

                                // safety: need to guarantee that now_garbage is no longer
                                // reachable. more specifically, no thread that executes _after_
                                // this line can ever get a reference to now_garbage.
                                //
                                // here are the possible cases:
                                //
                                //  - another thread already has a reference to now_garbage.
                                //    they must have read it before the call to swap while
                                //    marked as active (holding a guard), and are included in
                                //    the reference count. therefore t won't be freed until _after_
                                //    it decrements the reference count, which can only happen
                                //    when that thread drops its guard, and with it, any reference
                                //    to the value.
                                //  - another thread is about to get a reference to this value.
                                //    they execute _after_ the swap, and therefore do _not_ get a
                                //    reference to now_garbage (they get `value` instead). there are
                                //    no other ways to get to a value except through its Node's
                                //    `value` field (which is what we swapped), so freeing
                                //    now_garbage is fine.
                                unsafe { guard.retire_shared(now_garbage) };
                            }
                            break Some(current_value);
                        }

                        // TODO: This Ordering can probably be relaxed due to the Mutex
                        let next = n.next.load(Ordering::SeqCst, guard);
                        if next.is_null() {
                            // we're at the end of the bin -- stick the node here!
                            let node = Shared::boxed(
                                BinEntry::Node(Node::new(hash, key, value)),
                                &self.collector,
                            );
                            n.next.store(node, Ordering::SeqCst);
                            break None;
                        }
                        p = next;

                        bin_count += 1;
                    };
                    drop(head_lock);
                }
                // NOTE: BinEntry::Tree(ref tree_bin) if no_replacement && head.hash == h && &head.key == key
                // cannot occur as in the Java code, TreeBins have a special, indicator hash value
                BinEntry::Tree(ref tree_bin) => {
                    // bin is non-empty, need to link into it, so we must take the lock
                    let head_lock = tree_bin.lock.lock();

                    // need to check that this is _still_ the correct bin
                    let current_head = t.bin(bini, guard);
                    if current_head != bin {
                        // nope -- try again from the start
                        continue;
                    }

                    // yes, it is still the head, so we can now "own" the bin
                    // note that there can still be readers in the bin!

                    // we don't actually count bins, just set this low enough
                    // that we don't try to treeify the bin later
                    bin_count = 2;
                    let p = tree_bin.find_or_put_tree_val(hash, key, value, guard, &self.collector);
                    if p.is_null() {
                        // no TreeNode was returned, so the key did not previously exist in the
                        // TreeBin. This means it was successfully put there by the call above
                        // and we are done.
                        break;
                    }
                    // safety: the TreeBin was read under our guard, at which point the tree
                    // structure was valid. Since our guard marks the current thread as active,
                    // the TreeNodes remain valid for at least as long as we hold onto the
                    // guard.
                    // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                    let tree_node = unsafe { TreeNode::get_tree_node(p) };
                    old_val = {
                        let current_value = tree_node.node.value.load(Ordering::SeqCst, guard);
                        // safety: since the value is present now, and we've held a guard from
                        // the beginning of the search, the value cannot be dropped until after
                        // we drop our guard.
                        let current_value = unsafe { current_value.deref() };
                        if no_replacement {
                            // the key is not absent, so don't update because of
                            // `no_replacement`, we don't use the new value, so we need to clean
                            // it up and return it back to the caller
                            // safety: we own value and did not share it
                            return PutResult::Exists {
                                current: current_value,
                                not_inserted: unsafe { value.into_box() },
                            };
                        } else {
                            let now_garbage =
                                tree_node.node.value.swap(value, Ordering::SeqCst, guard);
                            // NOTE: now_garbage == current_value

                            // safety: need to guarantee that now_garbage is no longer
                            // reachable. more specifically, no thread that executes _after_
                            // this line can ever get a reference to now_garbage.
                            //
                            // here are the possible cases:
                            //
                            //  - another thread already has a reference to now_garbage.
                            //    they must have read it before the call to swap while
                            //    marked as active (holding a guard), and are included in
                            //    the reference count. therefore t won't be freed until _after_
                            //    it decrements the reference count, which can only happen
                            //    when that thread drops its guard, and with it, any reference
                            //    to the value.
                            //  - another thread is about to get a reference to this value.
                            //    they execute _after_ the swap, and therefore do _not_ get a
                            //    reference to now_garbage (they get `value` instead). there are
                            //    no other ways to get to a value except through its Node's
                            //    `value` field (which is what we swapped), so freeing
                            //    now_garbage is fine.
                            unsafe { guard.retire_shared(now_garbage) };
                        }
                        Some(current_value)
                    };
                    drop(head_lock);
                }
                BinEntry::TreeNode(_) => unreachable!(
                    "The head of a bin cannot be a TreeNode directly without BinEntry::Tree"
                ),
            }
            // NOTE: the Java code checks `bin_count` here because they also
            // reach this point if the bin changed while obtaining the lock.
            // However, our code doesn't (it uses continue) and `bin_count`
            // _cannot_ be 0 at this point.
            debug_assert_ne!(bin_count, 0);
            if bin_count >= TREEIFY_THRESHOLD {
                self.treeify_bin(t, bini, guard);
            }
            if let Some(old_val) = old_val {
                return PutResult::Replaced {
                    old: old_val,
                    // safety: we have not moved the node's value since we placed it into
                    // its `Atomic` in the very beginning of the method, so the ref is still
                    // valid. since the value is not currently marked as garbage, and since
                    // `value` was loaded under a guard, the returned reference will remain valid
                    // for the guard's lifetime.
                    new: unsafe { value.deref() },
                };
            }
            break;
        }
        // increment count, since we only get here if we did not return an old (updated) value
        debug_assert!(old_val.is_none());
        self.add_count(1, Some(bin_count), guard);
        PutResult::Inserted {
            // safety: we have not moved the node's value since we placed it into
            // its `Atomic` in the very beginning of the method, so the ref is still
            // valid. since the value is not currently marked as garbage, and since
            // `value` was loaded under a guard, the returned reference will remain valid
            // for the guard's lifetime.
            new: unsafe { value.deref() },
        }
    }

    fn put_all<I: Iterator<Item = (K, V)>>(&self, iter: I, guard: &Guard<'_>) {
        for (key, value) in iter {
            self.put(key, value, false, guard);
        }
    }

    /// If the value for the specified `key` is present, attempts to
    /// compute a new mapping given the key and its current mapped value.
    ///
    /// The new mapping is computed by the `remapping_function`, which may
    /// return `None` to signalize that the mapping should be removed.
    /// The entire method invocation is performed atomically.
    /// The supplied function is invoked exactly once per invocation of
    /// this method if the key is present, else not at all.  Some
    /// attempted update operations on this map by other threads may be
    /// blocked while computation is in progress, so the computation
    /// should be short and simple.
    ///
    /// Returns the new value associated with the specified `key`, or `None`
    /// if no value for the specified `key` is present.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Ord`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Ord`]: std::cmp::Ord
    /// [`Hash`]: std::hash::Hash
    pub fn compute_if_present<'g, Q, F>(
        &'g self,
        key: &Q,
        remapping_function: F,
        guard: &'g Guard<'_>,
    ) -> Option<&'g V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
        F: FnOnce(&K, &V) -> Option<V>,
    {
        self.check_guard(guard);
        let hash = self.hash(&key);

        let mut table = self.table.load(Ordering::SeqCst, guard);
        let mut new_val = None;
        let mut removed_node = false;
        let mut bin_count;
        loop {
            // safety: see argument below for !is_null case
            if table.is_null() || unsafe { table.deref() }.is_empty() {
                table = self.init_table(guard);
                continue;
            }

            // safety: table is a valid pointer.
            //
            // we are in one of three cases:
            //
            //  1. if table is the one we read before the loop, then we read it while holding the
            //     guard, so it won't be dropped until after we drop that guard b/c this thread must
            //     have been included in the reference count of a retirement.
            //
            //  2. if table is read by init_table, we did so while holding a guard, so the
            //     argument is as for point 1. or, we allocated the table while holding a guard,
            //     so the earliest it can be deallocated is after we drop our guard.
            //
            //  3. if table is set by a Moved node (below) through help_transfer, it will _either_
            //     keep using `table` (which is fine by 1. and 2.), or use the `next_table` raw
            //     pointer from inside the Moved. to see that if a Moved(t) is _read_, then t must
            //     still be valid, see the safety comment on Table.next_table.
            let t = unsafe { table.deref() };

            let bini = t.bini(hash);
            let bin = t.bin(bini, guard);
            if bin.is_null() {
                // fast path -- bin is empty so key is not present
                return None;
            }

            // slow path -- bin is non-empty
            //
            // safety: bin is a valid pointer.
            //
            // there are three cases when a bin pointer is invalidated:
            //
            //  1. if the table was resized, bin is a move entry, and the resize has completed. in
            //     that case, the table (and all its heads) have already been retired.
            //  2. if the table is being resized, bin may be swapped with a move entry. the old bin
            //     will only be retired after that happens.
            //  3. when elements are inserted into or removed from the map, bin may be changed into
            //     or from a TreeBin from or into a regular, linear bin. the old bin will be
            //     retired only once that happens.
            //
            // in all cases, we held the guard when we got the reference to the bin. if any such
            // swap happened, it must have happened _after_ we read. since we did the read while
            // the current thread was marked as active, we must be included in the reference count,
            // and the drop must happen _after_ we decrement the count (i.e drop our guard).
            match **unsafe { bin.deref() } {
                BinEntry::Moved => {
                    table = self.help_transfer(table, guard);
                    continue;
                }
                BinEntry::Node(ref head) => {
                    // bin is non-empty, need to link into it, so we must take the lock
                    let head_lock = head.lock.lock();

                    // need to check that this is _still_ the head
                    let current_head = t.bin(bini, guard);
                    if current_head != bin {
                        // nope -- try again from the start
                        continue;
                    }

                    // yes, it is still the head, so we can now "own" the bin
                    // note that there can still be readers in the bin!

                    // TODO: ReservationNode
                    bin_count = 1;
                    let mut p = bin;
                    let mut pred: Shared<'_, BinEntry<K, V>> = Shared::null();

                    new_val = loop {
                        // safety: we loaded the bin while holding a guard, so any retirements
                        // must have seen us as active. the bin and its nodes cannot be dropped
                        // until at least after we drop our guard.
                        let n = unsafe { p.deref() }.as_node().unwrap();
                        // TODO: This Ordering can probably be relaxed due to the Mutex
                        let next = n.next.load(Ordering::SeqCst, guard);
                        if n.hash == hash && n.key.borrow() == key {
                            // the key already exists in the map!
                            let current_value = n.value.load(Ordering::SeqCst, guard);

                            // safety: since the value is present now, and we've held a guard from
                            // the beginning of the search, the value cannot be dropped until after
                            // we drop our guard.
                            let new_value =
                                remapping_function(&n.key, unsafe { current_value.deref() });

                            if let Some(value) = new_value {
                                let value = Shared::boxed(value, &self.collector);
                                let now_garbage = n.value.swap(value, Ordering::SeqCst, guard);
                                // NOTE: now_garbage == current_value

                                // safety: need to guarantee that now_garbage is no longer
                                // reachable. more specifically, no thread that executes _after_
                                // this line can ever get a reference to now_garbage.
                                //
                                // here are the possible cases:
                                //
                                //  - another thread already has a reference to now_garbage.
                                //    they must have read it before the call to swap while
                                //    marked as active (holding a guard), and are included in
                                //    the reference count. therefore t won't be freed until _after_
                                //    it decrements the reference count, which can only happen
                                //    when that thread drops its guard, and with it, any reference
                                //    to the value.
                                //  - another thread is about to get a reference to this value.
                                //    they execute _after_ the swap, and therefore do _not_ get a
                                //    reference to now_garbage (they get `value` instead). there are
                                //    no other ways to get to a value except through its Node's
                                //    `value` field (which is what we swapped), so freeing
                                //    now_garbage is fine.
                                unsafe { guard.retire_shared(now_garbage) };

                                // safety: since the value is present now, and we've held a guard from
                                // the beginning of the search, the value cannot be dropped until after
                                // we drop our guard.
                                break Some(unsafe { value.deref() });
                            } else {
                                removed_node = true;
                                // remove the BinEntry containing the removed key value pair from the bucket
                                if !pred.is_null() {
                                    // either by changing the pointer of the previous BinEntry, if present
                                    // safety: see remove
                                    unsafe { pred.deref() }
                                        .as_node()
                                        .unwrap()
                                        .next
                                        .store(next, Ordering::SeqCst);
                                } else {
                                    // or by setting the next node as the first BinEntry if there is no previous entry
                                    t.store_bin(bini, next);
                                }

                                // in either case, mark the BinEntry as garbage, since it was just removed
                                // safety: need to guarantee that the old value is no longer
                                // reachable. more specifically, no thread that executes _after_
                                // this line can ever get a reference to val.
                                //
                                // here are the possible cases:
                                //
                                //  - another thread already has a reference to now_garbage.
                                //    they must have read it before the call to swap while
                                //    marked as active (holding a guard), and are included in
                                //    the reference count. therefore t won't be freed until _after_
                                //    it decrements the reference count, which can only happen
                                //    when that thread drops its guard, and with it, any reference
                                //    to the value.
                                //  - another thread is about to get a reference to this value.
                                //    they execute _after_ the swap, and therefore do _not_ get a
                                //    reference to now_garbage (they get `value` instead). there are
                                //    no other ways to get to a value except through its Node's
                                //    `value` field (which is what we swapped), so freeing
                                //    now_garbage is fine.
                                unsafe { guard.retire_shared(p) };
                                unsafe { guard.retire_shared(current_value) };
                                break None;
                            }
                        }

                        pred = p;
                        if next.is_null() {
                            // we're at the end of the bin
                            break None;
                        }
                        p = next;

                        bin_count += 1;
                    };
                    drop(head_lock);
                }
                BinEntry::Tree(ref tree_bin) => {
                    // bin is non-empty, need to link into it, so we must take the lock
                    let bin_lock = tree_bin.lock.lock();

                    // need to check that this is _still_ the head
                    let current_head = t.bin(bini, guard);
                    if current_head != bin {
                        // nope -- try again from the start
                        continue;
                    }

                    // yes, it is still the head, so we can now "own" the bin
                    // note that there can still be readers in the bin!
                    bin_count = 2;
                    let root = tree_bin.root.load(Ordering::SeqCst, guard);
                    if root.is_null() {
                        // TODO: Is this even reachable?
                        // The Java code has `NULL` checks for this, but in theory it should not be possible to
                        // encounter a tree that has no root when we have its lock. It should always have at
                        // least `UNTREEIFY_THRESHOLD` nodes. If it is indeed impossible we should replace
                        // this with an assertion instead.
                        break;
                    }
                    new_val = {
                        let p = TreeNode::find_tree_node(root, hash, key, guard);
                        if p.is_null() {
                            // the given key is not present in the map
                            None
                        } else {
                            // a node for the given key exists, so we try to update it
                            // safety: the TreeBin was read under our guard, at which point the tree
                            // structure was valid. Since our guard marks the current thread as active,
                            // the TreeNodes remain valid for at least as long as we hold onto the
                            // guard.
                            // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                            let n = &unsafe { TreeNode::get_tree_node(p) }.node;
                            let current_value = n.value.load(Ordering::SeqCst, guard);

                            // safety: since the value is present now, and we've held a guard from
                            // the beginning of the search, the value cannot be dropped until after
                            // we drop our guard.
                            let new_value =
                                remapping_function(&n.key, unsafe { current_value.deref() });

                            if let Some(value) = new_value {
                                let value = Shared::boxed(value, &self.collector);
                                let now_garbage = n.value.swap(value, Ordering::SeqCst, guard);
                                // NOTE: now_garbage == current_value

                                // safety: need to guarantee that now_garbage is no longer
                                // reachable. more specifically, no thread that executes _after_
                                // this line can ever get a reference to now_garbage.
                                //
                                // here are the possible cases:
                                //
                                //  - another thread already has a reference to now_garbage.
                                //    they must have read it before the call to swap while
                                //    marked as active (holding a guard), and are included in
                                //    the reference count. therefore t won't be freed until _after_
                                //    it decrements the reference count, which can only happen
                                //    when that thread drops its guard, and with it, any reference
                                //    to the value.
                                //  - another thread is about to get a reference to this value.
                                //    they execute _after_ the swap, and therefore do _not_ get a
                                //    reference to now_garbage (they get `value` instead). there are
                                //    no other ways to get to a value except through its Node's
                                //    `value` field (which is what we swapped), so freeing
                                //    now_garbage is fine.
                                unsafe { guard.retire_shared(now_garbage) };
                                // safety: since the value is present now, and we've held a guard from
                                // the beginning of the search, the value cannot be dropped until after
                                // we drop our guard.
                                Some(unsafe { value.deref() })
                            } else {
                                removed_node = true;
                                // remove the BinEntry::TreeNode containing the removed key value pair from the bucket
                                // also drop the old value stored in the tree node, as it was removed from the map
                                // safety: `p` and its value are either marked for garbage collection in `remove_tree_node`
                                // directly, or we will `need_to_untreeify`. In the latter case, we `defer_destroy`
                                // both `p` and its value below, after storing the linear bin. Thus, everything is
                                // always marked for garbage collection _after_ it becomes unaccessible by other threads.
                                let need_to_untreeify = unsafe {
                                    tree_bin.remove_tree_node(p, true, guard, &self.collector)
                                };
                                if need_to_untreeify {
                                    let linear_bin = self.untreeify(
                                        tree_bin.first.load(Ordering::SeqCst, guard),
                                        guard,
                                    );
                                    t.store_bin(bini, linear_bin);
                                    // the old bin is now garbage, but its values are not,
                                    // since they are re-used in the linear bin.
                                    // safety: in the same way as for `now_garbage` above, any existing
                                    // references to `bin` must have been obtained before storing the
                                    // linear bin. These references were obtained while holding a
                                    // guard, and are protected until they drop it and decrement
                                    // the reference count. After the store, threads will
                                    // always see the linear bin, so the cannot obtain new references either.
                                    //
                                    // The same holds for `p` and its value, which does not get dropped together
                                    // with `bin` here since `remove_tree_node` indicated that the bin needs to
                                    // be untreeified.
                                    unsafe {
                                        TreeBin::defer_drop_without_values(bin, guard);
                                        guard.retire_shared(p);
                                        guard.retire_shared(current_value);
                                    }
                                }
                                None
                            }
                        }
                    };
                    drop(bin_lock);
                }
                BinEntry::TreeNode(_) => unreachable!(
                    "The head of a bin cannot be a TreeNode directly without BinEntry::Tree"
                ),
            }
            // NOTE: the Java code checks `bin_count` here because they also
            // reach this point if the bin changed while obtaining the lock.
            // However, our code doesn't (it uses continue) and making this
            // break conditional confuses the compiler about how many times we
            // use the `remapping_function`.
            debug_assert_ne!(bin_count, 0);
            break;
        }
        if removed_node {
            // decrement count
            self.add_count(-1, Some(bin_count), guard);
        }
        new_val.map(|linked| &**linked)
    }

    /// Removes a key-value pair from the map, and returns the removed value (if any).
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Ord`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Ord`]: std::cmp::Ord
    /// [`Hash`]: std::hash::Hash
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    /// map.pin().insert(1, "a");
    /// assert_eq!(map.pin().remove(&1), Some(&"a"));
    /// assert_eq!(map.pin().remove(&1), None);
    /// ```
    pub fn remove<'g, Q>(&'g self, key: &Q, guard: &'g Guard<'_>) -> Option<&'g V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        // NOTE: _technically_, this method shouldn't require the thread-safety bounds, but a) that
        // would require special-casing replace_node for when new_value.is_none(), and b) it's sort
        // of useless to call remove on a collection that you know you can never insert into.
        self.check_guard(guard);
        self.replace_node(key, None, None, guard).map(|(_, v)| v)
    }

    /// Removes a key from the map, returning the stored key and value if the
    /// key was previously in the map.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Ord`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Ord`]: std::cmp::Ord
    /// [`Hash`]: std::hash::Hash
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    /// let guard = map.guard();
    /// map.insert(1, "a", &guard);
    /// assert_eq!(map.remove_entry(&1, &guard), Some((&1, &"a")));
    /// assert_eq!(map.remove(&1, &guard), None);
    /// ```
    pub fn remove_entry<'g, Q>(&'g self, key: &Q, guard: &'g Guard<'_>) -> Option<(&'g K, &'g V)>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.check_guard(guard);
        self.replace_node(key, None, None, guard)
    }

    /// Replaces node value with `new_value`.
    ///
    /// If an `observed_value` is provided, the replacement only happens if `observed_value` equals
    /// the value that is currently associated with the given key.
    ///
    /// If `new_value` is `None`, it removes the key (and its corresponding value) from this map.
    ///
    /// This method does nothing if the key is not in the map.
    ///
    /// Returns the previous key and value associated with the given key.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// [`Hash`] and [`Ord`] on the borrowed form *must* match those for
    /// the key type.
    ///
    /// [`Ord`]: std::cmp::Ord
    /// [`Hash`]: std::hash::Hash
    fn replace_node<'g, Q>(
        &'g self,
        key: &Q,
        new_value: Option<V>,
        observed_value: Option<Shared<'g, V>>,
        guard: &'g Guard<'_>,
    ) -> Option<(&'g K, &'g V)>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        let hash = self.hash(key);

        let is_remove = new_value.is_none();
        let mut old_val = None;
        let mut table = self.table.load(Ordering::SeqCst, guard);
        loop {
            if table.is_null() {
                break;
            }

            // safety: table is a valid pointer.
            //
            // we are in one of two cases:
            //
            //  1. if table is the one we read before the loop, then we read it while holding the
            //     guard, so it won't be dropped until after we drop that guard b/c this thread must
            //     have been included in the reference count of a retirement.
            //
            //  2. if table is set by a Moved node (below) through help_transfer, it will _either_
            //     keep using `table` (which is fine by 1. and 2.), or use the `next_table` raw
            //     pointer from inside the Moved. to see that if a Moved(t) is _read_, then t must
            //     still be valid, see the safety comment on Table.next_table.
            let t = unsafe { table.deref() };
            let n = t.len() as u64;
            if n == 0 {
                break;
            }
            let bini = t.bini(hash);
            let bin = t.bin(bini, guard);
            if bin.is_null() {
                break;
            }

            // safety: bin is a valid pointer.
            //
            // there are three cases when a bin pointer is invalidated:
            //
            //  1. if the table was resized, bin is a move entry, and the resize has completed. in
            //     that case, the table (and all its heads) have already been retired.
            //  2. if the table is being resized, bin may be swapped with a move entry. the old bin
            //     will only be retired after that happens.
            //  3. when elements are inserted into or removed from the map, bin may be changed into
            //     or from a TreeBin from or into a regular, linear bin. the old bin will be
            //     retired only once that happens.
            //
            // in all cases, we held the guard when we got the reference to the bin. if any such
            // swap happened, it must have happened _after_ we read. since we did the read while
            // the current thread was marked as active, we must be included in the reference count,
            // and the drop must happen _after_ we decrement the count (i.e drop our guard).
            match **unsafe { bin.deref() } {
                BinEntry::Moved => {
                    table = self.help_transfer(table, guard);
                    continue;
                }
                BinEntry::Node(ref head) => {
                    let head_lock = head.lock.lock();

                    // need to check that this is _still_ the head
                    if t.bin(bini, guard) != bin {
                        continue;
                    }

                    let mut e = bin;
                    let mut pred: Shared<'_, BinEntry<K, V>> = Shared::null();
                    loop {
                        // safety: either `e` is `bin`, in which case it is valid due to the above,
                        // or e was obtained from a next pointer. Any next pointer obtained from
                        // bin is valid at the time we look up bin in the table, at which point
                        // we held a guard. Since we found the next pointer in a valid map and
                        // it is not null (as checked above and below), the node it points to was
                        // present (i.e. not removed) from the map while we were marked as active.
                        // Thus, e cannot be dropped until we release our guard, so e is also valid
                        // if it was obtained from a next pointer.
                        let n = unsafe { e.deref() }.as_node().unwrap();
                        let next = n.next.load(Ordering::SeqCst, guard);
                        if n.hash == hash && n.key.borrow() == key {
                            let ev = n.value.load(Ordering::SeqCst, guard);

                            // only replace the node if the value is the one we expected at method call
                            if observed_value.map(|ov| ov == ev).unwrap_or(true) {
                                // we remember the old value so that we can return it and mark it for deletion below
                                old_val = Some((&n.key, ev));

                                // found the node but we have a new value to replace the old one
                                if let Some(nv) = new_value {
                                    n.value.store(
                                        Shared::boxed(nv, &self.collector),
                                        Ordering::SeqCst,
                                    );
                                    // we are just replacing entry value and we do not want to remove the node
                                    // so we stop iterating here
                                    break;
                                }
                                // remove the BinEntry containing the removed key value pair from the bucket
                                if !pred.is_null() {
                                    // either by changing the pointer of the previous BinEntry, if present
                                    // safety: as above
                                    unsafe { pred.deref() }
                                        .as_node()
                                        .unwrap()
                                        .next
                                        .store(next, Ordering::SeqCst);
                                } else {
                                    // or by setting the next node as the first BinEntry if there is no previous entry
                                    t.store_bin(bini, next);
                                }

                                // in either case, mark the BinEntry as garbage, since it was just removed
                                // safety: as for val below / in put
                                unsafe { guard.retire_shared(e) };
                            }
                            // since the key was found and only one node exists per key, we can break here
                            break;
                        }
                        pred = e;
                        if next.is_null() {
                            break;
                        } else {
                            e = next;
                        }
                    }
                    drop(head_lock);
                }
                BinEntry::Tree(ref tree_bin) => {
                    let bin_lock = tree_bin.lock.lock();

                    // need to check that this is _still_ the head
                    if t.bin(bini, guard) != bin {
                        continue;
                    }

                    let root = tree_bin.root.load(Ordering::SeqCst, guard);
                    if root.is_null() {
                        // we are in the correct bin for the given key's hash and the bin is not
                        // `Moved`, but it is empty. This means there is no node for the given
                        // key that could be replaced
                        // TODO: Is this even reachable?
                        // The Java code has `NULL` checks for this, but in theory it should not be possible to
                        // encounter a tree that has no root when we have its lock. It should always have at
                        // least `UNTREEIFY_THRESHOLD` nodes. If it is indeed impossible we should replace
                        // this with an assertion instead.
                        break;
                    }
                    let p = TreeNode::find_tree_node(root, hash, key, guard);
                    if p.is_null() {
                        // similarly to the above, there now are entries in this bin, but none of
                        // them matches the given key
                        break;
                    }
                    // safety: the TreeBin was read our guard, at which point the tree
                    // structure was valid. Since our guard marks the current thread as active,
                    // the TreeNodes remain valid for at least as long as we hold onto the
                    // guard.
                    // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                    let n = &unsafe { TreeNode::get_tree_node(p) }.node;
                    let pv = n.value.load(Ordering::SeqCst, guard);

                    // only replace the node if the value is the one we expected at method call
                    if observed_value.map(|ov| ov == pv).unwrap_or(true) {
                        // we remember the old value so that we can return it and mark it for deletion below
                        old_val = Some((&n.key, pv));

                        if let Some(nv) = new_value {
                            // found the node but we have a new value to replace the old one
                            n.value
                                .store(Shared::boxed(nv, &self.collector), Ordering::SeqCst);
                        } else {
                            // drop `p` without its value, since the old value is dropped
                            // in the check on `old_val` below
                            // safety: `p` is either marked for garbage collection in `remove_tree_node` directly,
                            // or we will `need_to_untreeify`. In the latter case, we `defer_destroy` `p` below,
                            // after storing the linear bin. The value stored in `p` is `defer_destroy`ed from within
                            // `old_val` at the end of the method. Thus, everything is always marked for garbage
                            // collection _after_ it becomes unaccessible by other threads.
                            let need_to_untreeify = unsafe {
                                tree_bin.remove_tree_node(p, false, guard, &self.collector)
                            };
                            if need_to_untreeify {
                                let linear_bin = self
                                    .untreeify(tree_bin.first.load(Ordering::SeqCst, guard), guard);
                                t.store_bin(bini, linear_bin);
                                // the old bin is now garbage, but its values are not,
                                // since they get re-used in the linear bin
                                // safety: same as in put
                                unsafe {
                                    TreeBin::defer_drop_without_values(bin, guard);
                                    guard.retire_shared(p);
                                }
                            }
                        }
                    }

                    drop(bin_lock);
                }
                BinEntry::TreeNode(_) => unreachable!(
                    "The head of a bin cannot be a TreeNode directly without BinEntry::Tree"
                ),
            }
            if let Some((key, val)) = old_val {
                if is_remove {
                    self.add_count(-1, None, guard);
                }

                // safety: need to guarantee that the old value is no longer
                // reachable. more specifically, no thread that executes _after_
                // this line can ever get a reference to val.
                //
                // here are the possible cases:
                //
                //  - another thread already has a reference to now_garbage.
                //    they must have read it before the call to swap while
                //    marked as active (holding a guard), and are included in
                //    the reference count. therefore t won't be freed until _after_
                //    it decrements the reference count, which can only happen
                //    when that thread drops its guard, and with it, any reference
                //    to the value.
                //  - another thread is about to get a reference to this value.
                //    they execute _after_ the swap, and therefore do _not_ get a
                //    reference to now_garbage (they get `value` instead). there are
                //    no other ways to get to a value except through its Node's
                //    `value` field (which is what we swapped), so freeing
                //    now_garbage is fine.
                unsafe { guard.retire_shared(val) };

                // safety: the lifetime of the reference is bound to the guard
                // supplied which means that the memory will not be freed
                // until at least after the guard goes out of scope
                return unsafe { val.as_ref() }.map(move |v| (key, &**v));
            }
            break;
        }
        None
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all pairs `(k, v)` such that `f(&k,&v)` returns `false`.
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    ///
    /// for i in 0..8 {
    ///     map.pin().insert(i, i*10);
    /// }
    /// map.pin().retain(|&k, _| k % 2 == 0);
    /// assert_eq!(map.pin().len(), 4);
    /// ```
    ///
    /// # Notes
    ///
    /// If `f` returns `false` for a given key/value pair, but the value for that pair is concurrently
    /// modified before the removal takes place, the entry will not be removed.
    /// If you want the removal to happen even in the case of concurrent modification, use [`HashMap::retain_force`].
    pub fn retain<F>(&self, mut f: F, guard: &Guard<'_>)
    where
        F: FnMut(&K, &V) -> bool,
    {
        self.check_guard(guard);
        let mut iter = self.iter(guard);
        while let Some((k, v)) = iter.next_internal() {
            // safety: flurry does not drop or move until after guard drop
            let value = unsafe { v.deref() };
            if !f(k, value) {
                self.replace_node(k, None, Some(v), guard);
            }
        }
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all pairs `(k, v)` such that `f(&k,&v)` returns `false`.
    ///
    /// This method always deletes any key/value pair that `f` returns `false` for, even if the
    /// value is updated concurrently. If you do not want that behavior, use [`HashMap::retain`].
    ///
    /// # Examples
    ///
    /// ```
    /// use flurry::HashMap;
    ///
    /// let map = HashMap::new();
    ///
    /// for i in 0..8 {
    ///     map.pin().insert(i, i*10);
    /// }
    /// map.pin().retain_force(|&k, _| k % 2 == 0);
    /// assert_eq!(map.pin().len(), 4);
    /// ```
    pub fn retain_force<F>(&self, mut f: F, guard: &Guard<'_>)
    where
        F: FnMut(&K, &V) -> bool,
    {
        self.check_guard(guard);
        // removed selected keys
        for (k, v) in self.iter(guard) {
            if !f(k, v) {
                self.replace_node(k, None, None, guard);
            }
        }
    }
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Clone + Ord,
{
    /// Replaces all linked nodes in the bin at the given index unless the table
    /// is too small, in which case a resize is initiated instead.
    fn treeify_bin<'g>(&'g self, tab: &Table<K, V>, index: usize, guard: &'g Guard<'_>) {
        let n = tab.len();
        if n < MIN_TREEIFY_CAPACITY {
            self.try_presize(n << 1, guard);
        } else {
            let bin = tab.bin(index, guard);
            if bin.is_null() {
                return;
            }
            // safety: we loaded `bin` while holding a guard.
            // if the bin was replaced since then, the old bin still
            // won't be dropped until after we release our guard.
            match **unsafe { bin.deref() } {
                BinEntry::Node(ref node) => {
                    let lock = node.lock.lock();
                    // check if `bin` is still the head
                    if tab.bin(index, guard) != bin {
                        return;
                    }
                    let mut e = bin;
                    let mut head = Shared::null();
                    let mut tail = Shared::null();
                    while !e.is_null() {
                        // safety: either `e` is `bin`, in which case it is valid due to the above,
                        // or e was obtained from a next pointer. Any next pointer obtained from
                        // bin is valid at the time we look up bin in the table, at which point
                        // we held a guard. Since we found the next pointer in a valid map and
                        // it is not null (as checked above and below), the node it points to was
                        // present (i.e. not removed) from the map while we were marked as active.
                        // Thus, e cannot be dropped until we release our guard, so e is also valid
                        // if it was obtained from a next pointer.
                        let e_deref = unsafe { e.deref() }.as_node().unwrap();
                        // NOTE: cloning the value uses a load with Ordering::Relaxed, but
                        // write access is synchronized through the bin lock
                        let new_tree_node = TreeNode::new(
                            e_deref.hash,
                            e_deref.key.clone(),
                            e_deref.value.clone(),
                            Atomic::null(),
                            Atomic::null(),
                        );
                        new_tree_node.prev.store(tail, Ordering::Relaxed);
                        let new_tree_node =
                            Shared::boxed(BinEntry::TreeNode(new_tree_node), &self.collector);
                        if tail.is_null() {
                            // this was the first TreeNode, so it becomes the head
                            head = new_tree_node;
                        } else {
                            // safety: if `tail` is not `null`, we have just created
                            // it in the last iteration, thus the pointer is valid
                            unsafe { tail.deref() }
                                .as_tree_node()
                                .unwrap()
                                .node
                                .next
                                .store(new_tree_node, Ordering::Relaxed);
                        }
                        tail = new_tree_node;
                        e = e_deref.next.load(Ordering::SeqCst, guard);
                    }

                    // safety: we have just created `head` and its `next` nodes using `Shared::boxed`
                    // and have never shared them
                    let head_bin = unsafe { BinEntry::Tree(TreeBin::new(head, guard)) };
                    tab.store_bin(index, Shared::boxed(head_bin, &self.collector));
                    drop(lock);
                    // make sure the old bin entries get dropped
                    e = bin;
                    while !e.is_null() {
                        // safety: we just replaced the bin containing this BinEntry, making it
                        // unreachable for other threads since subsequent loads will see the new
                        // bin. Threads with existing references to `e` must have obtained them
                        // while holding guards. Thus, `e` will only be dropped after these threads
                        // release their guard, at which point they can no longer hold their reference
                        // to `e`. Any loads after this point will see the new bin.
                        // The BinEntry pointers are valid to deref for the same reason as above.
                        //
                        // NOTE: we do not drop the value, since it gets moved to the new TreeNode
                        unsafe {
                            guard.retire_shared(e);
                            e = e
                                .deref()
                                .as_node()
                                .unwrap()
                                .next
                                .load(Ordering::SeqCst, guard);
                        }
                    }
                }
                BinEntry::Moved | BinEntry::Tree(_) => {
                    // The bin we wanted to treeify has changed under us. This is possible because
                    // the call to `treeify_bin` does not happen inside the critical section of its
                    // callers (while they are holding the lock). To see why, consider the
                    // implications for the cases we match here:
                    //
                    //   BinEntry::Moved:
                    //     One thread inserts a new entry and passes the `TREEIFY_THRESHOLD`, but a
                    //     different thread executes a move of that bin. If we _always_ forced
                    //     treeification to happen after the insert while still holding the lock, the
                    //     move would have to wait for the bin lock and would then move the treeified
                    //     bin. It is very likely that the move will split the bin in question into two
                    //     smaller bins, both below the threshold, and has to untreeify the bin again
                    //     (since the bin we inserted to _just_ passed the threshold right before the
                    //     move).
                    //       If we instead try to treeify after the releasing the lock, and due to
                    //     scheduling the move happens first, it is fine for us as the first thread to
                    //     see the `Moved` when we re-check here and just not treeify the bin. If either
                    //     of the two bins in the new table (after the bin is moved) is still large
                    //     enough to be above the `TREEIFY_THRESHOLD`, it will still get treeified in
                    //     the _new_ table with the next insert.
                    //   BinEntry::Tree(_):
                    //     In the same scenario of trying to treeify _outside_ of the critical section,
                    //     if there is one insert passing the threshold there may then be another insert
                    //     before the first insert actually gets to do the treeification (due to
                    //     scheduling). This second insert might also get to treeifying the bin (it will
                    //     try because it sees a regular bin and the number of elements in the bin is
                    //     still above the threshold). When the first thread resumes and re-checks here,
                    //     the bin is already treeified and so it is again fine to not treeify it here.
                    //       Of course there is a tradeoff here where the second insert would already have
                    //     happend into a tree bin if we forced the first thread to treeify while still
                    //     holding the lock. However, the second thread would then also have to wait for
                    //     the lock before executing its insert.
                    //
                    // With the above reasoning, we choose to minimize the time any thread holds the
                    // lock and allow other threads to possibly mutate the bin we want to treeify
                    // before we get to do just that. If we encounter such a situation, we don't
                    // need to perform any action on the bin anymore, since either it has already
                    // been treeified or it was moved to a new table.
                }
                BinEntry::TreeNode(_) => unreachable!("TreeNode cannot be the head of a bin"),
            }
        }
    }

    /// Returns a list of non-TreeNodes replacing those in the given list. Does
    /// _not_ clean up old TreeNodes, as they may still be reachable.
    fn untreeify<'g>(
        &self,
        bin: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard<'_>,
    ) -> Shared<'g, BinEntry<K, V>> {
        let mut head = Shared::null();
        let mut tail: Shared<'_, BinEntry<K, V>> = Shared::null();
        let mut q = bin;
        while !q.is_null() {
            // safety: we only untreeify sequences of TreeNodes which either
            //  - were just created (e.g. in transfer) and are thus valid, or
            //  - are read from a TreeBin loaded from the map. In this case,
            //    the bin gets loaded under our guard and at that point all
            //    of its nodes (its `first` and all `next` nodes) are valid.
            //    As `q` is not `null` (checked above), this means that `q`
            //    remains a valid pointer at least until our guard is dropped.
            let q_deref = unsafe { q.deref() }.as_tree_node().unwrap();
            // NOTE: cloning the value uses a load with Ordering::Relaxed, but
            // write access is synchronized through the bin lock
            let new_node = Shared::boxed(
                BinEntry::Node(Node::new(
                    q_deref.node.hash,
                    q_deref.node.key.clone(),
                    q_deref.node.value.clone(),
                )),
                &self.collector,
            );
            if tail.is_null() {
                head = new_node;
            } else {
                // safety: if `tail` is not `null`, we have just created it
                // in the last iteration, thus the pointer is valid
                unsafe { tail.deref() }
                    .as_node()
                    .unwrap()
                    .next
                    .store(new_node, Ordering::Relaxed);
            }
            tail = new_node;
            q = q_deref.node.next.load(Ordering::Relaxed, guard);
        }

        head
    }
}
impl<K, V, S> PartialEq for HashMap<K, V, S>
where
    K: Ord + Hash,
    V: PartialEq,
    S: BuildHasher,
{
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.guarded_eq(other, &self.guard(), &other.guard())
    }
}

impl<K, V, S> Eq for HashMap<K, V, S>
where
    K: Ord + Hash,
    V: Eq,
    S: BuildHasher,
{
}

impl<K, V, S> fmt::Debug for HashMap<K, V, S>
where
    K: Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let guard = self.collector.enter();
        f.debug_map().entries(self.iter(&guard)).finish()
    }
}

impl<K, V, S> Drop for HashMap<K, V, S> {
    fn drop(&mut self) {
        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references to
        // anything in the map.
        //
        // NOTE: we _could_ relax the bounds in all the methods that return `&'g ...` to not also
        // bound `&self` by `'g`, but if we did that, we would need to use a regular `Guard`
        // here rather than an unprotected one.
        let guard = unsafe { Guard::unprotected() };

        assert!(self.next_table.load(Ordering::SeqCst, &guard).is_null());
        let table = self.table.swap(Shared::null(), Ordering::SeqCst, &guard);
        if table.is_null() {
            // table was never allocated!
            return;
        }

        // safety: same as above + we own the table
        let mut table = unsafe { table.into_box() };
        table.drop_bins();
    }
}

impl<K, V, S> Extend<(K, V)> for &HashMap<K, V, S>
where
    K: Sync + Send + Clone + Hash + Ord,
    V: Sync + Send,
    S: BuildHasher,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        // from `hashbrown::HashMap::extend`:
        // Keys may be already present or show multiple times in the iterator.
        // Reserve the entire hint lower bound if the map is empty.
        // Otherwise reserve half the hint (rounded up), so the map
        // will only resize twice in the worst case.
        let iter = iter.into_iter();
        let reserve = if self.is_empty() {
            iter.size_hint().0
        } else {
            (iter.size_hint().0 + 1) / 2
        };

        let guard = self.collector.enter();
        self.reserve(reserve, &guard);
        (*self).put_all(iter, &guard);
    }
}

impl<'a, K, V, S> Extend<(&'a K, &'a V)> for &HashMap<K, V, S>
where
    K: Sync + Send + Copy + Hash + Ord,
    V: Sync + Send + Copy,
    S: BuildHasher,
{
    fn extend<T: IntoIterator<Item = (&'a K, &'a V)>>(&mut self, iter: T) {
        self.extend(iter.into_iter().map(|(&key, &value)| (key, value)));
    }
}

impl<K, V, S> FromIterator<(K, V)> for HashMap<K, V, S>
where
    K: Sync + Send + Clone + Hash + Ord,
    V: Sync + Send,
    S: BuildHasher + Default,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut iter = iter.into_iter();

        if let Some((key, value)) = iter.next() {
            // safety: we own `map`, so it's not concurrently accessed by
            // anyone else at this point.
            let guard = unsafe { Guard::unprotected() };

            let (lower, _) = iter.size_hint();
            let map = HashMap::with_capacity_and_hasher(lower.saturating_add(1), S::default());

            map.put(key, value, false, &guard);
            map.put_all(iter, &guard);
            map
        } else {
            Self::default()
        }
    }
}

impl<'a, K, V, S> FromIterator<(&'a K, &'a V)> for HashMap<K, V, S>
where
    K: Sync + Send + Copy + Hash + Ord,
    V: Sync + Send + Copy,
    S: BuildHasher + Default,
{
    fn from_iter<T: IntoIterator<Item = (&'a K, &'a V)>>(iter: T) -> Self {
        iter.into_iter().map(|(&k, &v)| (k, v)).collect()
    }
}

impl<'a, K, V, S> FromIterator<&'a (K, V)> for HashMap<K, V, S>
where
    K: Sync + Send + Copy + Hash + Ord,
    V: Sync + Send + Copy,
    S: BuildHasher + Default,
{
    fn from_iter<T: IntoIterator<Item = &'a (K, V)>>(iter: T) -> Self {
        iter.into_iter().map(|&(k, v)| (k, v)).collect()
    }
}

impl<K, V, S> Clone for HashMap<K, V, S>
where
    K: Sync + Send + Clone + Hash + Ord,
    V: Sync + Send + Clone,
    S: BuildHasher + Clone,
{
    fn clone(&self) -> HashMap<K, V, S> {
        let cloned_map = Self::with_capacity_and_hasher(self.len(), self.build_hasher.clone())
            .with_collector(self.collector.clone());

        {
            let guard = self.collector.enter();
            let cloned_guard = cloned_map.collector.enter();
            for (k, v) in self.iter(&guard) {
                cloned_map.insert(k.clone(), v.clone(), &cloned_guard);
            }
        }
        cloned_map
    }
}

#[cfg(not(miri))]
#[inline]
/// Returns the number of physical CPUs in the machine (_O(1)_).
fn num_cpus() -> usize {
    NCPU_INITIALIZER.call_once(|| NCPU.store(num_cpus::get_physical(), Ordering::Relaxed));
    NCPU.load(Ordering::Relaxed)
}

#[cfg(miri)]
#[inline]
const fn num_cpus() -> usize {
    1
}

#[test]
fn capacity() {
    let map = HashMap::<usize, usize>::new();
    let guard = map.guard();

    assert_eq!(map.capacity(&guard), 0);
    // The table has not yet been allocated

    map.insert(42, 0, &guard);

    assert_eq!(map.capacity(&guard), 16);
    // The table has been allocated and has default capacity

    for i in 0..16 {
        map.insert(i, 42, &guard);
    }

    assert_eq!(map.capacity(&guard), 32);
    // The table has been resized once (and it's capacity doubled),
    // since we inserted more elements than it can hold
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reserve() {
        let map = HashMap::<usize, usize>::new();
        let guard = map.guard();

        map.insert(42, 0, &guard);

        map.reserve(32, &guard);

        let capacity = map.capacity(&guard);
        assert!(capacity >= 16 + 32);
    }

    #[test]
    fn reserve_uninit() {
        let map = HashMap::<usize, usize>::new();
        let guard = map.guard();

        map.reserve(32, &guard);

        let capacity = map.capacity(&guard);
        assert!(capacity >= 32);
    }

    #[test]
    fn resize_stamp_negative() {
        let resize_stamp = HashMap::<usize, usize>::resize_stamp(1);
        assert!(resize_stamp << RESIZE_STAMP_SHIFT < 0);

        let resize_stamp = HashMap::<usize, usize>::resize_stamp(MAXIMUM_CAPACITY);
        assert!(resize_stamp << RESIZE_STAMP_SHIFT < 0);
    }
}

/// It's kind of stupid, but apparently there is no way to write a regular `#[test]` that is _not_
/// supposed to compile without pulling in `compiletest` as a dependency. See rust-lang/rust#12335.
/// But it _is_ possible to write `compile_test` tests as doctests, sooooo:
///
/// # No references outlive the map.
///
/// Note that these have to be _separate_ tests, otherwise you could have, say, `get` break the
/// contract, but `insert` continue to uphold it, and then the test would still fail to compile
/// (and so pass).
///
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.insert((), (), &guard);
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.get(&(), &guard);
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.remove(&(), &guard);
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.iter(&guard).next();
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.keys(&guard).next();
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.values(&guard).next();
/// drop(map);
/// drop(r);
/// ```
///
/// # No references outlive the guard.
///
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.insert((), (), &guard);
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.get(&(), &guard);
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.remove(&(), &guard);
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.iter(&guard).next();
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.keys(&guard).next();
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = map.pin();
/// let map = super::HashMap::default();
/// let r = map.values(&guard).next();
/// drop(guard);
/// drop(r);
/// ```
///
/// # Keys and values do not have be static
///
/// ```
/// let x = String::from("foo");
/// let map: flurry::HashMap<_, _> = std::iter::once((&x, &x)).collect();
/// ```
/// ```
/// let x = String::from("foo");
/// let map: flurry::HashMap<_, _> = flurry::HashMap::new();
/// map.insert(&x, &x, &map.guard());
/// ```
///
/// # get() key can be non-static
///
/// ```
/// let x = String::from("foo");
/// let map: flurry::HashMap<_, _> = flurry::HashMap::new();
/// map.insert(x.clone(), x.clone(), &map.guard());
/// map.get(&x, &map.guard());
/// ```
#[allow(dead_code)]
struct CompileFailTests;

#[test]
fn replace_empty() {
    let map = HashMap::<usize, usize>::new();

    {
        let guard = map.guard();
        assert_eq!(map.len(), 0);
        let old = map.replace_node(&42, None, None, &guard);
        assert_eq!(map.len(), 0);
        assert!(old.is_none());
    }
}

#[test]
fn replace_existing() {
    let map = HashMap::<usize, usize>::new();
    {
        let guard = map.guard();
        map.insert(42, 42, &guard);
        assert_eq!(map.len(), 1);
        let old = map.replace_node(&42, Some(10), None, &guard);
        assert_eq!(old, Some((&42, &42)));
        assert_eq!(*map.get(&42, &guard).unwrap(), 10);
        assert_eq!(map.len(), 1);
    }
}

#[test]
fn no_replacement_return_val() {
    // NOTE: this test also serves as a leak test for the injected value
    let map = HashMap::<usize, String>::new();
    {
        let guard = map.guard();
        map.insert(42, String::from("hello"), &guard);
        assert_eq!(
            map.put(42, String::from("world"), true, &guard),
            PutResult::Exists {
                current: &String::from("hello"),
                not_inserted: Box::new(map.collector.link_value(String::from("world"))),
            }
        );
    }
}

// TODO
// #[test]
// fn replace_existing_observed_value_matching() {
//     let map = HashMap::<usize, usize>::new();
//     {
//         let guard = map.guard();
//         map.insert(42, 42, &guard);
//         assert_eq!(map.len(), 1);
//         let observed_value = Shared::from(map.get(&42, &guard).unwrap() as *const _);
//         let old = map.replace_node(&42, Some(10), Some(observed_value), &guard);
//         assert_eq!(map.len(), 1);
//         assert_eq!(old, Some((&42, &42)));
//         assert_eq!(*map.get(&42, &guard).unwrap(), 10);
//     }
// }

#[test]
fn replace_existing_observed_value_non_matching() {
    let map = HashMap::<usize, usize>::new();
    {
        let guard = map.guard();
        map.insert(42, 42, &guard);
        assert_eq!(map.len(), 1);
        let old = map.replace_node(&42, Some(10), Some(Shared::null()), &guard);
        assert_eq!(map.len(), 1);
        assert!(old.is_none());
        assert_eq!(*map.get(&42, &guard).unwrap(), 42);
    }
}

#[test]
fn replace_twice() {
    let map = HashMap::<usize, usize>::new();
    {
        let guard = map.guard();
        map.insert(42, 42, &guard);
        assert_eq!(map.len(), 1);
        let old = map.replace_node(&42, Some(43), None, &guard);
        assert_eq!(map.len(), 1);
        assert_eq!(old, Some((&42, &42)));
        assert_eq!(*map.get(&42, &guard).unwrap(), 43);
        let old = map.replace_node(&42, Some(44), None, &guard);
        assert_eq!(map.len(), 1);
        assert_eq!(old, Some((&42, &43)));
        assert_eq!(*map.get(&42, &guard).unwrap(), 44);
    }
}

#[cfg(test)]
mod tree_bins {
    use super::*;
    use std::hash::Hasher;

    // Tests for the tree bin optimization.
    // Includes testing that bins are actually treeified and untreeified, and that, when tree bins
    // are untreeified, the associated values remain in the map.

    #[derive(Default)]
    struct ZeroHasher;

    struct ZeroHashBuilder;

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

    #[test]
    #[cfg_attr(miri, ignore)]
    fn concurrent_tree_bin() {
        let map = HashMap::<usize, usize, _>::with_hasher(ZeroHashBuilder);
        // first, ensure that we have a tree bin
        {
            let guard = &map.guard();
            // Force creation of a tree bin by inserting enough values that hash to 0
            for i in 0..10 {
                map.insert(i, i, guard);
            }
            // Ensure the bin was correctly treeified
            let t = map.table.load(Ordering::Relaxed, guard);
            let t = unsafe { t.deref() };
            let bini = t.bini(0);
            let bin = t.bin(bini, guard);
            match unsafe { &**bin.deref() } {
                BinEntry::Tree(_) => {} // pass
                BinEntry::Moved => panic!("bin was not correctly treeified -- is Moved"),
                BinEntry::Node(_) => panic!("bin was not correctly treeified -- is Node"),
                BinEntry::TreeNode(_) => panic!("bin was not correctly treeified -- is TreeNode"),
            }

            let _ = guard;
        }
        // then, spin up lots of reading and writing threads on a range of keys
        const NUM_WRITERS: usize = 5;
        const NUM_READERS: usize = 20;
        const NUM_REPEATS: usize = 1000;
        const NUM_KEYS: usize = 1000;
        use rand::{
            distributions::{Distribution, Uniform},
            thread_rng,
        };
        let uniform = Uniform::new(0, NUM_KEYS);
        let m = std::sync::Arc::new(map);

        let mut handles = Vec::with_capacity(2 * NUM_WRITERS + NUM_READERS);
        for _ in 0..NUM_READERS {
            // ...and a reading thread
            let map = m.clone();
            handles.push(std::thread::spawn(move || {
                let guard = &map.guard();
                let mut trng = thread_rng();
                for _ in 0..NUM_REPEATS {
                    let key = uniform.sample(&mut trng);
                    if let Some(v) = map.get(&key, guard) {
                        criterion::black_box(v);
                    }
                }
            }));
        }
        for i in 0..NUM_WRITERS {
            // NUM_WRITERS times, create a writing thread...
            let map = m.clone();
            handles.push(std::thread::spawn(move || {
                let guard = &map.guard();
                let mut trng = thread_rng();
                for _ in 0..NUM_REPEATS {
                    let key = uniform.sample(&mut trng);
                    map.insert(key, i, guard);
                }
            }));
            // ...a removing thread.
            let map = m.clone();
            handles.push(std::thread::spawn(move || {
                let guard = &map.guard();
                let mut trng = thread_rng();
                for _ in 0..NUM_REPEATS {
                    let key = uniform.sample(&mut trng);
                    if let Some(v) = map.remove(&key, guard) {
                        criterion::black_box(v);
                    }
                }
            }));
        }

        // in the end, join all threads
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn untreeify_shared_values_remove() {
        test_tree_bin_remove(|i, map, guard| {
            assert_eq!(map.remove(&i, guard), Some(&i));
        });
    }

    #[test]
    fn untreeify_shared_values_compute_if_present() {
        test_tree_bin_remove(|i, map, guard| {
            assert_eq!(map.compute_if_present(&i, |_, _| None, guard), None);
        });
    }

    fn test_tree_bin_remove<F>(f: F)
    where
        F: Fn(usize, &HashMap<usize, usize, ZeroHashBuilder>, &Guard<'_>),
    {
        let map = HashMap::<usize, usize, _>::with_hasher(ZeroHashBuilder);
        {
            let guard = &map.guard();
            // Force creation of a tree bin by inserting enough values that hash to 0
            for i in 0..10 {
                map.insert(i, i, guard);
            }
            // Ensure the bin was correctly treeified
            let t = map.table.load(Ordering::Relaxed, guard);
            let t = unsafe { t.deref() };
            let bini = t.bini(0);
            let bin = t.bin(bini, guard);
            match unsafe { &**bin.deref() } {
                BinEntry::Tree(_) => {} // pass
                BinEntry::Moved => panic!("bin was not correctly treeified -- is Moved"),
                BinEntry::Node(_) => panic!("bin was not correctly treeified -- is Node"),
                BinEntry::TreeNode(_) => panic!("bin was not correctly treeified -- is TreeNode"),
            }

            // Delete keys to force untreeifying the bin
            for i in 0..9 {
                f(i, &map, guard);
            }
            let _ = guard;
        }
        assert_eq!(map.len(), 1);

        {
            // Ensure the bin was correctly untreeified
            let guard = &map.guard();
            let t = map.table.load(Ordering::Relaxed, guard);
            let t = unsafe { t.deref() };
            let bini = t.bini(0);
            let bin = t.bin(bini, guard);
            match unsafe { &**bin.deref() } {
                BinEntry::Tree(_) => panic!("bin was not correctly untreeified -- is Tree"),
                BinEntry::Moved => panic!("bin was not correctly untreeified -- is Moved"),
                BinEntry::Node(_) => {} // pass
                BinEntry::TreeNode(_) => panic!("bin was not correctly untreeified -- is TreeNode"),
            }
        }

        // Create some guards to more reliably trigger garbage collection
        for _ in 0..10 {
            let _ = map.guard();
        }

        // Access a value that should still be in the map
        let guard = &map.guard();
        assert_eq!(map.get(&9, guard), Some(&9));
    }

    #[test]
    #[should_panic]
    fn disallow_evil() {
        let map: HashMap<_, _> = HashMap::default();
        map.insert(42, String::from("hello"), &map.guard());

        let evil = seize::Collector::new();
        let guard = evil.enter();
        let oops = map.get(&42, &guard);

        map.remove(&42, &map.guard());
        // at this point, the default collector is allowed to free `"hello"`
        // since no guard from `map`s collector is active.
        // `oops` is tied to the lifetime of a Guard that is not a part of
        // the same collector, and so can now be dangling.
        // but we can still access it!
        assert_eq!(oops.unwrap(), "hello");
    }
}
