//! A concurrent hash table based on Java's `ConcurrentHashMap`.
//!
//! A hash table that supports full concurrency of retrievals and high expected concurrency for
//! updates. This type is functionally very similar to `std::collections::HashMap`, and for the
//! most part has a similar API. Even though all operations on the map are thread-safe and operate
//! on shared references, retrieval operations do *not* entail locking, and there is *not* any
//! support for locking the entire table in a way that prevents all access.
//!
//! # A note on `Guard` and memory use
//!
//! You may have noticed that many of the access methods on this map take a reference to an
//! [`epoch::Guard`]. The exact details of this are beyond the scope of this documentation (for
//! that, see [`crossbeam::epoch`]), but some of the implications bear repeating here. You obtain a
//! `Guard` using [`epoch::pin`], and you can use references to the same guard to make multiple API
//! calls if you wish. Whenever you get a reference to something stored in the map, that reference
//! is tied to the lifetime of the `Guard` that you provided. This is because each `Guard` prevents
//! the destruction of any item associated with it. Whenever something is read under a `Guard`,
//! that something stays around for _at least_ as long as the `Guard` does. The map delays
//! deallocating values until it safe to do so, and in order to amortize the cost of the necessary
//! bookkeeping it may delay even further until there's a _batch_ of items that need to be
//! deallocated.
//!
//! Notice that there is a trade-off here. Creating and dropping a `Guard` is not free, since it
//! also needs to interact with said bookkeeping. But if you keep one around for a long time, you
//! may accumulate much garbage which will take up valuable free memory on your system. Use your
//! best judgement in deciding whether or not to re-use a `Guard`.
//!
//! # Consistency
//!
//! Retrieval operations (including [`get`](FlurryHashMap::get)) generally do not block, so may
//! overlap with update operations (including [`insert`](FlurryHashMap::insert)). Retrievals
//! reflect the results of the most recently *completed* update operations holding upon their
//! onset. (More formally, an update operation for a given key bears a _happens-before_ relation
//! with any successful retrieval for that key reporting the updated value.)
//!
//! Operations that inspect the map as a whole, rather than a single key, operate on a snapshot of
//! the underlying table. For example, iterators return elements reflecting the state of the hash
//! table at some point at or since the creation of the iterator. Aggregate status methods like
//! [`size`](FlurryHashMap::size) are typically useful only when a map is not undergoing concurrent
//! updates in other threads. Otherwise the results of these methods reflect transient states that
//! may be adequate for monitoring or estimation purposes, but not for program control.
//!
//! # Resizing behavior
//!
//! The table is dynamically expanded when there are too many collisions (i.e., keys that have
//! distinct hash codes but fall into the same slot modulo the table size), with the expected
//! average effect of maintaining roughly two bins per mapping (corresponding to a 0.75 load factor
//! threshold for resizing). There may be much variance around this average as mappings are added
//! and removed, but overall, this maintains a commonly accepted time/space tradeoff for hash
//! tables.  However, resizing this or any other kind of hash table may be a relatively slow
//! operation. When possible, it is a good idea to provide a size estimate by using the
//! [`with_capacity`](FlurryHashMap::with_capacity) constructor. Note that using many keys with
//! exactly the same [`Hash`](std::hash::Hash) value is a sure way to slow down performance of any
//! hash table. /* TODO: dynamic load factor */
//!
//! /* TODO: set projection */
//! /* TODO: frequency map through computeIfAbsent */
//! /* TODO: bulk operations like forEach, search, and reduce */
//!
//! # Implementation notes
//!
//! This data-structure is a pretty direct port of Java's `java.util.concurrent.ConcurrentHashMap`
//! [from Doug Lea and the rest of the JSR166
//! team](http://gee.cs.oswego.edu/dl/concurrency-interest/). Huge thanks to them for releasing the
//! code into the public domain! Much of the documentation is also lifted from there. What follows
//! is a slightly modified version of their implementation notes from within the [source
//! file](http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/java/util/concurrent/ConcurrentHashMap.java?view=markup).
//!
//! The primary design goal of this hash table is to maintain concurrent readability (typically
//! method `get()`, but also iterators and related methods) while minimizing update contention.
//! Secondary goals are to keep space consumption about the same or better than java.util.HashMap,
//! and to support high initial insertion rates on an empty table by many threads.
//!
//! This map usually acts as a binned (bucketed) hash table.  Each key-value mapping is held in a
//! `BinEntry.  Most nodes are of type `BinEntry::Node` with hash, key, value, and a `next` field.
//!  However, some nodes are of type `BinEntry::Moved`; these "forwarding nodes" are placed at the
//!  heads of bins during resizing. The Java version also has other special node types, but these
//!  have not yet been implemented in this port. These special nodes are all either uncommon or
//!  transient.
//! /* TODO: TreeNodes, ReservationNodes */
//!
//! The table is lazily initialized to a power-of-two size upon the first insertion.  Each bin in
//! the table normally contains a list of nodes (most often, the list has only zero or one
//! `BinEntry`). Table accesses require atomic reads, writes, and CASes.
//!
//! Insertion (via `put`) of the first node in an empty bin is performed by just CASing it to the
//! bin.  This is by far the most common case for put operations under most key/hash distributions.
//! Other update operations (insert, delete, and replace) require locks.  We do not want to waste
//! the space required to associate a distinct lock object with each bin, so we instead embed a
//! lock inside each node, and use the lock in the the first node of a bin list as the lock for the
//! bin.
//!
//! Using the first node of a list as a lock does not by itself suffice though: When a node is
//! locked, any update must first validate that it is still the first node after locking it, and
//! retry if not. Because new nodes are always appended to lists, once a node is first in a bin, it
//! remains first until deleted or the bin becomes invalidated (upon resizing).
//!
//! The main disadvantage of per-bin locks is that other update operations on other nodes in a bin
//! list protected by the same lock can stall, for example when user `Eq` implementations or
//! mapping functions take a long time.  However, statistically, under random hash codes, this is
//! not a common problem.  Ideally, the frequency of nodes in bins follows a Poisson distribution
//! (http://en.wikipedia.org/wiki/Poisson_distribution) with a parameter of about 0.5 on average,
//! given the resizing threshold of 0.75, although with a large variance because of resizing
//! granularity. Ignoring variance, the expected occurrences of list size `k` are `exp(-0.5) *
//! pow(0.5, k) / factorial(k)`. The first values are:
//!
//! ```text
//! 0:    0.60653066
//! 1:    0.30326533
//! 2:    0.07581633
//! 3:    0.01263606
//! 4:    0.00157952
//! 5:    0.00015795
//! 6:    0.00001316
//! 7:    0.00000094
//! 8:    0.00000006
//! more: less than 1 in ten million
//! ```
//!
//! Lock contention probability for two threads accessing distinct elements is roughly `1 / (8 *
//! #elements)` under random hashes.
//!
//! Actual hash code distributions encountered in practice sometimes deviate significantly from
//! uniform randomness.  This includes the case when `N > (1<<30)`, so some keys MUST collide.
//! Similarly for dumb or hostile usages in which multiple keys are designed to have identical hash
//! codes or ones that differs only in masked-out high bits. Here, the Java implementation uses an
//! optimization where a bin is turned into a binary tree, but this has not yet been ported over to
//! the Rust version. /* TODO */
//!
//! The table is resized when occupancy exceeds a percentage threshold (nominally, 0.75, but see
//! below).  Any thread noticing an overfull bin may assist in resizing after the initiating thread
//! allocates and sets up the replacement array. However, rather than stalling, these other threads
//! may proceed with insertions etc. Resizing proceeds by transferring bins, one by one, from the
//! table to the next table. However, threads claim small blocks of indices to transfer (via the
//! field `transfer_index`) before doing so, reducing contention.  A generation stamp in the field
//! `size_ctl` ensures that resizings do not overlap. Because we are using power-of-two expansion,
//! the elements from each bin must either stay at same index, or move with a power of two offset.
//! We eliminate unnecessary node creation by catching cases where old nodes can be reused because
//! their next fields won't change.  On average, only about one-sixth of them need cloning when a
//! table doubles. The nodes they replace will be garbage collectible as soon as they are no longer
//! referenced by any reader thread that may be in the midst of concurrently traversing table.
//! Upon transfer, the old table bin contains only a special forwarding node (`BinEntry::Moved`)
//! that contains the next table as its key. On encountering a forwarding node, access and update
//! operations restart, using the new table.
//! /* TODO: note on TreeBins */
//!
//! Each bin transfer requires its bin lock, which can stall waiting for locks while resizing.
//! However, because other threads can join in and help resize rather than contend for locks,
//! average aggregate waits become shorter as resizing progresses.  The transfer operation must
//! also ensure that all accessible bins in both the old and new table are usable by any traversal.
//! This is arranged in part by proceeding from the last bin `table.length - 1` up towards the
//! first.  Upon seeing a forwarding node, traversals (see `iter::traverser::Traverser`) arrange to
//! move to the new table without revisiting nodes.  To ensure that no intervening nodes are
//! skipped even when moved out of order, a stack (see class `iter::traverser::TableStack`) is
//! created on first encounter of a forwarding node during a traversal, to maintain its place if
//! later processing the current table. The need for these save/restore mechanics is relatively
//! rare, but when one forwarding node is encountered, typically many more will be. So `Traversers`
//! use a simple caching scheme to avoid creating so many new `TableStack` nodes. (Thanks to Peter
//! Levart for suggesting use of a stack here.)
//!
//! /*
//! TODO:
//! Lazy table initialization minimizes footprint until first use, and also avoids resizings when
//! the first operation is from a `from_iter`, `From::from`, or deserialization. These cases
//! attempt to override the initial capacity settings, but harmlessly fail to take effect in cases
//! of races.
//! */
//!
//! /*
//! TODO
//! The element count is maintained using a specialization of LongAdder. We need to incorporate a
//! specialization rather than just use a LongAdder in order to access implicit contention-sensing
//! that leads to creation of multiple CounterCells.  The counter mechanics avoid contention on
//! updates but can encounter cache thrashing if read too frequently during concurrent access. To
//! avoid reading so often, resizing under contention is attempted only upon adding to a bin
//! already holding two or more nodes. Under uniform hash distributions, the probability of this
//! occurring at threshold is around 13%, meaning that only about 1 in 8 puts check threshold (and
//! after resizing, many fewer do so).
//! */
//!
//! /* TODO: TreeBins comparisons and locking */
//!
//! ## Garbage collection
//!
//! The Java implementation can rely on Java's runtime garbage collection to safely deallocate
//! deleted or removed nodes, keys, and values. Since Rust does not have such a runtime, we must
//! ensure through some other mechanism that we do not drop values before all references to them
//! have goen away. We do this using [`crossbeam::epoch`], which provides an implementation of an
//! epoch-based garbae collection scheme. This forces us to make certain API changes such as
//! requiring `Guard` arguments to many methods or wrapping the return values, but provides much
//! more efficient operation than if everything had to be atomically reference-counted.
//!
//!  [`crossbeam::epoch`]: https://docs.rs/crossbeam/0.7/crossbeam/epoch/index.html
#![deny(missing_docs, missing_debug_implementations)]
#![warn(rust_2018_idioms)]

mod node;
use node::*;

use crossbeam::epoch::{Atomic, Guard, Owned, Shared};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::{atomic::{AtomicIsize, AtomicUsize, Ordering}, Once};
use std::iter::FromIterator;

/// The largest possible table capacity.  This value must be
/// exactly 1<<30 to stay within Java array allocation and indexing
/// bounds for power of two table sizes, and is further required
/// because the top two bits of 32bit hash fields are used for
/// control purposes.
const MAXIMUM_CAPACITY: usize = 1 << 30;

/// The default initial table capacity.  Must be a power of 2
/// (i.e., at least 1) and at most `MAXIMUM_CAPACITY`.
const DEFAULT_CAPACITY: usize = 16;

/// The load factor for this table. Overrides of this value in
/// constructors affect only the initial table capacity.  The
/// actual floating point value isn't normally used -- it is
/// simpler to use expressions such as `n - (n >>> 2)` for
/// the associated resizing threshold.
const LOAD_FACTOR: f64 = 0.75;

/// Minimum number of rebinnings per transfer step. Ranges are
/// subdivided to allow multiple resizer threads.  This value
/// serves as a lower bound to avoid resizers encountering
/// excessive memory contention.  The value should be at least
/// `DEFAULT_CAPACITY`.
const MIN_TRANSFER_STRIDE: isize = 16;

/// The number of bits used for generation stamp in `size_ctl`.
/// Must be at least 6 for 32bit arrays.
const RESIZE_STAMP_BITS: usize = 16;

/// The maximum number of threads that can help resize.
/// Must fit in `32 - RESIZE_STAMP_BITS` bits.
const MAX_RESIZERS: isize = (1 << (32 - RESIZE_STAMP_BITS)) - 1;

/// The bit shift for recording size stamp in `size_ctl`.
const RESIZE_STAMP_SHIFT: usize = 32 - RESIZE_STAMP_BITS;

static NCPU_INITIALIZER: Once = Once::new();
static mut NCPU: usize = 0;

/// Iterator types.
pub mod iter;
use iter::*;

/// Types needed to safely access shared data concurrently.
pub mod epoch {
    pub use crossbeam::epoch::{pin, Guard};
}

/// A concurrent hash table.
///
/// See the [crate-level documentation](index.html) for details.
#[derive(Debug)]
pub struct FlurryHashMap<K, V, S = RandomState> {
    /// The array of bins. Lazily initialized upon first insertion.
    /// Size is always a power of two. Accessed directly by iterators.
    table: Atomic<Table<K, V>>,

    /// The next table to use; non-null only while resizing.
    next_table: Atomic<Table<K, V>>,

    /// The next table index (plus one) to split while resizing.
    transfer_index: AtomicIsize,

    count: AtomicUsize,

    /// Table initialization and resizing control.  When negative, the
    /// table is being initialized or resized: -1 for initialization,
    /// else -(1 + the number of active resizing threads).  Otherwise,
    /// when table is null, holds the initial table size to use upon
    /// creation, or 0 for default. After initialization, holds the
    /// next element count value upon which to resize the table.
    size_ctl: AtomicIsize,

    build_hasher: S,
}

impl<K, V> Default for FlurryHashMap<K, V, RandomState>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> FlurryHashMap<K, V, RandomState>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
{
    /// Creates a new, empty map with the default initial table size (16).
    pub fn new() -> Self {
        Self {
            table: Atomic::null(),
            next_table: Atomic::null(),
            transfer_index: AtomicIsize::new(0),
            count: AtomicUsize::new(0),
            size_ctl: AtomicIsize::new(0),
            build_hasher: RandomState::new(),
        }
    }

    /// Creates a new, empty map with an initial table size accommodating the specified number of
    /// elements without the need to dynamically resize.
    ///
    /// # Panics
    ///
    /// If the given capacity is 0.
    pub fn with_capacity(n: usize) -> Self {
        assert_ne!(n, 0);
        let mut m = Self::new();
        let size = (1.0 + (n as f64) / LOAD_FACTOR) as usize;
        // NOTE: tableSizeFor in Java
        let cap = std::cmp::min(MAXIMUM_CAPACITY, size.next_power_of_two());
        m.size_ctl = AtomicIsize::new(cap as isize);
        m
    }
}

impl<K, V, S> FlurryHashMap<K, V, S>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
    S: BuildHasher,
{
    fn hash(&self, key: &K) -> u64 {
        use std::hash::Hasher;
        let mut h = self.build_hasher.build_hasher();
        key.hash(&mut h);
        h.finish()
    }

    /// Tests if `key` is a key in this table.
    pub fn contains_key(&self, key: &K) -> bool {
        let guard = crossbeam::epoch::pin();
        self.get(key, &guard).is_some()
    }

    /// Returns the value to which `key` is mapped.
    ///
    /// Returns `None` if this map contains no mapping for the key.
    ///
    /// To obtain a `Guard`, use [`epoch::pin`].
    // TODO: implement a guard API of our own
    pub fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let h = self.hash(key);
        let table = self.table.load(Ordering::SeqCst, guard);
        if table.is_null() {
            return None;
        }

        // safety: we loaded the table while epoch was pinned. table won't be deallocated until
        // next epoch at the earliest.
        let table = unsafe { table.deref() };
        if table.bins.len() == 0 {
            return None;
        }
        let bini = table.bini(h);
        let bin = table.bin(bini, guard);
        if bin.is_null() {
            return None;
        }

        // safety: bin is a valid pointer.
        //
        // there are two cases when a bin pointer is invalidated:
        //
        //  1. if the table was resized, bin is a move entry, and the resize has completed. in
        //     that case, the table (and all its heads) will be dropped in the next epoch
        //     following that.
        //  2. if the table is being resized, bin may be swapped with a move entry. the old bin
        //     will then be dropped in the following epoch after that happens.
        //
        // in both cases, we held the guard when we got the reference to the bin. if any such
        // swap happened, it must have happened _after_ we read. since we did the read while
        // pinning the epoch, the drop must happen in the _next_ epoch (i.e., the one that we
        // are holding up by holding on to our guard).
        let node = unsafe { bin.deref() }.find(h, key, guard);
        if node.is_null() {
            return None;
        }
        // safety: we read the bin while pinning the epoch. a bin will never be dropped until the
        // next epoch after it is removed. since it wasn't removed, and the epoch was pinned, that
        // cannot be until after we drop our guard.
        let node = unsafe { node.deref() };
        let node = node.as_node().unwrap();

        let v = node.value.load(Ordering::SeqCst, guard);
        assert!(!v.is_null());
        // safety: the lifetime of the reference is bound to the guard
        // supplied which means that the memory will not be modified
        // until at least after the guard goes out of scope
        unsafe { v.as_ref() }
    }

    /// Obtains the value to which `key` is mapped and passes it through the closure `then`.
    ///
    /// Returns `None` if this map contains no mapping for `key`.
    pub fn get_and<R, F: FnOnce(&V) -> R>(&self, key: &K, then: F) -> Option<R> {
        let guard = &crossbeam::epoch::pin();
        self.get(key, guard).map(then)
    }

    fn init_table<'g>(&self, guard: &'g Guard) -> Shared<'g, Table<K, V>> {
        loop {
            let table = self.table.load(Ordering::SeqCst, guard);
            // safety: we loaded the table while epoch was pinned. table won't be deallocated until
            // next epoch at the earliest.
            if !table.is_null() && !unsafe { table.deref() }.bins.is_empty() {
                break table;
            }
            // try to allocate the table
            let mut sc = self.size_ctl.load(Ordering::SeqCst);
            if sc < 0 {
                // we lost the initialization race; just spin
                std::thread::yield_now();
                continue;
            }

            if self.size_ctl.compare_and_swap(sc, -1, Ordering::SeqCst) == sc {
                // we get to do it!
                let mut table = self.table.load(Ordering::SeqCst, guard);

                // safety: we loaded the table while epoch was pinned. table won't be deallocated
                // until next epoch at the earliest.
                if table.is_null() || unsafe { table.deref() }.bins.is_empty() {
                    let n = if sc > 0 {
                        sc as usize
                    } else {
                        DEFAULT_CAPACITY
                    };
                    let new_table = Owned::new(Table {
                        bins: vec![Atomic::null(); n].into_boxed_slice(),
                    });
                    table = new_table.into_shared(guard);
                    self.table.store(table, Ordering::SeqCst);
                    sc = n as isize - (n >> 2) as isize;
                }
                self.size_ctl.store(sc, Ordering::SeqCst);
                break table;
            }
        }
    }

    /// Maps `key` to `value` in this table.
    ///
    /// The value can be retrieved by calling [`get`] with a key that is equal to the original key.
    pub fn insert<'g>(&self, key: K, value: V, guard: &'g Guard) -> Option<&'g V> {
        self.put(key, value, false, guard)
    }

    fn put<'g>(&self, key: K, value: V, no_replacement: bool, guard: &'g Guard) -> Option<&'g V> {
        let h = self.hash(&key);

        let mut table = self.table.load(Ordering::SeqCst, guard);

        let mut node = Owned::new(BinEntry::Node(Node {
            key,
            value: Atomic::new(value),
            hash: h,
            next: Atomic::null(),
            lock: parking_lot::Mutex::new(()),
        }));

        loop {
            // safety: see argument below for !is_null case
            if table.is_null() || unsafe { table.deref() }.bins.len() == 0 {
                table = self.init_table(guard);
                continue;
            }

            // safety: table is a valid pointer.
            //
            // we are in one of three cases:
            //
            //  1. if table is the one we read before the loop, then we read it while holding the
            //     guard, so it won't be dropped until after we drop that guard b/c the drop logic
            //     only queues a drop for the next epoch after removing the table.
            //
            //  2. if table is read by init_table, then either we did a load, and the argument is
            //     as for point 1. or, we allocated a table, in which case the earliest it can be
            //     deallocated is in the next epoch. we are holding up the epoch by holding the
            //     guard, so this deref is safe.
            //
            //  3. if table is set by a Moved node (below) through help_transfer, it will _either_
            //     keep using `table` (which is fine by 1. and 2.), or use the `next_table` raw
            //     pointer from inside the Moved. how do we know that that is safe?
            //
            //     we must demonstrate that if a Moved(t) is _read_, then t must still be valid.
            //     FIXME
            let t = unsafe { table.deref() };

            let bini = t.bini(h);
            let mut bin = t.bin(bini, guard);
            if bin.is_null() {
                // fast path -- bin is empty so stick us at the front
                match t.cas_bin(bini, bin, node, guard) {
                    Ok(_old_null_ptr) => {
                        self.add_count(1, Some(0), guard);
                        guard.flush();
                        return None;
                    }
                    Err(changed) => {
                        assert!(!changed.current.is_null());
                        node = changed.new;
                        bin = changed.current;
                    }
                }
            }

            // slow path -- bin is non-empty
            // safety: bin is a valid pointer.
            //
            // there are two cases when a bin pointer is invalidated:
            //
            //  1. if the table was resized, bin is a move entry, and the resize has completed. in
            //     that case, the table (and all its heads) will be dropped in the next epoch
            //     following that.
            //  2. if the table is being resized, bin may be swapped with a move entry. the old bin
            //     will then be dropped in the following epoch after that happens.
            //
            // in both cases, we held the guard when we got the reference to the bin. if any such
            // swap happened, it must have happened _after_ we read. since we did the read while
            // pinning the epoch, the drop must happen in the _next_ epoch (i.e., the one that we
            // are holding up by holding on to our guard).
            let key = &node.as_node().unwrap().key;
            match *unsafe { bin.deref() } {
                BinEntry::Moved(next_table) => {
                    table = self.help_transfer(table, next_table, guard);
                }
                BinEntry::Node(ref head)
                    if no_replacement && head.hash == h && &head.key == key =>
                {
                    // fast path if replacement is disallowed and first bin matches
                    let v = head.value.load(Ordering::SeqCst, guard);
                    // safety: since the value is present now, and we've held a guard from the
                    // beginning of the search, the value cannot be dropped until the next epoch,
                    // which won't arrive until after we drop our guard.
                    return Some(unsafe { v.deref() });
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

                    // TODO: TreeBin & ReservationNode

                    let mut bin_count = 1;
                    let mut p = bin;

                    let old_val = loop {
                        // safety: we read the bin while pinning the epoch. a bin will never be
                        // dropped until the next epoch after it is removed. since it wasn't
                        // removed, and the epoch was pinned, that cannot be until after we drop
                        // our guard.
                        let n = unsafe { p.deref() }.as_node().unwrap();
                        if n.hash == h && &n.key == key {
                            // the key already exists in the map!
                            let current_value = head.value.load(Ordering::SeqCst, guard);

                            // safety: since the value is present now, and we've held a guard from
                            // the beginning of the search, the value cannot be dropped until the
                            // next epoch, which won't arrive until after we drop our guard.
                            let current_value = unsafe { current_value.deref() };

                            if no_replacement {
                                // the key is not absent, so don't update
                            } else if let BinEntry::Node(Node { value, .. }) = *node.into_box() {
                                // safety: we own value and have never shared it
                                let now_garbage = n.value.swap(
                                    unsafe { value.into_owned() },
                                    Ordering::SeqCst,
                                    guard,
                                );
                                // NOTE: now_garbage == current_value

                                // safety: need to guarantee that now_garbage is no longer
                                // reachable. more specifically, no thread that executes _after_
                                // this line can ever get a reference to now_garbage.
                                //
                                // here are the possible cases:
                                //
                                //  - another thread already has a reference to now_garbage.
                                //    they must have read it before the call to swap.
                                //    because of this, that thread must be pinned to an epoch <=
                                //    the epoch of our guard. since the garbage is placed in our
                                //    epoch, it won't be freed until the _next_ epoch, at which
                                //    point, that thread must have dropped its guard, and with it,
                                //    any reference to the value.
                                //  - another thread is about to get a reference to this value.
                                //    they execute _after_ the swap, and therefore do _not_ get a
                                //    reference to now_garbage (they get value instead). there are
                                //    no other ways to get to a value except through its Node's
                                //    `value` field (which is what we swapped), so freeing
                                //    now_garbage is fine.
                                unsafe { guard.defer_destroy(now_garbage) };
                            } else {
                                unreachable!();
                            }
                            break Some(current_value);
                        }

                        // TODO: This Ordering can probably be relaxed due to the Mutex
                        let next = n.next.load(Ordering::SeqCst, guard);
                        if next.is_null() {
                            // we're at the end of the bin -- stick the node here!
                            n.next.store(node, Ordering::SeqCst);
                            break None;
                        }
                        p = next;

                        bin_count += 1;
                    };
                    drop(head_lock);

                    // TODO: TREEIFY_THRESHOLD

                    if old_val.is_none() {
                        // increment count
                        self.add_count(1, Some(bin_count), guard);
                    }
                    guard.flush();
                    return old_val;
                }
            }
        }
    }

    fn help_transfer<'g>(
        &self,
        table: Shared<'g, Table<K, V>>,
        next_table: *const Table<K, V>,
        guard: &'g Guard,
    ) -> Shared<'g, Table<K, V>> {
        if table.is_null() || next_table.is_null() {
            return table;
        }

        let next_table = Shared::from(next_table);

        // safety: table is only dropped on the next epoch change after it is swapped to null.
        // we read it as not null, so it must not be dropped until a subsequent epoch. since we
        // held `guard` at the time, we know that the current epoch continues to persist, and that
        // our reference is therefore valid.
        let rs = Self::resize_stamp(unsafe { table.deref() }.bins.len()) << RESIZE_STAMP_SHIFT;

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

            if self.size_ctl.compare_and_swap(sc, sc + 1, Ordering::SeqCst) == sc {
                self.transfer(table, next_table, guard);
                break;
            }
        }
        next_table
    }

    fn add_count(&self, n: isize, resize_hint: Option<usize>, guard: &Guard) {
        // TODO: implement the Java CounterCell business here

        use std::cmp;
        let mut count = match n.cmp(&0) {
            cmp::Ordering::Greater => {
                let n = n as usize;
                self.count.fetch_add(n, Ordering::SeqCst) + n
            }
            cmp::Ordering::Less => {
                let n = n.abs() as usize;
                self.count.fetch_sub(n, Ordering::SeqCst) - n
            }
            cmp::Ordering::Equal => self.count.load(Ordering::SeqCst),
        };

        // if resize_hint is None, it means the caller does not want us to consider a resize.
        // if it is Some(n), the caller saw n entries in a bin
        if resize_hint.is_none() {
            return;
        }

        // TODO: use the resize hint
        let _saw_bin_length = resize_hint.unwrap();

        loop {
            let sc = self.size_ctl.load(Ordering::SeqCst);
            if (count as isize) < sc {
                // we're not at the next resize point yet
                break;
            }

            let table = self.table.load(Ordering::SeqCst, guard);
            if table.is_null() {
                // table will be initalized by another thread anyway
                break;
            }

            // safety: table is only dropped on the next epoch change after it is swapped to null.
            // we read it as not null, so it must not be dropped until a subsequent epoch. since we
            // hold a Guard, we know that the current epoch will persist, and that our reference
            // will therefore remain valid.
            let n = unsafe { table.deref() }.bins.len();
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
                if self.size_ctl.compare_and_swap(sc, sc + 1, Ordering::SeqCst) == sc {
                    self.transfer(table, nt, guard);
                }
            } else if self.size_ctl.compare_and_swap(sc, rs + 2, Ordering::SeqCst) == sc {
                // a resize is needed, but has not yet started
                // TODO: figure out why this is rs + 2, not just rs
                self.transfer(table, Shared::null(), guard);
            }

            // another resize may be needed!
            count = self.count.load(Ordering::SeqCst);
        }
    }

    fn transfer<'g>(
        &self,
        table: Shared<'g, Table<K, V>>,
        mut next_table: Shared<'g, Table<K, V>>,
        guard: &'g Guard,
    ) {
        // safety: table was read while `guard` was held. the code that drops table only drops it
        // after it is no longer reachable, and any outstanding references are no longer active.
        // this references is still active (marked by the guard), so the target of the references
        // won't be dropped while the guard remains active.
        let n = unsafe { table.deref() }.bins.len();
        let ncpu = num_cpus();
        
        let stride = if ncpu > 1 { (n >> 3) / ncpu } else { n }; 
        let stride = std::cmp::max(stride as isize, MIN_TRANSFER_STRIDE);

        if next_table.is_null() {
            // we are initiating a resize
            let table = Owned::new(Table {
                bins: vec![Atomic::null(); n << 1].into_boxed_slice(),
            });

            let now_garbage = self.next_table.swap(table, Ordering::SeqCst, guard);
            assert!(now_garbage.is_null());
            self.transfer_index.store(n as isize, Ordering::SeqCst);
            next_table = self.next_table.load(Ordering::Relaxed, guard);
        }

        // safety: same argument as for table above
        let next_n = unsafe { next_table.deref() }.bins.len();

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
                    .compare_and_swap(next_index, next_bound, Ordering::SeqCst)
                    == next_index
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
                    let now_garbage = self.table.swap(next_table, Ordering::SeqCst, guard);
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
                    // this means that no _future_ thread (i.e., in a later epoch where the value
                    // may be freed) can get a reference to now_garbage.
                    //
                    // next, let's talk about threads with _existing_ references to now_garbage.
                    // such a thread must have gotten that reference before the call to swap.
                    // because of this, that thread must be pinned to an epoch <= the epoch of our
                    // guard (since our guard is pinning the epoch). since the garbage is placed in
                    // our epoch, it won't be freed until the _next_ epoch, at which point, that
                    // thread must have dropped its guard, and with it, any reference to the value.
                    unsafe { guard.defer_destroy(now_garbage) };
                    self.size_ctl
                        .store(((n as isize) << 1) - ((n as isize) >> 1), Ordering::SeqCst);
                    return;
                }

                let sc = self.size_ctl.load(Ordering::SeqCst);
                if self.size_ctl.compare_and_swap(sc, sc - 1, Ordering::SeqCst) == sc {
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
            let next_table = unsafe { next_table.deref() };

            let bin = table.bin(i as usize, guard);
            if bin.is_null() {
                advance = table
                    .cas_bin(
                        i,
                        Shared::null(),
                        Owned::new(BinEntry::Moved(next_table as *const _)),
                        guard,
                    )
                    .is_ok();
                continue;
            }

            // safety: bin is a valid pointer.
            //
            // there are two cases when a bin pointer is invalidated:
            //
            //  1. if the table was resized, bin is a move entry, and the resize has completed. in
            //     that case, the table (and all its heads) will be dropped in the next epoch
            //     following that.
            //  2. if the table is being resized, bin may be swapped with a move entry. the old bin
            //     will then be dropped in the following epoch after that happens.
            //
            // in both cases, we held the guard when we got the reference to the bin. if any such
            // swap happened, it must have happened _after_ we read. since we did the read while
            // pinning the epoch, the drop must happen in the _next_ epoch (i.e., the one that we
            // are holding up by holding on to our guard).
            match *unsafe { bin.deref() } {
                BinEntry::Moved(_) => {
                    // already processed
                    advance = true;
                }
                BinEntry::Node(ref head) => {
                    // bin is non-empty, need to link into it, so we must take the lock
                    let head_lock = head.lock.lock();

                    // need to check that this is _still_ the head
                    let current_head = table.bin(i, guard);
                    if current_head.as_raw() != bin.as_raw() {
                        // nope -- try again from the start
                        continue;
                    }

                    // yes, it is still the head, so we can now "own" the bin
                    // note that there can still be readers in the bin!

                    // TODO: TreeBin & ReservationNode

                    let mut run_bit = head.hash & n as u64;
                    let mut last_run = bin;
                    let mut p = bin;
                    loop {
                        // safety: p is a valid pointer.
                        //
                        // p is only dropped in the next epoch following when its bin is replaced
                        // with a move node (see safety comment near table.store_bin below). we
                        // read the bin, and got to p, so its bin has not yet been swapped with a
                        // move node. and, we have the epoch pinned, so the next epoch cannot have
                        // arrived yet. therefore, it will be dropped in a future epoch, and is
                        // safe to use now.
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
                        // p is only dropped in the next epoch following when its bin is replaced
                        // with a move node (see safety comment near table.store_bin below). we
                        // read the bin, and got to p, so its bin has not yet been swapped with a
                        // move node. and, we have the epoch pinned, so the next epoch cannot have
                        // arrived yet. therefore, it will be dropped in a future epoch, and is
                        // safe to use now.
                        let node = unsafe { p.deref() }.as_node().unwrap();

                        let link = if node.hash & n as u64 == 0 {
                            // to the low bin!
                            &mut low_bin
                        } else {
                            // to the high bin!
                            &mut high_bin
                        };

                        *link = Owned::new(BinEntry::Node(Node {
                            hash: node.hash,
                            key: node.key.clone(),
                            lock: parking_lot::Mutex::new(()),
                            value: node.value.clone(),
                            next: Atomic::from(*link),
                        }))
                        .into_shared(guard);

                        p = node.next.load(Ordering::SeqCst, guard);
                    }

                    next_table.store_bin(i, low_bin);
                    next_table.store_bin(i + n, high_bin);
                    table.store_bin(i, Owned::new(BinEntry::Moved(next_table as *const _)));

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
                        // at that time we had the epoch pinned, so any threads that have such a
                        // reference must be before or at our epoch. since the p isn't destroyed
                        // until the next epoch, those old references are fine since they are tied
                        // to those old threads' pins of the old epoch.
                        let next = unsafe { p.deref() }
                            .as_node()
                            .unwrap()
                            .next
                            .load(Ordering::SeqCst, guard);
                        unsafe { guard.defer_destroy(p) };
                        p = next;
                    }

                    advance = true;

                    drop(head_lock);
                }
            }
        }
    }

    /// Returns the stamp bits for resizing a table of size n.
    /// Must be negative when shifted left by RESIZE_STAMP_SHIFT.
    fn resize_stamp(n: usize) -> isize {
        n.leading_zeros() as isize | (1 << (RESIZE_STAMP_BITS - 1)) as isize
    }

    /// An iterator visiting all key-value pairs in arbitrary order.
    /// The iterator element type is `(&'g K, &'g V)`.
    ///
    /// To obtain a `Guard`, use [`epoch::pin`].
    pub fn iter<'g>(&self, guard: &'g Guard) -> Iter<'g, K, V> {
        let table = self.table.load(Ordering::SeqCst, guard);
        let node_iter = NodeIter::new(table, guard);
        Iter { node_iter, guard }
    }

    /// An iterator visiting all keys in arbitrary order.
    /// The iterator element type is `&'g K`.
    ///
    /// To obtain a `Guard`, use [`epoch::pin`].
    pub fn keys<'g>(&self, guard: &'g Guard) -> Keys<'g, K, V> {
        let table = self.table.load(Ordering::SeqCst, guard);
        let node_iter = NodeIter::new(table, guard);
        Keys { node_iter }
    }

    /// An iterator visiting all values in arbitrary order.
    /// The iterator element type is `&'g V`.
    ///
    /// To obtain a `Guard`, use [`epoch::pin`].
    pub fn values<'g>(&self, guard: &'g Guard) -> Values<'g, K, V> {
        let table = self.table.load(Ordering::SeqCst, guard);
        let node_iter = NodeIter::new(table, guard);
        Values { node_iter, guard }
    }
}

impl<K, V, S> Drop for FlurryHashMap<K, V, S> {
    fn drop(&mut self) {
        // safety: we have &mut self, so not concurrently accessed by anyone else
        let guard = unsafe { crossbeam::epoch::unprotected() };

        assert!(self.next_table.load(Ordering::SeqCst, guard).is_null());
        let table = self.table.swap(Shared::null(), Ordering::SeqCst, guard);
        if table.is_null() {
            // table was never allocated!
            return;
        }

        // safety: same as above + we own the table
        let mut table = unsafe { table.into_owned() }.into_box();
        table.drop_bins();
    }
}

impl <K, V, S> Extend<(K, V)> for &FlurryHashMap<K, V, S>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
    S: BuildHasher,
{
    // TODO: Implement Java's `tryPresize` method to pre-allocate space for
    // the incoming entries
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        let guard = crossbeam::epoch::pin();

        for (key, value) in iter {
            (*self).put(key, value, false, &guard);
        }
    }
}

impl<K, V> FromIterator<(K, V)> for FlurryHashMap<K, V, RandomState> 
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        
        let output = Self::with_capacity(lower);
        (&mut &output).extend(it);

        output
    }
}

#[derive(Debug)]
struct Table<K, V> {
    bins: Box<[Atomic<BinEntry<K, V>>]>,
}

impl<K, V> Table<K, V> {
    fn drop_bins(&mut self) {
        // safety: we have &mut self, so not concurrently accessed by anyone else
        let guard = unsafe { crossbeam::epoch::unprotected() };

        for bin in Vec::from(std::mem::replace(&mut self.bins, vec![].into_boxed_slice())) {
            if bin.load(Ordering::SeqCst, guard).is_null() {
                // bin was never used
                continue;
            }

            // safety: same as above + we own the bin
            let bin = unsafe { bin.into_owned() };
            match *bin {
                BinEntry::Moved(_) => {}
                BinEntry::Node(_) => {
                    let mut p = bin;
                    loop {
                        // safety below:
                        // we're dropping the entire map, so no-one else is accessing it.
                        // we replaced the bin with a NULL, so there's no future way to access it
                        // either; we own all the nodes in the list.

                        let node = if let BinEntry::Node(node) = *p.into_box() {
                            node
                        } else {
                            unreachable!();
                        };

                        // first, drop the value in this node
                        let _ = unsafe { node.value.into_owned() };

                        // then we move to the next node
                        if node.next.load(Ordering::SeqCst, guard).is_null() {
                            break;
                        }
                        p = unsafe { node.next.into_owned() };
                    }
                }
            }
        }
    }
}

impl<K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        // we need to drop any forwarding nodes (since they are heap allocated).

        // safety below:
        // no-one else is accessing this Table any more, so we own all its contents, which is all
        // we are going to use this guard for.
        let guard = unsafe { crossbeam::epoch::unprotected() };

        for bin in &self.bins[..] {
            let bin = bin.swap(Shared::null(), Ordering::SeqCst, guard);
            if bin.is_null() {
                continue;
            }

            // safety: we have mut access to self, so no-one else will drop this value under us.
            let bin = unsafe { bin.into_owned() };
            if let BinEntry::Moved(_) = *bin {
            } else {
                unreachable!("dropped table with non-empty bin");
            }
        }
    }
}

impl<K, V> Table<K, V> {
    #[inline]
    fn bini(&self, hash: u64) -> usize {
        let mask = self.bins.len() as u64 - 1;
        (hash & mask) as usize
    }

    #[inline]
    fn bin<'g>(&'g self, i: usize, guard: &'g Guard) -> Shared<'g, BinEntry<K, V>> {
        self.bins[i].load(Ordering::Acquire, guard)
    }

    #[inline]
    #[allow(clippy::type_complexity)]
    fn cas_bin<'g>(
        &self,
        i: usize,
        current: Shared<'_, BinEntry<K, V>>,
        new: Owned<BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> Result<
        Shared<'g, BinEntry<K, V>>,
        crossbeam::epoch::CompareAndSetError<'g, BinEntry<K, V>, Owned<BinEntry<K, V>>>,
    > {
        self.bins[i].compare_and_set(current, new, Ordering::AcqRel, guard)
    }

    #[inline]
    fn store_bin<P: crossbeam::epoch::Pointer<BinEntry<K, V>>>(&self, i: usize, new: P) {
        self.bins[i].store(new, Ordering::Release)
    }
}

#[inline]
/// Returns the number of physical CPUs in the machine (_O(1)_).
fn num_cpus() -> usize {
    NCPU_INITIALIZER.call_once(|| unsafe {NCPU = num_cpus::get_physical()});
    
    unsafe { NCPU }
}

#[cfg(test)]
mod tests {}

