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
//! best judgement in deciding whether or not to re-use a `Guard`. This is also the reason why the
//! map requires that `K: 'static` and `V: 'static`. If we did not, then your keys and values may
//! get dropped far later, potentially after those lifetimes have passed, which would not be sound.
//!
//! # Consistency
//!
//! Retrieval operations (including [`get`](HashMap::get)) generally do not block, so may
//! overlap with update operations (including [`insert`](HashMap::insert)). Retrievals
//! reflect the results of the most recently *completed* update operations holding upon their
//! onset. (More formally, an update operation for a given key bears a _happens-before_ relation
//! with any successful retrieval for that key reporting the updated value.)
//!
//! Operations that inspect the map as a whole, rather than a single key, operate on a snapshot of
//! the underlying table. For example, iterators return elements reflecting the state of the hash
//! table at some point at or since the creation of the iterator. Aggregate status methods like
//! [`len`](HashMap::len) are typically useful only when a map is not undergoing concurrent
//! updates in other threads. Otherwise the results of these methods reflect transient states that
//! may be adequate for monitoring or estimation purposes, but not for program control.
//! Similarly, [`Clone`](std::clone::Clone) may not produce a "perfect" clone if the underlying
//! map is being concurrently modified.
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
//! [`with_capacity`](HashMap::with_capacity) constructor. Note that using many keys with
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
#![deny(
    missing_docs,
    missing_debug_implementations,
    unreachable_pub,
    intra_doc_link_resolution_failure
)]
#![warn(rust_2018_idioms)]

mod map;
mod node;
mod raw;

/// Iterator types.
pub mod iter;

pub use map::HashMap;

/// Default hasher for [`HashMap`].
pub type DefaultHashBuilder = ahash::RandomState;

/// Types needed to safely access shared data concurrently.
pub mod epoch {
    pub use crossbeam_epoch::{pin, Guard};
}
