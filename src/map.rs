use crate::iter::*;
use crate::node::*;
use crate::raw::*;
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::fmt::{self, Debug, Formatter};
use std::hash::{BuildHasher, Hash, Hasher};
use std::iter::FromIterator;
use std::sync::{
    atomic::{AtomicIsize, AtomicUsize, Ordering},
    Once,
};

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
/// simpler to use expressions such as `n - (n >> 2)` for
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
static NCPU: AtomicUsize = AtomicUsize::new(0);

/// A concurrent hash table.
///
/// See the [crate-level documentation](index.html) for details.
pub struct HashMap<K, V, S = RandomState> {
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

impl<K, V> Default for HashMap<K, V, RandomState>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> HashMap<K, V, RandomState>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
{
    /// Creates a new, empty map with the default initial table size (16).
    pub fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }

    /// Creates a new, empty map with an initial table size accommodating the specified number of
    /// elements without the need to dynamically resize.
    pub fn with_capacity(n: usize) -> Self {
        Self::with_capacity_and_hasher(RandomState::new(), n)
    }
}

impl<K, V, S: BuildHasher> HashMap<K, V, S> {
    /// Creates an empty map which will use `hash_builder` to hash keys.
    ///
    /// The created map has the default initial capacity.
    ///
    /// Warning: `hash_builder` is normally randomly generated, and is designed to
    /// allow the map to be resistant to attacks that cause many collisions and
    /// very poor performance. Setting it manually using this
    /// function can expose a DoS attack vector.
    pub fn with_hasher(hash_builder: S) -> Self {
        Self {
            table: Atomic::null(),
            next_table: Atomic::null(),
            transfer_index: AtomicIsize::new(0),
            count: AtomicUsize::new(0),
            size_ctl: AtomicIsize::new(0),
            build_hasher: hash_builder,
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
    pub fn with_capacity_and_hasher(hash_builder: S, n: usize) -> Self {
        if n == 0 {
            return Self::with_hasher(hash_builder);
        }

        let mut m = Self::with_hasher(hash_builder);
        let size = (1.0 + (n as f64) / LOAD_FACTOR) as usize;
        // NOTE: tableSizeFor in Java
        let cap = std::cmp::min(MAXIMUM_CAPACITY, size.next_power_of_two());
        m.size_ctl = AtomicIsize::new(cap as isize);
        m
    }
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
    S: BuildHasher,
{
    fn hash<Q: ?Sized + Hash>(&self, key: &Q) -> u64 {
        let mut h = self.build_hasher.build_hasher();
        key.hash(&mut h);
        h.finish()
    }

    /// Tests if `key` is a key in this table.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form must match those for the key type.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let guard = crossbeam_epoch::pin();
        self.get(key, &guard).is_some()
    }

    fn get_node<'g, Q>(&'g self, key: &Q, guard: &'g Guard) -> Option<&'g Node<K, V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let table = self.table.load(Ordering::SeqCst, guard);
        if table.is_null() {
            return None;
        }

        // safety: we loaded the table while epoch was pinned. table won't be deallocated until
        // next epoch at the earliest.
        let table = unsafe { table.deref() };
        if table.len() == 0 {
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
        Some(
            node.as_node()
                .expect("`BinEntry::find` should always return a Node"),
        )
    }

    /// Returns the value to which `key` is mapped.
    ///
    /// Returns `None` if this map contains no mapping for the key.
    ///
    /// To obtain a `Guard`, use [`epoch::pin`].
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form must match those for the key type.
    // TODO: implement a guard API of our own
    pub fn get<'g, Q>(&'g self, key: &Q, guard: &'g Guard) -> Option<&'g V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let node = self.get_node(key, guard)?;

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
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form must match those for the key type.
    pub fn get_and<Q, R, F>(&self, key: &Q, then: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
        F: FnOnce(&V) -> R,
    {
        let guard = &crossbeam_epoch::pin();
        self.get(key, guard).map(then)
    }

    /// Returns the key-value pair corresponding to `key`.
    ///
    /// Returns `None` if this map contains no mapping for `key`.
    ///
    /// The supplied `key` may be any borrowed form of the
    /// map's key type, but `Hash` and `Eq` on the borrowed form
    /// must match those for the key type.
    pub fn get_key_value<'g, Q>(&'g self, key: &Q, guard: &'g Guard) -> Option<(&'g K, &'g V)>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let node = self.get_node(key, guard)?;

        let v = node.value.load(Ordering::SeqCst, guard);
        assert!(!v.is_null());
        // safety: the lifetime of the reference is bound to the guard
        // supplied which means that the memory will not be modified
        // until at least after the guard goes out of scope
        unsafe { v.as_ref() }.map(|v| (&node.key, v))
    }

    fn init_table<'g>(&'g self, guard: &'g Guard) -> Shared<'g, Table<K, V>> {
        loop {
            let table = self.table.load(Ordering::SeqCst, guard);
            // safety: we loaded the table while epoch was pinned. table won't be deallocated until
            // next epoch at the earliest.
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

            if self.size_ctl.compare_and_swap(sc, -1, Ordering::SeqCst) == sc {
                // we get to do it!
                let mut table = self.table.load(Ordering::SeqCst, guard);

                // safety: we loaded the table while epoch was pinned. table won't be deallocated
                // until next epoch at the earliest.
                if table.is_null() || unsafe { table.deref() }.is_empty() {
                    let n = if sc > 0 {
                        sc as usize
                    } else {
                        DEFAULT_CAPACITY
                    };
                    let new_table = Owned::new(Table::new(n));
                    table = new_table.into_shared(guard);
                    self.table.store(table, Ordering::SeqCst);
                    // sc = ¾ n = n - n/4 = n - (n >> 2)
                    sc = n as isize - (n >> 2) as isize;
                }
                self.size_ctl.store(sc, Ordering::SeqCst);
                break table;
            }
        }
    }

    #[inline]
    /// Maps `key` to `value` in this table.
    ///
    /// The value can be retrieved by calling [`get`] with a key that is equal to the original key.
    pub fn insert<'g>(&'g self, key: K, value: V, guard: &'g Guard) -> Option<&'g V> {
        self.put(key, value, false, guard)
    }

    fn put<'g>(
        &'g self,
        key: K,
        value: V,
        no_replacement: bool,
        guard: &'g Guard,
    ) -> Option<&'g V> {
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
            if table.is_null() || unsafe { table.deref() }.len() == 0 {
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
                            let current_value = n.value.load(Ordering::SeqCst, guard);

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

    fn put_all<'g, I: Iterator<Item = (K, V)>>(&self, iter: I, guard: &'g Guard) {
        for (key, value) in iter {
            self.put(key, value, false, guard);
        }
    }

    fn help_transfer<'g>(
        &'g self,
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
        &'g self,
        table: Shared<'g, Table<K, V>>,
        mut next_table: Shared<'g, Table<K, V>>,
        guard: &'g Guard,
    ) {
        // safety: table was read while `guard` was held. the code that drops table only drops it
        // after it is no longer reachable, and any outstanding references are no longer active.
        // this references is still active (marked by the guard), so the target of the references
        // won't be dropped while the guard remains active.
        let n = unsafe { table.deref() }.len();
        let ncpu = num_cpus();

        let stride = if ncpu > 1 { (n >> 3) / ncpu } else { n };
        let stride = std::cmp::max(stride as isize, MIN_TRANSFER_STRIDE);

        if next_table.is_null() {
            // we are initiating a resize
            let table = Owned::new(Table::new(n << 1));
            let now_garbage = self.next_table.swap(table, Ordering::SeqCst, guard);
            assert!(now_garbage.is_null());
            self.transfer_index.store(n as isize, Ordering::SeqCst);
            next_table = self.next_table.load(Ordering::Relaxed, guard);
        }

        // safety: same argument as for table above
        let next_n = unsafe { next_table.deref() }.len();

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

    /// Removes the key (and its corresponding value) from this map.
    /// This method does nothing if the key is not in the map.
    /// Returns the previous value associated with the given key.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form must match those for the key type.
    pub fn remove<'g, Q>(&'g self, key: &Q, guard: &'g Guard) -> Option<&'g V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let h = self.hash(key);

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
            //     guard, so it won't be dropped until after we drop that guard b/c the drop logic
            //     only queues a drop for the next epoch after removing the table.
            //
            //  2. if table is set by a Moved node (below) through help_transfer, it will use the
            //     `next_table` raw pointer from inside the Moved. how do we know that that is safe?
            //
            //     we must demonstrate that if a Moved(t) is _read_, then t must still be valid.
            //     FIXME cf. put
            let t = unsafe { table.deref() };
            let n = t.len() as u64;
            if n == 0 {
                break;
            }
            let i = t.bini(h);
            let bin = t.bin(i, guard);
            if bin.is_null() {
                break;
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
                BinEntry::Moved(next_table) => {
                    table = self.help_transfer(table, next_table, guard);
                }
                BinEntry::Node(ref head) => {
                    let head_lock = head.lock.lock();
                    let mut old_val: Option<Shared<'g, V>> = None;

                    // need to check that this is _still_ the head
                    if t.bin(i, guard) != bin {
                        continue;
                    }

                    // TODO: tree nodes
                    let mut e = bin;
                    let mut pred: Shared<'_, BinEntry<K, V>> = Shared::null();
                    loop {
                        // safety: either e is bin, in which case it is valid due to the above,
                        // or e was obtained from a next pointer. Any next pointer obtained from
                        // bin is valid at the time we look up bin in the table, at which point
                        // the epoch is pinned by our guard. Since we found the next pointer
                        // in a valid map and it is not null (as checked above and below), the
                        // node it points to was present (i.e. not removed) from the map in the
                        // current epoch. Thus, because the epoch cannot advance until we release
                        // our guard, e is also valid if it was obtained from a next pointer.
                        let n = unsafe { e.deref() }.as_node().unwrap();
                        let next = n.next.load(Ordering::SeqCst, guard);
                        if n.hash == h && n.key.borrow() == key {
                            let ev = n.value.load(Ordering::SeqCst, guard);
                            old_val = Some(ev);

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
                                t.store_bin(i, next);
                            }

                            // in either case, mark the BinEntry as garbage, since it was just removed
                            // safety: as for val below / in put
                            unsafe { guard.defer_destroy(e) };

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

                    if let Some(val) = old_val {
                        self.add_count(-1, None, guard);
                        // safety: need to guarantee that the old value is no longer
                        // reachable. more specifically, no thread that executes _after_
                        // this line can ever get a reference to val.
                        //
                        // here are the possible cases:
                        //
                        //  - another thread already has a reference to the old value.
                        //    they must have read it before the call to store_bin.
                        //    because of this, that thread must be pinned to an epoch <=
                        //    the epoch of our guard. since the garbage is placed in our
                        //    epoch, it won't be freed until the _next_ epoch, at which
                        //    point, that thread must have dropped its guard, and with it,
                        //    any reference to the value.
                        //  - another thread is about to get a reference to this value.
                        //    they execute _after_ the store_bin, and therefore do _not_ get a
                        //    reference to the old value. there are no other ways to get to a
                        //    value except through its Node's `value` field (which is now gone
                        //    together with the node), so freeing the old value is fine.
                        unsafe { guard.defer_destroy(val) };

                        // safety: the lifetime of the reference is bound to the guard
                        // supplied which means that the memory will not be freed
                        // until at least after the guard goes out of scope
                        return unsafe { val.as_ref() };
                    }
                    break;
                }
            }
        }
        None
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all pairs (k, v) such that f(&k,&v) returns false.
    pub fn retain<F>(&mut self, mut f: F)
        where
            F: FnMut(&K, &V) -> bool
    {
        let guard = epoch::pin();
        // filter the keys which need to be removed
        let invalid_keys : Vec<&K> = self.iter(&guard)
            .filter_map(
                |(k, v)| {
                    match f(k, v) {
                        true  => None,
                        false => Some(k)
                    }
                }
            ).collect();
        // removed selected keys
        for k in invalid_keys {
            self.remove(k, &guard);
        }
    }

    /// An iterator visiting all key-value pairs in arbitrary order.
    /// The iterator element type is `(&'g K, &'g V)`.
    ///
    /// To obtain a `Guard`, use [`epoch::pin`].
    pub fn iter<'g>(&'g self, guard: &'g Guard) -> Iter<'g, K, V> {
        let table = self.table.load(Ordering::SeqCst, guard);
        let node_iter = NodeIter::new(table, guard);
        Iter { node_iter, guard }
    }

    /// An iterator visiting all keys in arbitrary order.
    /// The iterator element type is `&'g K`.
    ///
    /// To obtain a `Guard`, use [`epoch::pin`].
    pub fn keys<'g>(&'g self, guard: &'g Guard) -> Keys<'g, K, V> {
        let table = self.table.load(Ordering::SeqCst, guard);
        let node_iter = NodeIter::new(table, guard);
        Keys { node_iter }
    }

    /// An iterator visiting all values in arbitrary order.
    /// The iterator element type is `&'g V`.
    ///
    /// To obtain a `Guard`, use [`epoch::pin`].
    pub fn values<'g>(&'g self, guard: &'g Guard) -> Values<'g, K, V> {
        let table = self.table.load(Ordering::SeqCst, guard);
        let node_iter = NodeIter::new(table, guard);
        Values { node_iter, guard }
    }

    #[inline]
    /// Returns the number of entries in the map.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    #[inline]
    /// Returns `true` if the map is empty. Otherwise returns `false`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K, V, S> PartialEq for HashMap<K, V, S>
where
    K: Sync + Send + Clone + Eq + Hash,
    V: Sync + Send + PartialEq,
    S: BuildHasher,
{
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        let guard = epoch::pin();
        self.iter(&guard)
            .all(|(key, value)| other.get(key, &guard).map_or(false, |v| *value == *v))
    }
}

impl<K, V, S> Eq for HashMap<K, V, S>
where
    K: Sync + Send + Clone + Eq + Hash,
    V: Sync + Send + Eq,
    S: BuildHasher,
{
}

impl<K, V, S> fmt::Debug for HashMap<K, V, S>
where
    K: Sync + Send + Clone + Debug + Eq + Hash,
    V: Sync + Send + Debug,
    S: BuildHasher,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let guard = epoch::pin();
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
        // bound `&self` by `'g`, but if we did that, we would need to use a regular `epoch::Guard`
        // here rather than an unprotected one.
        let guard = unsafe { crossbeam_epoch::unprotected() };

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

impl<K, V, S> Extend<(K, V)> for &HashMap<K, V, S>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
    S: BuildHasher,
{
    #[inline]
    // TODO: Implement Java's `tryPresize` method to pre-allocate space for
    // the incoming entries
    // NOTE: `hashbrown::HashMap::extend` provides some good guidance on how
    // to choose the presizing value based on the iterator lower bound.
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        let guard = crossbeam_epoch::pin();
        (*self).put_all(iter.into_iter(), &guard);
    }
}

impl<'a, K, V, S> Extend<(&'a K, &'a V)> for &HashMap<K, V, S>
where
    K: Sync + Send + Copy + Hash + Eq,
    V: Sync + Send + Copy,
    S: BuildHasher,
{
    #[inline]
    fn extend<T: IntoIterator<Item = (&'a K, &'a V)>>(&mut self, iter: T) {
        self.extend(iter.into_iter().map(|(&key, &value)| (key, value)));
    }
}

impl<K, V> FromIterator<(K, V)> for HashMap<K, V, RandomState>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut iter = iter.into_iter();

        if let Some((key, value)) = iter.next() {
            // safety: we own `map`, so it's not concurrently accessed by
            // anyone else at this point.
            let guard = unsafe { crossbeam_epoch::unprotected() };

            let (lower, _) = iter.size_hint();
            let map = Self::with_capacity(lower.saturating_add(1));

            map.put(key, value, false, &guard);
            map.put_all(iter, &guard);
            map
        } else {
            Self::new()
        }
    }
}

impl<'a, K, V> FromIterator<(&'a K, &'a V)> for HashMap<K, V, RandomState>
where
    K: Sync + Send + Copy + Hash + Eq,
    V: Sync + Send + Copy,
{
    #[inline]
    fn from_iter<T: IntoIterator<Item = (&'a K, &'a V)>>(iter: T) -> Self {
        Self::from_iter(iter.into_iter().map(|(&k, &v)| (k, v)))
    }
}

impl<'a, K, V> FromIterator<&'a (K, V)> for HashMap<K, V, RandomState>
where
    K: Sync + Send + Copy + Hash + Eq,
    V: Sync + Send + Copy,
{
    #[inline]
    fn from_iter<T: IntoIterator<Item = &'a (K, V)>>(iter: T) -> Self {
        Self::from_iter(iter.into_iter().map(|&(k, v)| (k, v)))
    }
}

impl<K, V, S> Clone for HashMap<K, V, S>
where
    K: Sync + Send + Clone + Hash + Eq,
    V: Sync + Send + Clone,
    S: BuildHasher + Clone,
{
    fn clone(&self) -> HashMap<K, V, S> {
        let cloned_map = Self::with_capacity_and_hasher(self.build_hasher.clone(), self.len());
        {
            let guard = epoch::pin();
            for (k, v) in self.iter(&guard) {
                cloned_map.insert(k.clone(), v.clone(), &guard);
            }
        }
        cloned_map
    }
}

#[inline]
/// Returns the number of physical CPUs in the machine (_O(1)_).
fn num_cpus() -> usize {
    NCPU_INITIALIZER.call_once(|| NCPU.store(num_cpus::get_physical(), Ordering::Relaxed));

    NCPU.load(Ordering::Relaxed)
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
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.insert((), (), &guard);
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.get(&(), &guard);
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.remove(&(), &guard);
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.iter(&guard).next();
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.keys(&guard).next();
/// drop(map);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.values(&guard).next();
/// drop(map);
/// drop(r);
/// ```
///
/// # No references outlive the guard.
///
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.insert((), (), &guard);
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.get(&(), &guard);
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.remove(&(), &guard);
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.iter(&guard).next();
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.keys(&guard).next();
/// drop(guard);
/// drop(r);
/// ```
/// ```compile_fail
/// let guard = crossbeam_epoch::pin();
/// let map = super::HashMap::default();
/// let r = map.values(&guard).next();
/// drop(guard);
/// drop(r);
/// ```
#[allow(dead_code)]
struct CompileFailTests;
