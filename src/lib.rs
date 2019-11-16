mod node;
use node::*;

use crossbream::epoch::{Atomic, Guard, Owned, Shared};
use parking_lot::lock_api::RawMutex;
use std::collections::hash_map::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicIsize, AtomicUsize};

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
const MIN_TRANSFER_STRIDE: usize = 16;

/// The number of bits used for generation stamp in `size_ctl`.
/// Must be at least 6 for 32bit arrays.
const RESIZE_STAMP_BITS: usize = 16;

/// The maximum number of threads that can help resize.
/// Must fit in `32 - RESIZE_STAMP_BITS` bits.
const MAX_RESIZERS: usize = (1 << (32 - RESIZE_STAMP_BITS)) - 1;

/// The bit shift for recording size stamp in `size_ctl`.
const RESIZE_STAMP_SHIFT: usize = 32 - RESIZE_STAMP_BITS;

pub struct FlurryHashMap<K, V, S = RandomState> {
    /// The array of bins. Lazily initialized upon first insertion.
    /// Size is always a power of two. Accessed directly by iterators.
    table: Atomic<Table<K, V, S>>,

    /// The next table to use; non-null only while resizing.
    next_table: Atomic<Table<K, V, S>>,

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

impl<K, V, S> FlurryHashMap<K, V, S>
where
    K: Hash,
    S: BuildHasher,
{
    fn hash(&self, key: &K) -> u64 {
        let mut h = self.build_hasher.build_hasher();
        key.hash(&mut h);
        h.finish()
    }

    pub fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<Shared<'g, V>> {
        let h = self.hash(key);
        let table = self.table.load(Ordering::SeqCst, guard);
        if table.is_null() {
            return None;
        }
        if table.bins.len() == 0 {
            return None;
        }
        let bini = table.bini(h);
        let bin = table.bin(bini, guard);
        if bin.is_null() {
            return None;
        }

        let node = bin.find(h, key);
        if node.is_null() {
            return None;
        }

        let v = node.value.load(Ordering::SeqCst, guard);
        assert!(!v.is_null());
        Some(v)
    }

    pub fn get_and<R, F: FnOnce(&V) -> R>(&self, key: &K, then: F) -> Option<R> {
        let guard = &crossbeam::epoch::pin();
        self.get(key, guard).map(|v| then(&*v))
    }

    pub fn insert(&self, key: K, value: V) -> Option<()> {}

    fn put(&self, key: K, value: V, no_replacement: bool) -> Option<()> {
        let h = self.hash(key);

        let guard = crossbeam::epoch::pin();
        let mut table = self.table.load(Ordering::SeqCst, &guard);

        let mut node = Owned::new(BinEntry::Node {
            key,
            value: Owned::new(value),
            hash: h,
            next: Atomic::null(),
        });

        loop {
            if table.is_null() || table.bins.len() == 0 {
                table = self.init_table(guard);
                continue;
            }

            let bini = table.bini(h);
            let mut bin = table.bin(bini, guard);
            if bin.is_null() {
                // fast path -- bin is empty so stick us at the front
                match table.cas_bin(bini, bin, node, guard) {
                    Ok(_old_null_ptr) => {
                        self.add_count(1, 0);
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
            match *bin {
                BinEntry::Moved => {
                    // FIXME: Moved.nextTable vs self.next_table ???
                    table = table.help_transfer();
                    unimplemented!()
                }
                BinEntry::Node(ref head)
                    if if_absent && head.hash == h && &head.key == &node.key =>
                {
                    // fast path if replacement is disallowed and first bin matches
                    return Some(());
                }
                BinEntry::Node(ref head) => {
                    // bin is non-empty, need to link into it, so we must take the lock
                    let _guard = head.lock.lock();

                    // need to check that this is _still_ the head
                    let current_head = table.bin(bini, guard);
                    if current_head.as_raw() != bin.as_raw() {
                        // nope -- try again from the start
                        continue;
                    }

                    // yes, it is still the head, so we can now "own" the bin
                    // note that there can still be readers in the bin!

                    // TODO: TreeBin & ReservationNode

                    let mut bin_count = 1;
                    let mut n = head;
                    let old_val = loop {
                        if n.hash == node.hash && n.key == node.key {
                            // the key already exists in the map!
                            if no_replacement {
                                // the key is not absent, so don't update
                            } else {
                                let now_garbage = n.value.swap(node.value, Ordering::SeqCst, guard);
                                unimplemented!("need to dispose of garbage");
                            }
                            break Some(());
                        }

                        // TODO: This Ordering can probably be relaxed due to the Mutex
                        let next = n.next.load(Ordering::SeqCst, guard);
                        if next.is_null() {
                            // we're at the end of the bin -- stick the node here!
                            n.next.store(node, Ordering::SeqCst);
                            break None;
                        }
                        n = next;

                        bin_count += 1;
                    };

                    // TODO: TREEIFY_THRESHOLD

                    if old_val.is_none() {
                        // increment count
                        self.add_count(1, bin_count);
                    }
                    return old_value;
                }
            }
        }
    }

    fn add_count(&self, n: isize, resize_hint: Option<usize>) {
        // TODO: implement the Java CounterCell business here

        let count = if n > 0 {
            let n = n as usize;
            self.count.fetch_add(n, Ordering::SeqCst) + n
        } else if n < 0 {
            let n = n.abs() as usize;
            self.count.fetch_sub(n, Ordering::SeqCst) - n
        } else {
            self.count.load(Ordering::SeqCst)
        };

        // if resize_hint is None, it means the caller does not want us to consider a resize.
        // if it is Some(n), the caller saw n entries in a bin
        if resize_hint.is_none() {
            return;
        }
        let saw_bin_length = resize_hint.unwrap();

        loop {
            let sc = self.size_ctl.load(Ordering::SeqCst);
            if count < sc {
                // we're not at the next resize point yet
                break;
            }

            let guard = crossbeam::epoch::pin();
            let mut table = self.table.load(Ordering::SeqCst, &guard);
            if table.is_null() {
                // table will be initalized by another thread anyway
                break;
            }

            let n = table.bins.len();
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
                let nt = self.next_table.load(Ordering::SeqCst, &guard);
                if nt.is_null() {
                    break;
                }
                if self.transfer_index.load(Ordering::SeqCst) <= 0 {
                    break;
                }

                // try to join!
                if self.size_ctl.compare_and_swap(sc, sc + 1, Ordering::SeqCst) == sc {
                    self.transfer(table, nt);
                }
            } else if self.size_ctl.compare_and_swap(sc, rs + 2, Ordering::SeqCst) == sc {
                // a resize is needed, but has not yet started
                // TODO: figure out why this is rs + 2, not just rs
                self.transfer(table, Shared::null());
            }

            // another resize may be needed!
            count = self.count.load(Ordering::SeqCst);
        }
    }

    fn transfer(&self, table: Shared<Table>, mut next_table: Shared<Table>, guard: &Guard) {
        let n = table.bins.len();
        // TODO: use num_cpus to help determine stride
        let stride = MIN_TRANSFER_STRIDE;

        if next_table.is_null() {
            // we are initiating a resize
            let table = Owned::new(Table {
                bins: [Atomic::null(); n << 1],
            });

            let now_garbage = self.next_table.store(table, Ordering::SeqCst, guard);
            assert!(now_garbage.is_null());
            self.transfer_index.store(n, Ordering::SeqCst);
            next_table = self.next_table.load(Ordering::Relaxed, guard);
        }

        let next_n = next_table.bins.len();
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

            if i < 0 || i >= n || i + n >= next_n {
                // the resize has finished

                if finishing {
                    // this branch is only taken for one thread partaking in the resize!
                    self.next_table.store(Owned::null(), Ordering::SeqCst);
                    let now_garbage = self.table.swap(next_table, Ordering::SeqCst);
                    unimplemented!("do something with the garbage");
                    self.size_ctl.store((n << 1) - (n >> 1), Ordering::SeqCst);
                    return;
                }

                let sc = self.size_ctl.load(Ordering::SeqCst);
                if self.size_ctl.compare_and_swap(sc, sc - 1) == sc {
                    if (sc - 2) != Self::resize_stamp(n) << RESIZE_STAMP_SHIFT {
                        return;
                    }

                    // we are the chosen thread to finish the resize!
                    finish = true;

                    // ???
                    advance = true;

                    // NOTE: the java code says "recheck before commit" here
                    i = n;
                }

                continue;
            }

            let bin = table.bin(i as usize);
            if bin.is_null() {
                advance = table
                    .cas_bin(
                        i,
                        Shared::null(),
                        Owned::new(BinEntry::Moved(next_table.as_raw())),
                        guard,
                    )
                    .is_ok();
                continue;
            }

            match *bin {
                BinEntry::Moved(_) => {
                    // already processed
                    advance = true;
                }
                BinEntry::Node(ref head) => {
                    // bin is non-empty, need to link into it, so we must take the lock
                    let _guard = head.lock.lock();

                    // need to check that this is _still_ the head
                    let current_head = table.bin(bini, guard);
                    if current_head.as_raw() != bin.as_raw() {
                        // nope -- try again from the start
                        continue;
                    }

                    // yes, it is still the head, so we can now "own" the bin
                    // note that there can still be readers in the bin!

                    // TODO: TreeBin & ReservationNode

                    let run_bit = head.hash & n as u64;
                    let mut last_run = head;
                    let mut p = head;
                    while !p.next.is_null() {
                        let b = p.hash & n as u64;
                        if b != run_bit {
                            run_bit = b;
                            last_run = p;
                        }
                        p = p.next.load(Ordering::SeqCst, guard);
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

                    p = head;
                    while p != last_run {
                        let link = if p.hash & n == 0 {
                            // to the low bin!
                            &mut low_bin
                        } else {
                            // to the high bin!
                            &mut high_bin
                        };

                        *link = Owned::new(Node {
                            hash: p.hash,
                            key: p.key.clone(),
                            lock: p.lock.clone(),
                            value: p.value.clone(),
                            next: *link,
                        })
                        .into_shared(guard);

                        p = p.next.load(Ordering::SeqCst, guard);
                    }

                    next_table.store(i, low_bin);
                    next_table.store(i + n, high_bin);
                    table.store(i, Owned::new(BinEntry::Moved(next_table.as_raw())));
                    advance = true;
                }
            }
        }
    }

    /// Returns the stamp bits for resizing a table of size n.
    /// Must be negative when shifted left by RESIZE_STAMP_SHIFT.
    fn resize_stamp(n: isize) -> isize {
        n.leading_zeros() | (1 << (RESIZE_STAMP_BITS - 1))
    }
}

struct Table<K, V> {
    bins: [Atomic<BinEntry<K, V>>],
}

impl Table<K, V> {
    #[inline]
    fn bini(&self, hash: u64) -> usize {
        let mask = self.bins.len() as u64 - 1;
        (h & mask) as usize
    }

    #[inline]
    fn bin<'g>(&'g self, i: usize, guard: &'g Guard) -> Shared<'g, BinEntry<K, V>> {
        self.bins[i].load(Ordering::Acquire, guard)
    }

    #[inline]
    fn cas_bin<'g>(
        &self,
        i: usize,
        current: Shared<BinEntry<K, V>>,
        new: Owned<BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> Result<
        Shared<'g, BinEntry<K, V>>,
        crossbeam::epoch::CompareAndSetError<'g, BinEntry<K, V>, Owned<BinEntry<K, V>>>,
    > {
        self.bins[i].compare_and_set(current, new, Ordering::AcqRel, guard)
    }

    #[inline]
    fn store_bin(&self, i: usize, new: Owned<BinEntry<K, V>>) {
        self.bins[i].store(new, Ordering::Relase)
    }
}
