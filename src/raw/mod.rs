use seize::Linked;

use crate::node::*;
use crate::reclaim::{self, Atomic, Collector, Guard, Shared};
use std::borrow::Borrow;
use std::fmt::Debug;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) struct Table<K, V> {
    bins: Box<[Atomic<BinEntry<K, V>>]>,

    // since a Moved does not contain associated information,
    // one instance is sufficient and shared across all bins in this table
    moved: Atomic<BinEntry<K, V>>,

    // since the information content of moved nodes is the same across
    // the table, we share this information
    //
    // safety: next_table is a valid pointer if it was read as consequence of loading _this_
    // table as `map::HashMap.table` and reading a BinEntry::Moved while still holding the
    // guard used for this load:
    //
    // When loading the current table of the HashMap with a guard g, the current epoch will be
    // pinned by g. This happens _before_ the resize which put the Moved entry into the this
    // table finishes, as otherwise a different table would have been loaded (see
    // `map::HashMap::transfer`).
    //
    // Hence:
    //
    //   - When trying to access next_table during the current resize, it points to
    //     map::HashMap.next_table and is thus valid.
    //
    //   - After the current resize and before another resize, `next_table == map::HashMap.table`
    //     as the "next table" it pointed to during the resize has become the current table. Thus,
    //     next_table is still valid.
    //
    //   - The above is true until a subsequent resize ends, at which point `map::HashMap.table´ is
    //     set to another new table != next_table and next_table is `epoch::Guard::defer_destroy`ed
    //     (again, see `map::HashMap::transfer`). At this point, next_table is not referenced by the
    //     map anymore. However, the guard g used to load _this_ table is still pinning the epoch at
    //     the time of the call to `defer_destroy`. Thus, next_table remains valid for at least the
    //     lifetime of g and, in particular, cannot be dropped before _this_ table.
    //
    //   - After releasing g, either the current resize is finished and operations on the map
    //     cannot access next_table anymore (as a more recent table will be loaded as the current
    //     table; see once again `map::HashMap::transfer`), or the argument is as above.
    //
    // Since finishing a resize is the only time a table is `defer_destroy`ed, the above covers
    // all cases.
    next_table: Atomic<Table<K, V>>,
}

impl<K, V> Table<K, V> {
    pub(crate) fn from(bins: Vec<Atomic<BinEntry<K, V>>>, collector: &Collector) -> Self {
        Self {
            bins: bins.into_boxed_slice(),
            moved: Atomic::from(Shared::boxed(BinEntry::Moved, collector)),
            next_table: Atomic::null(),
        }
    }

    pub(crate) fn new(bins: usize, collector: &Collector) -> Self {
        Self::from(vec![Atomic::null(); bins], collector)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bins.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.bins.len()
    }

    pub(crate) fn get_moved<'g>(
        &'g self,
        for_table: Shared<'g, Table<K, V>>,
        guard: &'g Guard<'_>,
    ) -> Shared<'g, BinEntry<K, V>> {
        match self.next_table(guard) {
            t if t.is_null() => {
                // if a no next table is yet associated with this table,
                // create one and store it in `self.next_table`
                match self.next_table.compare_exchange(
                    Shared::null(),
                    for_table,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    guard,
                ) {
                    Ok(_) => {}
                    Err(changed) => {
                        assert!(!changed.current.is_null());
                        assert_eq!(changed.current, for_table);
                    }
                }
            }
            t => {
                assert_eq!(t, for_table);
            }
        }
        // return a shared pointer to BinEntry::Moved
        self.moved.load(Ordering::SeqCst, guard)
    }

    pub(crate) fn find<'g, Q>(
        &'g self,
        bin: &Linked<BinEntry<K, V>>,
        hash: u64,
        key: &Q,
        guard: &'g Guard<'_>,
    ) -> Shared<'g, BinEntry<K, V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord,
    {
        match **bin {
            BinEntry::Node(_) => {
                let mut node = bin;
                loop {
                    let n = if let BinEntry::Node(ref n) = **node {
                        n
                    } else {
                        unreachable!("BinEntry::Node only points to BinEntry::Node");
                    };

                    if n.hash == hash && n.key.borrow() == key {
                        return Shared::from(node as *const _);
                    }
                    let next = n.next.load(Ordering::SeqCst, guard);
                    if next.is_null() {
                        return Shared::null();
                    }
                    // safety: next will only be dropped, if bin are dropped. bin won't be dropped until
                    // an epoch passes, which is protected by guard.
                    node = unsafe { next.deref() };
                }
            }
            BinEntry::Moved => {
                // safety: `self` is a reference to the old table. We got that under the given Guard.
                // Since we have not yet dropped that guard, _this_ table has not been garbage collected,
                // and so the _later_ table in `next_table`, _definitely_ hasn't.
                let mut table = unsafe { self.next_table(guard).deref() };

                loop {
                    if table.is_empty() {
                        return Shared::null();
                    }
                    let bini = table.bini(hash);
                    let bin = table.bin(bini, guard);
                    if bin.is_null() {
                        return Shared::null();
                    }
                    // safety: the table is protected by the guard, and so is the bin.
                    let bin = unsafe { bin.deref() };

                    match **bin {
                        BinEntry::Node(_) | BinEntry::Tree(_) => {
                            break table.find(bin, hash, key, guard)
                        }
                        BinEntry::Moved => {
                            // safety: same as above.
                            table = unsafe { table.next_table(guard).deref() };
                            continue;
                        }
                        BinEntry::TreeNode(_) => unreachable!("`find` was called on a Moved entry pointing to a TreeNode, which cannot be the first entry in a bin"),
                    }
                }
            }
            BinEntry::TreeNode(_) => {
                unreachable!(
                    "`find` was called on a TreeNode, which cannot be the first entry in a bin"
                );
            }
            BinEntry::Tree(_) => TreeBin::find(Shared::from(bin as *const _), hash, key, guard),
        }
    }

    pub(crate) fn drop_bins(&mut self) {
        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references to
        // anything in the map.
        let guard = unsafe { reclaim::unprotected() };

        for bin in Vec::from(std::mem::replace(&mut self.bins, vec![].into_boxed_slice())) {
            if bin.load(Ordering::SeqCst, guard).is_null() {
                // bin was never used
                continue;
            }

            // use deref first so we down turn shared BinEntry::Moved pointers to owned
            // note that dropping the shared Moved, if it exists, is the responsibility
            // of `drop`
            // safety: same as above
            let bin_entry = unsafe { bin.load(Ordering::SeqCst, guard).deref() };
            match **bin_entry {
                BinEntry::Moved => {}
                BinEntry::Node(_) => {
                    // safety: same as above + we own the bin - Nodes are not shared across the table
                    let mut p = unsafe { bin.into_box() };
                    loop {
                        // safety below:
                        // we're dropping the entire map, so no-one else is accessing it.
                        // we replaced the bin with a NULL, so there's no future way to access it
                        // either; we own all the nodes in the list.

                        let node = if let BinEntry::Node(node) = Linked::into_inner(*p) {
                            node
                        } else {
                            unreachable!();
                        };

                        // first, drop the value in this node
                        let _ = unsafe { node.value.into_box() };

                        // then we move to the next node
                        if node.next.load(Ordering::SeqCst, guard).is_null() {
                            break;
                        }
                        p = unsafe { node.next.into_box() };
                    }
                }
                BinEntry::Tree(_) => {
                    // safety: same as for BinEntry::Node
                    let p = unsafe { bin.into_box() };
                    let bin = if let BinEntry::Tree(bin) = Linked::into_inner(*p) {
                        bin
                    } else {
                        unreachable!();
                    };
                    // TreeBin::drop will take care of freeing the contained TreeNodes and their values
                    drop(bin);
                }
                BinEntry::TreeNode(_) => unreachable!(
                    "The head of a bin cannot be a TreeNode directly without BinEntry::Tree"
                ),
            }
        }
    }
}

impl<K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references to
        // anything in the map.
        let guard = unsafe { reclaim::unprotected() };

        // since BinEntry::Nodes are either dropped by drop_bins or transferred to a new table,
        // all bins are empty or contain a Shared pointing to shared the BinEntry::Moved (if
        // self.bins was not replaced by drop_bins anyway)
        let bins = Vec::from(std::mem::replace(&mut self.bins, vec![].into_boxed_slice()));

        // when testing, we check the above invariant. in production, we assume it to be true
        if cfg!(debug_assertions) {
            for bin in bins.iter() {
                let bin = bin.load(Ordering::SeqCst, guard);
                if bin.is_null() {
                    continue;
                } else {
                    // safety: we have mut access to self, so no-one else will drop this value under us.
                    let bin = unsafe { bin.deref() };
                    if let BinEntry::Moved = **bin {
                    } else {
                        unreachable!("dropped table with non-empty bin");
                    }
                }
            }
        }

        // as outlined above, at this point `bins` may still contain pointers to the shared
        // forwarding node. dropping `bins` here makes sure there is no way to accidentally access
        // the shared Moved after it gets dropped below.
        drop(bins);

        // we need to drop the shared forwarding node (since it is heap allocated).
        // Note that this needs to happen _independently_ of whether or not there was
        // a previous call to drop_bins.
        let moved = self.moved.swap(Shared::null(), Ordering::SeqCst, guard);
        assert!(
            !moved.is_null(),
            "self.moved is initialized together with the table"
        );

        // safety: we have mut access to self, so no-one else will drop this value under us.
        let moved = unsafe { moved.into_box() };
        drop(moved);

        // NOTE that the current table _is not_ responsible for `defer_destroy`ing the _next_ table
    }
}

impl<K, V> Table<K, V> {
    #[inline]
    pub(crate) fn bini(&self, hash: u64) -> usize {
        let mask = self.bins.len() as u64 - 1;
        (hash & mask) as usize
    }

    #[inline]
    pub(crate) fn bin<'g>(&'g self, i: usize, guard: &'g Guard<'_>) -> Shared<'g, BinEntry<K, V>> {
        self.bins[i].load(Ordering::Acquire, guard)
    }

    #[inline]
    #[allow(clippy::type_complexity)]
    pub(crate) fn cas_bin<'g>(
        &'g self,
        i: usize,
        current: Shared<'_, BinEntry<K, V>>,
        new: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard<'_>,
    ) -> Result<Shared<'g, BinEntry<K, V>>, reclaim::CompareExchangeError<'g, BinEntry<K, V>>> {
        self.bins[i].compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire, guard)
    }

    #[inline]
    pub(crate) fn store_bin(&self, i: usize, new: Shared<'_, BinEntry<K, V>>) {
        self.bins[i].store(new, Ordering::Release)
    }

    #[inline]
    pub(crate) fn next_table<'g>(&'g self, guard: &'g Guard<'_>) -> Shared<'g, Table<K, V>> {
        self.next_table.load(Ordering::SeqCst, guard)
    }
}
