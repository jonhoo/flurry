use crate::node::*;
use crossbeam_epoch::{Atomic, Guard, Owned, Pointer, Shared};
use std::fmt::Debug;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) struct Table<K, V> {
    bins: Box<[Atomic<BinEntry<K, V>>]>,
    // if a table contains moved bins, the BinEntry::Moved referring
    // to the new table looks exactly the same for all moved bins.
    // thus, we share the Moved across the table and store a reference
    // to it here
    moved: Atomic<BinEntry<K, V>>,
}

impl<K, V> From<Vec<Atomic<BinEntry<K, V>>>> for Table<K, V> {
    fn from(bins: Vec<Atomic<BinEntry<K, V>>>) -> Self {
        Self {
            bins: bins.into_boxed_slice(),
            moved: Atomic::null(),
        }
    }
}

impl<K, V> Table<K, V> {
    pub(crate) fn new(bins: usize) -> Self {
        Self::from(vec![Atomic::null(); bins])
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bins.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.bins.len()
    }

    pub(crate) fn get_moved<'g>(
        &'g self,
        for_table: *const Table<K, V>,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>> {
        match self.moved.load(Ordering::SeqCst, guard) {
            s if s.is_null() => {
                // if a BinEntry::Moved associated with this table does not yet exist,
                // create one and store it in `self.moved`
                self.moved
                    .store(Owned::new(BinEntry::Moved(for_table)), Ordering::SeqCst);
                self.moved.load(Ordering::SeqCst, guard)
            }
            s => {
                // safety: we only drop self.moved when we drop the table and
                // it is not null, so until then it points to valid memory
                if let BinEntry::Moved(ref table) = unsafe { s.deref() } {
                    assert_eq!(for_table, *table);
                    s
                } else {
                    unreachable!();
                }
            }
        }
    }

    pub(crate) fn drop_bins(&mut self) {
        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references to
        // anything in the map.
        let guard = unsafe { crossbeam_epoch::unprotected() };

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
            match *bin_entry {
                BinEntry::Moved(_) => {}
                BinEntry::Node(_) => {
                    // safety: same as above + we own the bin - Node are not shared across the table
                    let mut p = unsafe { bin.into_owned() };
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
        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references to
        // anything in the map.
        let guard = unsafe { crossbeam_epoch::unprotected() };

        // since BinEntry::Nodes are either dropped by drop_bins or transferred to a new table,
        // all bins are empty or contain a Shared pointing to shared the BinEntry::Moved (if
        // self.bins was not replaced by drop_bins anyway)
        for bin in &self.bins[..] {
            let bin = bin.swap(Shared::null(), Ordering::SeqCst, guard);
            // TODO: do we need the safety check against moved or do we assume the above invariant here?
            if bin.is_null() {
                continue;
            } else {
                // safety: we have mut access to self, so no-one else will drop this value under us.
                let bin = unsafe { bin.deref() };
                if let BinEntry::Moved(_) = *bin {
                } else {
                    unreachable!("dropped table with non-empty bin");
                }
            }
        }

        // we need to drop the shared forwarding node if it exists (since it is heap allocated).
        // Note that this needs to happen _independently_ of whether or not there was
        // a previous call to drop_bins.
        let moved = self.moved.swap(Shared::null(), Ordering::SeqCst, guard);
        if !moved.is_null() {
            // safety: we have mut access to self, so no-one else will drop this value under us.
            let moved = unsafe { moved.into_owned() };
            drop(moved);
        }
    }
}

impl<K, V> Table<K, V> {
    #[inline]
    pub(crate) fn bini(&self, hash: u64) -> usize {
        let mask = self.bins.len() as u64 - 1;
        (hash & mask) as usize
    }

    #[inline]
    pub(crate) fn bin<'g>(&'g self, i: usize, guard: &'g Guard) -> Shared<'g, BinEntry<K, V>> {
        self.bins[i].load(Ordering::Acquire, guard)
    }

    #[inline]
    #[allow(clippy::type_complexity)]
    pub(crate) fn cas_bin<'g, P>(
        &'g self,
        i: usize,
        current: Shared<'_, BinEntry<K, V>>,
        new: P,
        guard: &'g Guard,
    ) -> Result<
        Shared<'g, BinEntry<K, V>>,
        crossbeam_epoch::CompareAndSetError<'g, BinEntry<K, V>, P>,
    >
    where
        P: Pointer<BinEntry<K, V>>,
    {
        self.bins[i].compare_and_set(current, new, Ordering::AcqRel, guard)
    }

    #[inline]
    pub(crate) fn store_bin<P: Pointer<BinEntry<K, V>>>(&self, i: usize, new: P) {
        self.bins[i].store(new, Ordering::Release)
    }
}
