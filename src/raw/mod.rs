use crate::node::*;
#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::fmt::Debug;
use core::sync::atomic::Ordering;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

#[derive(Debug)]
pub(crate) struct Table<K, V> {
    bins: Box<[Atomic<BinEntry<K, V>>]>,
}

impl<K, V> From<Vec<Atomic<BinEntry<K, V>>>> for Table<K, V> {
    fn from(bins: Vec<Atomic<BinEntry<K, V>>>) -> Self {
        Self {
            bins: bins.into_boxed_slice(),
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

    pub(crate) fn drop_bins(&mut self) {
        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references to
        // anything in the map.
        let guard = unsafe { crossbeam_epoch::unprotected() };

        for bin in Vec::from(core::mem::replace(
            &mut self.bins,
            vec![].into_boxed_slice(),
        )) {
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

        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references to
        // anything in the map.
        let guard = unsafe { crossbeam_epoch::unprotected() };

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
    pub(crate) fn cas_bin<'g>(
        &'g self,
        i: usize,
        current: Shared<'_, BinEntry<K, V>>,
        new: Owned<BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> Result<
        Shared<'g, BinEntry<K, V>>,
        crossbeam_epoch::CompareAndSetError<'g, BinEntry<K, V>, Owned<BinEntry<K, V>>>,
    > {
        self.bins[i].compare_and_set(current, new, Ordering::AcqRel, guard)
    }

    #[inline]
    pub(crate) fn store_bin<P: crossbeam_epoch::Pointer<BinEntry<K, V>>>(&self, i: usize, new: P) {
        self.bins[i].store(new, Ordering::Release)
    }
}
