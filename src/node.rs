use super::Table;
use crossbeam::epoch::{Atomic, Guard, Shared};
use parking_lot::Mutex;
use std::sync::atomic::Ordering;

/// Entry in a bin.
///
/// Will _generally_ be `Node`. Any entry that is not first in the bin, will be a `Node`.
pub(crate) enum BinEntry<K, V> {
    Node(Node<K, V>),
    Moved(*const Table<K, V>),
}

unsafe impl<K, V> Send for BinEntry<K, V>
where
    K: Send,
    V: Send,
    Node<K, V>: Send,
    Table<K, V>: Send,
{
}

unsafe impl<K, V> Sync for BinEntry<K, V>
where
    K: Sync,
    V: Sync,
    Node<K, V>: Sync,
    Table<K, V>: Sync,
{
}

impl<K, V> BinEntry<K, V> {
    pub(crate) fn as_node(&self) -> Option<&Node<K, V>> {
        if let BinEntry::Node(ref n) = *self {
            Some(n)
        } else {
            None
        }
    }
}

impl<K, V> BinEntry<K, V>
where
    K: Eq,
{
    pub(crate) fn find<'g>(
        &'g self,
        hash: u64,
        key: &K,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>> {
        match *self {
            BinEntry::Node(_) => {
                let mut node = self;
                loop {
                    let n = if let BinEntry::Node(ref n) = node {
                        n
                    } else {
                        unreachable!();
                    };

                    if n.hash == hash && &n.key == key {
                        break Shared::from(self as *const _);
                    }
                    let next = n.next.load(Ordering::SeqCst, guard);
                    if next.is_null() {
                        break Shared::null();
                    }
                    // safety: next will only be dropped, if we are dropped. we won't be dropped until epoch
                    // passes, which is protected by guard.
                    node = unsafe { next.deref() };
                }
            }
            BinEntry::Moved(next_table) => {
                // safety: We have a reference to the old table, otherwise we wouldn't have a reference to
                // self. We got that under the given Guard. Since we have not yet dropped that
                // guard, _this_ table has not been garbage collected, and so the _later_ table in
                // next_table, _definitely_ hasn't.
                let mut table = unsafe { &*next_table };

                loop {
                    if table.bins.is_empty() {
                        return Shared::null();
                    }
                    let bini = table.bini(hash);
                    let bin = table.bin(bini, guard);
                    if bin.is_null() {
                        return Shared::null();
                    }
                    // safety: the table is protected by the guard, and so is the bin.
                    let bin = unsafe { bin.deref() };

                    match *bin {
                        BinEntry::Node(_) => break bin.find(hash, key, guard),
                        BinEntry::Moved(next_table) => {
                            // safety: same as above.
                            table = unsafe { &*next_table };
                            continue;
                        }
                    }
                }
            }
        }
    }
}

/// Key-value entry.
pub(crate) struct Node<K, V> {
    pub(crate) hash: u64,
    pub(crate) key: K,
    pub(crate) value: Atomic<V>,
    pub(crate) next: Atomic<BinEntry<K, V>>,
    pub(crate) lock: Mutex<()>,
}
