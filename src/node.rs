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

impl<K, V> BinEntry<K, V>
where
    K: Eq,
{
    pub(crate) fn find<'g>(
        &'g self,
        hash: u64,
        key: &K,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        match *self {
            BinEntry::Node(ref n) => n.find(hash, key, guard),
            BinEntry::Moved(next_table) => {
                // We have a reference to the old table, otherwise we wouldn't have a reference to
                // self. We got that under the given Guard. Since we have not yet dropped that
                // guard, no collection has happened since then. Since _this_ table has not been
                // garbage collected, the _later_ table in next_table, _definitely_ hasn't.
                let mut table = Shared::from(next_table);

                loop {
                    if table.is_null() || table.bins.is_empty() {
                        return Shared::null();
                    }
                    let bini = table.bini(hash);
                    let mut bin = table.bin(bini);
                    if bin.is_null() {
                        return Shared::null();
                    }

                    match *bin {
                        BinEntry::Node(ref n) => break n.find(hash, key, guard),
                        BinEntry::Moved(next_table) => {
                            table = Shared::from(next_table);
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
    pub(crate) next: Atomic<Node<K, V>>,
    pub(crate) lock: Mutex<()>,
}

impl<K, V> Node<K, V>
where
    K: Eq,
{
    pub(crate) fn find<'g>(
        &'g self,
        hash: u64,
        key: &K,
        guard: &'g Guard,
    ) -> Shared<'g, Node<K, V>> {
        // TODO: maybe turn into a loop instead of recursing
        if self.hash == hash && &self.key == key {
            return Shared::from(self as *const _);
        }
        let next = self.next.load(Ordering::SeqCst, guard);
        if next.is_null() {
            return Shared::null();
        }
        return next.find(hash, key, guard);
    }
}
