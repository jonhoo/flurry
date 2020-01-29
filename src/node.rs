use crate::raw::Table;
use core::borrow::Borrow;
use core::sync::atomic::Ordering;
use crossbeam_epoch::{Atomic, Guard, Shared};
use parking_lot::Mutex;

/// Entry in a bin.
///
/// Will _generally_ be `Node`. Any entry that is not first in the bin, will be a `Node`.
#[derive(Debug)]
pub(crate) enum BinEntry<K, V> {
    Node(Node<K, V>),
    // safety: the pointer t to the next table inside Moved(t) is a valid pointer if the Moved(t)
    // entry was read after loading `map::HashMap.table` while the guard used to load that table is
    // still alive:
    //
    // When loading the current table of the HashMap with a guard g, the current epoch will be
    // pinned by g. This happens _before_ the resize which put the Moved entry into the current
    // table finishes, as otherwise a different table would have been loaded (see
    // `map::HashMap::transfer`).
    //
    // Hence, for the Moved(t) read from the loaded table:
    //
    //   - When trying to access t during the current resize, t points to map::HashMap.next_table
    //     and is thus valid.
    //
    //   - After the current resize and before another resize, `t == map::HashMap.table` as the
    //     "next table" t pointed to during the resize has become the current table. Thus t is
    //     still valid.
    //
    //   - The above is true until a subsequent resize ends, at which point `map::HashMap.table´ is
    //     set to another new table != t and t is `epoch::Guard::defer_destroy`ed (again, see
    //     `map::HashMap::transfer`). At this point, t is not referenced by the map anymore.
    //     However, the guard g used to load the table is still pinning the epoch at the time of
    //     the call to `defer_destroy`. Thus, t remains valid for at least the lifetime of g.
    //
    //   - After releasing g, either the current resize is finished and operations on the map
    //     cannot access t anymore as a more recent table will be loaded as the current table
    //     (see once again `map::HashMap::transfer`), or the argument is as above.
    //
    // Since finishing a resize is the only time a table is `defer_destroy`ed, the above covers
    // all cases.
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

impl<K, V> BinEntry<K, V> {
    pub(crate) fn find<'g, Q>(
        &'g self,
        hash: u64,
        key: &Q,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Eq,
    {
        match *self {
            BinEntry::Node(_) => {
                let mut node = self;
                loop {
                    let n = if let BinEntry::Node(ref n) = node {
                        n
                    } else {
                        unreachable!();
                    };

                    if n.hash == hash && n.key.borrow() == key {
                        return Shared::from(node as *const _);
                    }
                    let next = n.next.load(Ordering::SeqCst, guard);
                    if next.is_null() {
                        return Shared::null();
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
#[derive(Debug)]
pub(crate) struct Node<K, V> {
    pub(crate) hash: u64,
    pub(crate) key: K,
    pub(crate) value: Atomic<V>,
    pub(crate) next: Atomic<BinEntry<K, V>>,
    pub(crate) lock: Mutex<()>,
}
