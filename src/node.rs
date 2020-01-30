use crate::raw::Table;
use crossbeam_epoch::Atomic;
use lock_api::Mutex;

/// Entry in a bin.
///
/// Will _generally_ be `Node`. Any entry that is not first in the bin, will be a `Node`.
#[derive(Debug)]
pub(crate) enum BinEntry<K, V, L>
where
    L: lock_api::RawMutex,
{
    Node(Node<K, V, L>),
    Moved,
}

unsafe impl<K, V, L> Send for BinEntry<K, V, L>
where
    K: Send,
    V: Send,
    L: Send,
    Node<K, V, L>: Send,
    Table<K, V, L>: Send,
    L: lock_api::RawMutex,
{
}

unsafe impl<K, V, L> Sync for BinEntry<K, V, L>
where
    K: Sync,
    V: Sync,
    L: Sync,
    Node<K, V, L>: Sync,
    Table<K, V, L>: Sync,
    L: lock_api::RawMutex,
{
}

impl<K, V, L> BinEntry<K, V, L>
where
    L: lock_api::RawMutex,
{
    pub(crate) fn as_node(&self) -> Option<&Node<K, V, L>> {
        if let BinEntry::Node(ref n) = *self {
            Some(n)
        } else {
            None
        }
    }
}

/// Key-value entry.
#[derive(Debug)]
pub(crate) struct Node<K, V, L>
where
    L: lock_api::RawMutex,
{
    pub(crate) hash: u64,
    pub(crate) key: K,
    pub(crate) value: Atomic<V>,
    pub(crate) next: Atomic<BinEntry<K, V, L>>,
    pub(crate) lock: Mutex<L, ()>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_epoch::Owned;
    use std::sync::atomic::Ordering;

    fn new_node(hash: u64, key: usize, value: usize) -> Node<usize, usize, parking_lot::RawMutex> {
        Node {
            hash,
            key,
            value: Atomic::new(value),
            next: Atomic::null(),
            lock: Mutex::new(()),
        }
    }

    #[test]
    fn find_node_no_match() {
        let guard = &crossbeam_epoch::pin();
        let node2 = new_node(4, 5, 6);
        let entry2 = BinEntry::Node(node2);
        let node1 = new_node(1, 2, 3);
        node1.next.store(Owned::new(entry2), Ordering::SeqCst);
        let entry1 = Owned::new(BinEntry::Node(node1)).into_shared(guard);
        let mut tab = Table::from(vec![Atomic::from(entry1)]);

        // safety: we have not yet dropped entry1
        assert!(tab.find(unsafe { entry1.deref() }, 1, &0, guard).is_null());
        tab.drop_bins();
    }

    #[test]
    fn find_node_single_match() {
        let guard = &crossbeam_epoch::pin();
        let entry = Owned::new(BinEntry::Node(new_node(1, 2, 3))).into_shared(guard);
        let mut tab = Table::from(vec![Atomic::from(entry)]);
        assert_eq!(
            // safety: we have not yet dropped entry
            unsafe { tab.find(entry.deref(), 1, &2, guard).deref() }
                .as_node()
                .unwrap()
                .key,
            2
        );
        tab.drop_bins();
    }

    #[test]
    fn find_node_multi_match() {
        let guard = &crossbeam_epoch::pin();
        let node2 = new_node(4, 5, 6);
        let entry2 = BinEntry::Node(node2);
        let node1 = new_node(1, 2, 3);
        node1.next.store(Owned::new(entry2), Ordering::SeqCst);
        let entry1 = Owned::new(BinEntry::Node(node1)).into_shared(guard);
        let mut tab = Table::from(vec![Atomic::from(entry1)]);
        assert_eq!(
            // safety: we have not yet dropped entry1
            unsafe { tab.find(entry1.deref(), 4, &5, guard).deref() }
                .as_node()
                .unwrap()
                .key,
            5
        );
        tab.drop_bins();
    }

    #[test]
    fn find_moved_empty_bins_no_match() {
        let guard = &crossbeam_epoch::pin();
        let mut table = Table::<usize, usize, parking_lot::RawMutex>::new(1);
        let mut table2 = Owned::new(Table::new(1)).into_shared(guard);

        let entry = table.get_moved(table2, guard);
        table.store_bin(0, entry);
        assert!(table.find(&BinEntry::Moved, 1, &2, guard).is_null());
        table.drop_bins();
        // safety: table2 is still valid and not accessed by different threads
        unsafe { table2.deref_mut() }.drop_bins();
        unsafe { guard.defer_destroy(table2) };
    }

    #[test]
    fn find_moved_no_bins_no_match() {
        let guard = &crossbeam_epoch::pin();
        let mut table = Table::<usize, usize, parking_lot::RawMutex>::new(1);
        let mut table2 = Owned::new(Table::new(0)).into_shared(guard);
        let entry = table.get_moved(table2, guard);
        table.store_bin(0, entry);
        assert!(table.find(&BinEntry::Moved, 1, &2, guard).is_null());
        table.drop_bins();
        // safety: table2 is still valid and not accessed by different threads
        unsafe { table2.deref_mut() }.drop_bins();
        unsafe { guard.defer_destroy(table2) };
    }

    #[test]
    fn find_moved_null_bin_no_match() {
        let guard = &crossbeam_epoch::pin();
        let mut table = Table::<usize, usize, parking_lot::RawMutex>::new(1);
        let mut table2 = Owned::new(Table::new(2)).into_shared(guard);
        unsafe { table2.deref() }.store_bin(0, Owned::new(BinEntry::Node(new_node(1, 2, 3))));
        let entry = table.get_moved(table2, guard);
        table.store_bin(0, entry);
        assert!(table.find(&BinEntry::Moved, 0, &1, guard).is_null());
        table.drop_bins();
        // safety: table2 is still valid and not accessed by different threads
        unsafe { table2.deref_mut() }.drop_bins();
        unsafe { guard.defer_destroy(table2) };
    }

    #[test]
    fn find_moved_match() {
        let guard = &crossbeam_epoch::pin();
        let mut table = Table::<usize, usize, parking_lot::RawMutex>::new(1);
        let mut table2 = Owned::new(Table::new(1)).into_shared(guard);
        // safety: table2 is still valid
        unsafe { table2.deref() }.store_bin(0, Owned::new(BinEntry::Node(new_node(1, 2, 3))));
        let entry = table.get_moved(table2, guard);
        table.store_bin(0, entry);
        assert_eq!(
            // safety: entry is still valid since the table was not dropped and the
            // entry was not removed
            unsafe { table.find(&BinEntry::Moved, 1, &2, guard).deref() }
                .as_node()
                .unwrap()
                .key,
            2
        );
        table.drop_bins();
        // safety: table2 is still valid and not accessed by different threads
        unsafe { table2.deref_mut() }.drop_bins();
        unsafe { guard.defer_destroy(table2) };
    }
}
