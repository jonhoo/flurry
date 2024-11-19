use crate::node::{BinEntry, Node, TreeNode};
use crate::raw::Table;
use crate::reclaim::{Guard, Linked, Shared};
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) struct NodeIter<'g, K, V> {
    /// Current table; update if resized
    table: Option<&'g Linked<Table<K, V>>>,

    stack: Option<Box<TableStack<'g, K, V>>>,
    spare: Option<Box<TableStack<'g, K, V>>>,

    /// The last bin entry iterated over
    prev: Option<&'g Node<K, V>>,

    /// Index of bin to use next
    index: usize,

    /// Current index of initial table
    base_index: usize,

    /// Index bound for inital table
    base_limit: usize,

    /// Initial table size
    base_size: usize,

    guard: &'g Guard<'g>,
}

impl<'g, K, V> NodeIter<'g, K, V> {
    pub(crate) fn new(table: Shared<'g, Table<K, V>>, guard: &'g Guard<'_>) -> Self {
        let (table, len) = if table.is_null() {
            (None, 0)
        } else {
            // safety: flurry guarantees that a table read under a guard is never dropped or moved
            // until after that guard is dropped.
            let table = unsafe { table.deref() };
            (Some(table), table.len())
        };

        Self {
            table,
            stack: None,
            spare: None,
            prev: None,
            base_size: len,
            base_index: 0,
            index: 0,
            base_limit: len,
            guard,
        }
    }

    fn push_state(&mut self, t: &'g Linked<Table<K, V>>, i: usize, n: usize) {
        let mut s = self.spare.take();
        if let Some(ref mut s) = s {
            self.spare = s.next.take();
        }

        let target = TableStack {
            table: t,
            length: n,
            index: i,
            next: self.stack.take(),
        };

        self.stack = if let Some(mut s) = s {
            *s = target;
            Some(s)
        } else {
            Some(Box::new(target))
        };
    }

    fn recover_state(&mut self, mut n: usize) {
        while let Some(ref mut s) = self.stack {
            if self.index + s.length < n {
                // if we haven't checked the high "side" of this bucket,
                // then do _not_ pop the stack frame,
                // and instead moveon to that bin.
                self.index += s.length;
                break;
            }

            // we _are_ popping the stack
            let mut s = self.stack.take().expect("while let Some");
            n = s.length;
            self.index = s.index;
            self.table = Some(s.table);
            self.stack = s.next.take();

            // save stack frame for re-use
            s.next = self.spare.take();
            self.spare = Some(s);
        }

        if self.stack.is_none() {
            // move to next "part" of the top-level bin in the largest table
            self.index += self.base_size;
            if self.index >= n {
                // we've gone past the last part of this top-level bin,
                // so move to the _next_ top-level bin.
                self.base_index += 1;
                self.index = self.base_index;
            }
        }
    }
}

impl<'g, K, V> Iterator for NodeIter<'g, K, V> {
    type Item = &'g Node<K, V>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut e = None;
        if let Some(prev) = self.prev {
            let next = prev.next.load(Ordering::SeqCst, self.guard);
            if !next.is_null() {
                // we have to check if we are iterating over a regular bin or a
                // TreeBin. the Java code gets away without this due to
                // inheritance (everything is a node), but we have to explicitly
                // check
                // safety: flurry does not drop or move until after guard drop
                match **unsafe { next.deref() } {
                    BinEntry::Node(ref node) => {
                        e = Some(node);
                    }
                    BinEntry::TreeNode(ref tree_node) => {
                        e = Some(&tree_node.node);
                    }
                    BinEntry::Moved | BinEntry::Tree(_) => {
                        unreachable!("Nodes can only point to Nodes or TreeNodes")
                    }
                }
            }
        }

        loop {
            if e.is_some() {
                self.prev = e;
                return e;
            }

            // safety: flurry does not drop or move until after guard drop
            if self.base_index >= self.base_limit
                || self.table.is_none()
                || self.table.as_ref().unwrap().len() <= self.index
            {
                self.prev = None;
                return None;
            }

            let t = self.table.expect("is_none in if above");
            let i = self.index;
            let n = t.len();
            let bin = t.bin(i, self.guard);
            if !bin.is_null() {
                // safety: flurry does not drop or move until after guard drop
                let bin = unsafe { bin.deref() };
                match **bin {
                    BinEntry::Moved => {
                        // recurse down into the target table
                        // safety: same argument as for following Moved in Table::find
                        self.table = Some(unsafe { t.next_table(self.guard).deref() });
                        self.prev = None;
                        // make sure we can get back "up" to where we're at
                        self.push_state(t, i, n);
                        continue;
                    }
                    BinEntry::Node(ref node) => {
                        e = Some(node);
                    }
                    BinEntry::Tree(ref tree_bin) => {
                        // since we want to iterate over all entries, TreeBins
                        // are also traversed via the `next` pointers of their
                        // contained node
                        e = Some(
                            // safety: `bin` was read under our guard, at which
                            // point the tree was valid. Since our guard marks
                            // the current thread as active, the TreeNodes remain valid for
                            // at least as long as we hold onto the guard.
                            // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                            &unsafe {
                                TreeNode::get_tree_node(
                                    tree_bin.first.load(Ordering::SeqCst, self.guard),
                                )
                            }
                            .node,
                        );
                    }
                    BinEntry::TreeNode(_) => unreachable!(
                        "The head of a bin cannot be a TreeNode directly without BinEntry::Tree"
                    ),
                }
            }

            if self.stack.is_some() {
                self.recover_state(n);
            } else {
                self.index = i + self.base_size;
                if self.index >= n {
                    self.base_index += 1;
                    self.index = self.base_index;
                }
            }
        }
    }
}

#[derive(Debug)]
struct TableStack<'g, K, V> {
    length: usize,
    index: usize,
    table: &'g Linked<Table<K, V>>,
    next: Option<Box<TableStack<'g, K, V>>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::Table;
    use crate::reclaim::Atomic;
    use parking_lot::Mutex;

    #[test]
    fn iter_new() {
        let guard = unsafe { seize::Guard::unprotected() };
        let iter = NodeIter::<usize, usize>::new(Shared::null(), &guard);
        assert_eq!(iter.count(), 0);
    }

    #[test]
    fn iter_empty() {
        let collector = seize::Collector::new();

        let table = Shared::boxed(Table::<usize, usize>::new(16, &collector), &collector);
        let guard = collector.enter();
        let iter = NodeIter::new(table, &guard);
        assert_eq!(iter.count(), 0);

        // safety: nothing holds on to references into the table any more
        let mut t = unsafe { table.into_box() };
        t.drop_bins();
    }

    #[test]
    fn iter_simple() {
        let collector = seize::Collector::new();
        let mut bins = vec![Atomic::null(); 16];
        bins[8] = Atomic::from(Shared::boxed(
            BinEntry::Node(Node {
                hash: 0,
                key: 0usize,
                value: Atomic::from(Shared::boxed(0usize, &collector)),
                next: Atomic::null(),
                lock: Mutex::new(()),
            }),
            &collector,
        ));

        let table = Shared::boxed(Table::from(bins, &collector), &collector);
        let guard = collector.enter();
        {
            let mut iter = NodeIter::new(table, &guard);
            let e = iter.next().unwrap();
            assert_eq!(e.key, 0);
            assert!(iter.next().is_none());
        }

        // safety: nothing holds on to references into the table any more
        let mut t = unsafe { table.into_box() };
        t.drop_bins();
    }

    #[test]
    fn iter_fw() {
        // construct the forwarded-to table
        let collector = seize::Collector::new();
        let mut deep_bins = vec![Atomic::null(); 16];
        deep_bins[8] = Atomic::from(Shared::boxed(
            BinEntry::Node(Node {
                hash: 0,
                key: 0usize,
                value: Atomic::from(Shared::boxed(0usize, &collector)),
                next: Atomic::null(),
                lock: Mutex::new(()),
            }),
            &collector,
        ));

        let guard = collector.enter();
        let deep_table = Shared::boxed(Table::from(deep_bins, &collector), &collector);

        // construct the forwarded-from table
        let mut bins = vec![Shared::null(); 16];
        let table = Table::<usize, usize>::new(bins.len(), &collector);
        for bin in &mut bins[8..] {
            // this also sets table.next_table to deep_table
            *bin = table.get_moved(deep_table, &guard);
        }
        // this cannot use Table::from(bins), since we need the table to get
        // the Moved and set its next_table
        for i in 0..bins.len() {
            table.store_bin(i, bins[i]);
        }
        let table = Shared::boxed(table, &collector);
        {
            let mut iter = NodeIter::new(table, &guard);
            let e = iter.next().unwrap();
            assert_eq!(e.key, 0);
            assert!(iter.next().is_none());
        }

        // safety: nothing holds on to references into the table any more
        let mut t = unsafe { table.into_box() };
        t.drop_bins();
        // no one besides this test case uses deep_table
        unsafe { deep_table.into_box() }.drop_bins();
    }
}
