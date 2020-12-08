use crate::ebr::{Guard, Shared};
use crate::node::{BinEntry, Node, TreeNode};
use crate::raw::Table;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) struct NodeIter<'m, 'g, K, V, SH> {
    /// Current table; update if resized
    table: Option<&'g Table<K, V>>,

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

    guard: &'g Guard<'m, SH>,
}

impl<'m, 'g, K, V, SH> NodeIter<'m, 'g, K, V, SH>
where
    SH: flize::Shield<'m>,
{
    pub(crate) fn new(table: Shared<'g, Table<K, V>>, guard: &'g Guard<'m, SH>) -> Self {
        let (table, len) = if table.is_null() {
            (None, 0)
        } else {
            // safety: flurry guarantees that a table read under a guard is never dropped or moved
            // until after that guard is dropped.
            let table = unsafe { table.as_ref_unchecked() };
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

    fn push_state(&mut self, t: &'g Table<K, V>, i: usize, n: usize) {
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

impl<'m, 'g, K, V, SH> Iterator for NodeIter<'m, 'g, K, V, SH>
where
    SH: flize::Shield<'m>,
{
    type Item = &'g Node<K, V>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut e = None;
        if let Some(prev) = self.prev {
            let next = prev.next.load(Ordering::SeqCst, &self.guard.shield);
            if !next.is_null() {
                // we have to check if we are iterating over a regular bin or a
                // TreeBin. the Java code gets away without this due to
                // inheritance (everything is a node), but we have to explicitly
                // check
                // safety: flurry does not drop or move until after guard drop
                match unsafe { next.as_ref_unchecked() } {
                    BinEntry::Node(node) => {
                        e = Some(node);
                    }
                    BinEntry::TreeNode(tree_node) => {
                        e = Some(&tree_node.node);
                    }
                    BinEntry::Moved => unreachable!("Nodes can only point to Nodes or TreeNodes"),
                    BinEntry::Tree(_) => unreachable!("Nodes can only point to Nodes or TreeNodes"),
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
                let bin = unsafe { bin.as_ref_unchecked() };
                match bin {
                    BinEntry::Moved => {
                        // recurse down into the target table
                        // safety: same argument as for following Moved in Table::find
                        self.table = Some(unsafe { t.next_table(self.guard).as_ref_unchecked() });
                        self.prev = None;
                        // make sure we can get back "up" to where we're at
                        self.push_state(t, i, n);
                        continue;
                    }
                    BinEntry::Node(node) => {
                        e = Some(node);
                    }
                    BinEntry::Tree(tree_bin) => {
                        // since we want to iterate over all entries, TreeBins
                        // are also traversed via the `next` pointers of their
                        // contained node
                        e = Some(
                            // safety: `bin` was read under our guard, at which
                            // point the tree was valid. Since our guard pins
                            // the current epoch, the TreeNodes remain valid for
                            // at least as long as we hold onto the guard.
                            // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                            &unsafe {
                                TreeNode::get_tree_node(
                                    tree_bin.first.load(Ordering::SeqCst, &self.guard.shield),
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
    table: &'g Table<K, V>,
    next: Option<Box<TableStack<'g, K, V>>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ebr::{Atomic, Shared, SharedExt};
    use crate::raw::Table;
    use parking_lot::Mutex;

    #[test]
    fn iter_new() {
        let collector = flize::Collector::new();
        let guard = Guard {
            shield: collector.thin_shield(),
            collector: &collector,
        };
        let iter = NodeIter::<usize, usize, _>::new(Shared::null(), &guard);
        assert_eq!(iter.count(), 0);
    }

    #[test]
    fn iter_empty() {
        let table = Shared::boxed(Table::<usize, usize>::new(16));
        let collector = flize::Collector::new();
        let guard = Guard {
            shield: collector.thin_shield(),
            collector: &collector,
        };
        let iter = NodeIter::new(table, &guard);
        assert_eq!(iter.count(), 0);

        // safety: nothing holds on to references into the table any more
        let mut t = table.into_box();
        t.drop_bins();
    }

    #[test]
    fn iter_simple() {
        let mut bins = Vec::with_capacity(16);
        (0..16).for_each(|_| bins.push(Atomic::null()));
        bins[8] = Atomic::new(Shared::boxed(BinEntry::Node(Node {
            hash: 0,
            key: 0usize,
            value: Atomic::new(Shared::boxed(0usize)),
            next: Atomic::null(),
            lock: Mutex::new(()),
        })));

        let table = Shared::boxed(Table::from(bins));
        let collector = flize::Collector::new();
        let guard = Guard {
            shield: collector.thin_shield(),
            collector: &collector,
        };
        {
            let mut iter = NodeIter::new(table, &guard);
            let e = iter.next().unwrap();
            assert_eq!(e.key, 0);
            assert!(iter.next().is_none());
        }

        // safety: nothing holds on to references into the table any more
        let mut t = table.into_box();
        t.drop_bins();
    }

    #[test]
    fn iter_fw() {
        // construct the forwarded-to table
        let mut deep_bins = Vec::with_capacity(16);
        (0..16).for_each(|_| deep_bins.push(Atomic::null()));
        deep_bins[8] = Atomic::new(Shared::boxed(BinEntry::Node(Node {
            hash: 0,
            key: 0usize,
            value: Atomic::new(Shared::boxed(0usize)),
            next: Atomic::null(),
            lock: Mutex::new(()),
        })));
        let collector = flize::Collector::new();
        let guard = Guard {
            shield: collector.thin_shield(),
            collector: &collector,
        };
        let deep_table = Shared::boxed(Table::from(deep_bins));

        // construct the forwarded-from table
        let mut bins = vec![Shared::null(); 16];
        let table = Table::<usize, usize>::new(bins.len());
        for bin in &mut bins[8..] {
            // this also sets table.next_table to deep_table
            *bin = table.get_moved(deep_table, &guard);
        }
        // this cannot use Table::from(bins), since we need the table to get
        // the Moved and set its next_table
        for i in 0..bins.len() {
            table.store_bin(i, bins[i]);
        }
        let table = Shared::boxed(table);
        {
            let mut iter = NodeIter::new(table, &guard);
            let e = iter.next().unwrap();
            assert_eq!(e.key, 0);
            assert!(iter.next().is_none());
        }

        // safety: nothing holds on to references into the table any more
        let mut t = table.into_box();
        t.drop_bins();
        // no one besides this test case uses deep_table
        deep_table.into_box().drop_bins();
    }
}
