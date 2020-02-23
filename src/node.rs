use crate::raw::Table;
use core::sync::atomic::{AtomicBool, Ordering};
use crossbeam_epoch::{Atomic, Guard, Shared};
use parking_lot::Mutex;
use std::thread::{current, Thread};

/// Entry in a bin.
///
/// Will _generally_ be `Node`. Any entry that is not first in the bin, will be a `Node`.
#[derive(Debug)]
pub(crate) enum BinEntry<K, V> {
    Node(Node<K, V>),
    Tree(TreeBin<K, V>),
    TreeNode(TreeNode<K, V>),
    Moved,
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

    pub(crate) fn as_tree_node(&self) -> Option<&TreeNode<K, V>> {
        if let BinEntry::TreeNode(ref n) = *self {
            Some(n)
        } else {
            None
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

/* ------------------------ TreeNodes ------------------------ */

/// Nodes for use in TreeBins.
#[derive(Debug)]
pub(crate) struct TreeNode<K, V> {
    // Node properties
    pub(crate) node: Node<K, V>,

    // red-black tree links
    pub(crate) parent: Atomic<BinEntry<K, V>>,
    pub(crate) left: Atomic<BinEntry<K, V>>,
    pub(crate) right: Atomic<BinEntry<K, V>>,
    pub(crate) prev: Atomic<BinEntry<K, V>>, // needed to unlink `next` upon deletion
    pub(crate) red: AtomicBool,
}

impl<K, V> TreeNode<K, V> {
    pub fn new(
        key: K,
        value: Atomic<V>,
        hash: u64,
        next: Atomic<BinEntry<K, V>>,
        parent: Atomic<BinEntry<K, V>>,
    ) -> Self {
        TreeNode {
            node: Node {
                key: key,
                value: value,
                hash: hash,
                next: next,
                lock: parking_lot::Mutex::new(()),
            },
            parent: parent,
            left: Atomic::null(),
            right: Atomic::null(),
            prev: Atomic::null(),
            red: AtomicBool::new(false),
        }
    }
}

const WRITER: u64 = 1; // set while holding write lock
const WAITER: u64 = 2; // set when waiting for write lock
const READER: u64 = 4; // increment value for setting read lock

/// TreeNodes used at the heads of bins. TreeBins do not hold user keys or
/// values, but instead point to a list of TreeNodes and their root. They also
/// maintain a parasitic read-write lock forcing writers (who hold the bin lock)
/// to wait for readers (who do not) to complete before tree restructuring
/// operations.
#[derive(Debug)]
pub(crate) struct TreeBin<K, V> {
    pub(crate) root: Atomic<BinEntry<K, V>>,
    pub(crate) first: Atomic<BinEntry<K, V>>,
    pub(crate) waiter: Option<Thread>,
    pub(crate) lock_state: Atomic<u64>,
}

impl<K, V> TreeBin<K, V>
where
    K: Ord + PartialOrd,
{
    pub fn new<'g>(bin: Shared<'g, BinEntry<K, V>>, guard: &'g Guard) -> Self {
        let mut root: Shared<'_, BinEntry<K, V>> = Shared::null();
        let mut x = bin;
        let mut next: Shared<'_, BinEntry<K, V>>;

        while !x.is_null() {
            let x_deref = TreeNode::get_tree_node(x);
            next = x_deref.node.next.load(Ordering::SeqCst, guard);
            x_deref.left.store(Shared::null(), Ordering::Relaxed);
            x_deref.right.store(Shared::null(), Ordering::Relaxed);

            // if there is no root yet, make x the root
            if root.is_null() {
                x_deref.parent.store(Shared::null(), Ordering::Relaxed);
                x_deref.red.store(false, Ordering::Relaxed);
                root = x;
                x = next;
                continue;
            }

            let key = &x_deref.node.key;
            let hash = x_deref.node.hash;

            // Traverse the tree that was constructed so far from the root to
            // find out where to insert x
            let mut p = root;
            loop {
                let dir: i8;
                let pd = TreeNode::get_tree_node(p);
                let pk = &pd.node.key;
                let ph = pd.node.hash;
                if ph > hash {
                    dir = -1;
                } else if ph < hash {
                    dir = 1;
                } else {
                    dir = match pk.cmp(&key) {
                        std::cmp::Ordering::Greater => -1,
                        std::cmp::Ordering::Less => 1,
                        std::cmp::Ordering::Equal => unreachable!("one key references two nodes"),
                    }
                }

                // Select successor of p in the direction dir. We will continue
                // to descend the tree through this successor.
                let xp = p;
                p = if dir <= 0 {
                    pd.left.load(Ordering::SeqCst, guard)
                } else {
                    pd.right.load(Ordering::SeqCst, guard)
                };

                if p.is_null() {
                    x_deref.parent.store(xp, Ordering::SeqCst);
                    if dir <= 0 {
                        TreeNode::get_tree_node(xp).left.store(x, Ordering::SeqCst);
                    } else {
                        TreeNode::get_tree_node(xp).right.store(x, Ordering::SeqCst);
                    }
                    root = TreeNode::balance_insertion(root, x, guard);
                    break;
                }
            }

            x = next;
        }

        assert!(TreeNode::check_invariants(root, guard));
        TreeBin {
            root: Atomic::from(root),
            first: Atomic::from(bin),
            waiter: None,
            lock_state: Atomic::new(0),
        }
    }
}

/* ----------------------------------------------------------------- */
// Red-black tree methods, all adapted from CLR

impl<K, V> TreeNode<K, V> {
    #[inline]
    fn get_tree_node<'g>(bin: Shared<'g, BinEntry<K, V>>) -> &'g TreeNode<K, V> {
        bin.deref().as_tree_node().unwrap()
    }

    fn rotate_left<'g>(
        mut root: Shared<'g, BinEntry<K, V>>,
        p: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>> {
        if p.is_null() {
            return root;
        }
        let p_deref = Self::get_tree_node(p);
        let right = p_deref.right.load(Ordering::SeqCst, guard);
        if !right.is_null() {
            let right_deref = Self::get_tree_node(right);
            let right_left = right_deref.left.load(Ordering::SeqCst, guard);
            p_deref.right.store(right_left, Ordering::SeqCst);
            if !right_left.is_null() {
                Self::get_tree_node(right_left)
                    .parent
                    .store(p, Ordering::SeqCst);
            }

            let p_parent = p_deref.parent.load(Ordering::SeqCst, guard);
            right_deref.parent.store(p_parent, Ordering::SeqCst);
            if p_parent.is_null() {
                root = right;
                right_deref.red.store(false, Ordering::Relaxed);
            } else {
                let p_parent_deref = Self::get_tree_node(p_parent);
                if p_parent_deref.left.load(Ordering::SeqCst, guard) == p {
                    p_parent_deref.left.store(right, Ordering::SeqCst);
                } else {
                    p_parent_deref.right.store(right, Ordering::SeqCst);
                }
            }

            right_deref.left.store(p, Ordering::SeqCst);
            p_deref.parent.store(right, Ordering::SeqCst);
        }

        root
    }

    fn rotate_right<'g>(
        mut root: Shared<'g, BinEntry<K, V>>,
        p: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>> {
        if p.is_null() {
            return root;
        }
        let p_deref = Self::get_tree_node(p);
        let left = p_deref.left.load(Ordering::SeqCst, guard);
        if !left.is_null() {
            let left_deref = Self::get_tree_node(left);
            let left_right = left_deref.right.load(Ordering::SeqCst, guard);
            p_deref.left.store(left_right, Ordering::SeqCst);
            if !left_right.is_null() {
                Self::get_tree_node(left_right)
                    .parent
                    .store(p, Ordering::SeqCst);
            }

            let p_parent = p_deref.parent.load(Ordering::SeqCst, guard);
            left_deref.parent.store(p_parent, Ordering::SeqCst);
            if p_parent.is_null() {
                root = left;
                left_deref.red.store(false, Ordering::Relaxed);
            } else {
                let p_parent_deref = Self::get_tree_node(p_parent);
                if p_parent_deref.right.load(Ordering::SeqCst, guard) == p {
                    p_parent_deref.right.store(left, Ordering::SeqCst);
                } else {
                    p_parent_deref.left.store(left, Ordering::SeqCst);
                }
            }

            left_deref.right.store(p, Ordering::SeqCst);
            p_deref.parent.store(left, Ordering::SeqCst);
        }

        root
    }

    fn balance_insertion<'g>(
        mut root: Shared<'g, BinEntry<K, V>>,
        mut x: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>> {
        Self::get_tree_node(x).red.store(true, Ordering::Relaxed);

        let mut x_parent: Shared<'_, BinEntry<K, V>>;
        let mut x_parent_parent: Shared<'_, BinEntry<K, V>>;
        let mut x_parent_parent_left: Shared<'_, BinEntry<K, V>>;
        let mut x_parent_parent_right: Shared<'_, BinEntry<K, V>>;
        loop {
            x_parent = Self::get_tree_node(x).parent.load(Ordering::SeqCst, guard);
            if x_parent.is_null() {
                Self::get_tree_node(x).red.store(false, Ordering::Relaxed);
                return x;
            }
            x_parent_parent = Self::get_tree_node(x_parent)
                .parent
                .load(Ordering::SeqCst, guard);
            if !Self::get_tree_node(x_parent).red.load(Ordering::Relaxed)
                || x_parent_parent.is_null()
            {
                return root;
            }
            x_parent_parent_left = Self::get_tree_node(x_parent_parent)
                .left
                .load(Ordering::SeqCst, guard);
            if x_parent == x_parent_parent_left {
                x_parent_parent_right = Self::get_tree_node(x_parent_parent)
                    .right
                    .load(Ordering::SeqCst, guard);
                if !x_parent_parent_right.is_null()
                    && Self::get_tree_node(x_parent_parent_right)
                        .red
                        .load(Ordering::Relaxed)
                {
                    Self::get_tree_node(x_parent_parent_right)
                        .red
                        .store(false, Ordering::Relaxed);
                    Self::get_tree_node(x_parent)
                        .red
                        .store(false, Ordering::Relaxed);
                    Self::get_tree_node(x_parent_parent)
                        .red
                        .store(true, Ordering::Relaxed);
                    x = x_parent_parent;
                } else {
                    if x == Self::get_tree_node(x_parent)
                        .right
                        .load(Ordering::SeqCst, guard)
                    {
                        x = x_parent;
                        root = Self::rotate_left(root, x, guard);
                        x_parent = Self::get_tree_node(x).parent.load(Ordering::SeqCst, guard);
                        x_parent_parent = if x_parent.is_null() {
                            Shared::null()
                        } else {
                            Self::get_tree_node(x_parent)
                                .parent
                                .load(Ordering::SeqCst, guard)
                        };
                    }
                    if !x_parent.is_null() {
                        Self::get_tree_node(x_parent)
                            .red
                            .store(false, Ordering::Relaxed);
                        if !x_parent_parent.is_null() {
                            Self::get_tree_node(x_parent_parent)
                                .red
                                .store(true, Ordering::Relaxed);
                            root = Self::rotate_right(root, x_parent_parent, guard);
                        }
                    }
                }
            } else if !x_parent_parent_left.is_null()
                && Self::get_tree_node(x_parent_parent_left)
                    .red
                    .load(Ordering::Relaxed)
            {
                Self::get_tree_node(x_parent_parent_left)
                    .red
                    .store(false, Ordering::Relaxed);
                Self::get_tree_node(x_parent)
                    .red
                    .store(false, Ordering::Relaxed);
                Self::get_tree_node(x_parent_parent)
                    .red
                    .store(true, Ordering::Relaxed);
                x = x_parent_parent;
            } else {
                if x == Self::get_tree_node(x_parent)
                    .left
                    .load(Ordering::SeqCst, guard)
                {
                    x = x_parent;
                    root = Self::rotate_right(root, x, guard);
                    x_parent = Self::get_tree_node(x).parent.load(Ordering::SeqCst, guard);
                    x_parent_parent = if x_parent.is_null() {
                        Shared::null()
                    } else {
                        Self::get_tree_node(x_parent)
                            .parent
                            .load(Ordering::SeqCst, guard)
                    };
                }
                if !x_parent.is_null() {
                    Self::get_tree_node(x_parent)
                        .red
                        .store(false, Ordering::Relaxed);
                    if !x_parent_parent.is_null() {
                        Self::get_tree_node(x_parent_parent)
                            .red
                            .store(true, Ordering::Relaxed);
                        root = Self::rotate_left(root, x_parent_parent, guard);
                    }
                }
            }
        }
    }

    fn balance_deletion<'g>(
        mut root: Shared<'g, BinEntry<K, V>>,
        mut x: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>> {
        let mut x_parent: Shared<'_, BinEntry<K, V>>;
        let mut x_parent_left: Shared<'_, BinEntry<K, V>>;
        let mut x_parent_right: Shared<'_, BinEntry<K, V>>;
        loop {
            if x.is_null() || x == root {
                return root;
            }
            x_parent = Self::get_tree_node(x).parent.load(Ordering::SeqCst, guard);
            if x_parent.is_null() {
                Self::get_tree_node(x).red.store(false, Ordering::Relaxed);
                return x;
            }
            x_parent_left = Self::get_tree_node(x_parent)
                .left
                .load(Ordering::SeqCst, guard);
            if x_parent_left == x {
                x_parent_right = Self::get_tree_node(x_parent)
                    .right
                    .load(Ordering::SeqCst, guard);
                if !x_parent_right.is_null()
                    && Self::get_tree_node(x_parent_right)
                        .red
                        .load(Ordering::Relaxed)
                {
                    Self::get_tree_node(x_parent_right)
                        .red
                        .store(false, Ordering::Relaxed);
                    Self::get_tree_node(x_parent)
                        .red
                        .store(true, Ordering::Relaxed);
                    root = Self::rotate_left(root, x_parent, guard);
                    x_parent = Self::get_tree_node(x).parent.load(Ordering::SeqCst, guard);
                    x_parent_right = if x_parent.is_null() {
                        Shared::null()
                    } else {
                        Self::get_tree_node(x_parent)
                            .right
                            .load(Ordering::SeqCst, guard)
                    };
                }
                if x_parent_right.is_null() {
                    x = x_parent;
                } else {
                    let s_left = Self::get_tree_node(x_parent_right)
                        .left
                        .load(Ordering::SeqCst, guard);
                    let mut s_right = Self::get_tree_node(x_parent_right)
                        .right
                        .load(Ordering::SeqCst, guard);

                    if (s_right.is_null()
                        || !Self::get_tree_node(s_right).red.load(Ordering::Relaxed))
                        && (s_left.is_null()
                            || !Self::get_tree_node(s_left).red.load(Ordering::Relaxed))
                    {
                        Self::get_tree_node(x_parent_right)
                            .red
                            .store(true, Ordering::Relaxed);
                        x = x_parent;
                    } else {
                        if s_right.is_null()
                            || !Self::get_tree_node(s_right).red.load(Ordering::Relaxed)
                        {
                            if !s_left.is_null() {
                                Self::get_tree_node(s_left)
                                    .red
                                    .store(false, Ordering::Relaxed);
                            }
                            Self::get_tree_node(x_parent_right)
                                .red
                                .store(true, Ordering::Relaxed);
                            root = Self::rotate_right(root, x_parent_right, guard);
                            x_parent = Self::get_tree_node(x).parent.load(Ordering::SeqCst, guard);
                            x_parent_right = if x_parent.is_null() {
                                Shared::null()
                            } else {
                                Self::get_tree_node(x_parent)
                                    .right
                                    .load(Ordering::SeqCst, guard)
                            };
                        }
                        if !x_parent_right.is_null() {
                            Self::get_tree_node(x_parent_right).red.store(
                                if x_parent.is_null() {
                                    false
                                } else {
                                    Self::get_tree_node(x_parent).red.load(Ordering::Relaxed)
                                },
                                Ordering::Relaxed,
                            );
                            s_right = Self::get_tree_node(x_parent_right)
                                .right
                                .load(Ordering::SeqCst, guard);
                            if !s_right.is_null() {
                                Self::get_tree_node(s_right)
                                    .red
                                    .store(false, Ordering::Relaxed);
                            }
                        }
                        if !x_parent.is_null() {
                            Self::get_tree_node(x_parent)
                                .red
                                .store(false, Ordering::Relaxed);
                            root = Self::rotate_left(root, x_parent, guard);
                        }
                        x = root;
                    }
                }
            } else {
                // symmetric
                if !x_parent_left.is_null()
                    && Self::get_tree_node(x_parent_left)
                        .red
                        .load(Ordering::Relaxed)
                {
                    Self::get_tree_node(x_parent_left)
                        .red
                        .store(false, Ordering::Relaxed);
                    Self::get_tree_node(x_parent)
                        .red
                        .store(true, Ordering::Relaxed);
                    root = Self::rotate_right(root, x_parent, guard);
                    x_parent = Self::get_tree_node(x).parent.load(Ordering::SeqCst, guard);
                    x_parent_left = if x_parent.is_null() {
                        Shared::null()
                    } else {
                        Self::get_tree_node(x_parent)
                            .left
                            .load(Ordering::SeqCst, guard)
                    };
                }
                if x_parent_left.is_null() {
                    x = x_parent;
                } else {
                    let mut s_left = Self::get_tree_node(x_parent_left)
                        .left
                        .load(Ordering::SeqCst, guard);
                    let s_right = Self::get_tree_node(x_parent_left)
                        .right
                        .load(Ordering::SeqCst, guard);

                    if (s_right.is_null()
                        || !Self::get_tree_node(s_right).red.load(Ordering::Relaxed))
                        && (s_left.is_null()
                            || !Self::get_tree_node(s_left).red.load(Ordering::Relaxed))
                    {
                        Self::get_tree_node(x_parent_left)
                            .red
                            .store(true, Ordering::Relaxed);
                        x = x_parent;
                    } else {
                        if s_left.is_null()
                            || !Self::get_tree_node(s_left).red.load(Ordering::Relaxed)
                        {
                            if !s_right.is_null() {
                                Self::get_tree_node(s_right)
                                    .red
                                    .store(false, Ordering::Relaxed);
                            }
                            Self::get_tree_node(x_parent_left)
                                .red
                                .store(true, Ordering::Relaxed);
                            root = Self::rotate_left(root, x_parent_left, guard);
                            x_parent = Self::get_tree_node(x).parent.load(Ordering::SeqCst, guard);
                            x_parent_left = if x_parent.is_null() {
                                Shared::null()
                            } else {
                                Self::get_tree_node(x_parent)
                                    .left
                                    .load(Ordering::SeqCst, guard)
                            };
                        }
                        if !x_parent_left.is_null() {
                            Self::get_tree_node(x_parent_left).red.store(
                                if x_parent.is_null() {
                                    false
                                } else {
                                    Self::get_tree_node(x_parent).red.load(Ordering::Relaxed)
                                },
                                Ordering::Relaxed,
                            );
                            s_left = Self::get_tree_node(x_parent_left)
                                .left
                                .load(Ordering::SeqCst, guard);
                            if !s_left.is_null() {
                                Self::get_tree_node(s_left)
                                    .red
                                    .store(false, Ordering::Relaxed);
                            }
                        }
                        if !x_parent.is_null() {
                            Self::get_tree_node(x_parent)
                                .red
                                .store(false, Ordering::Relaxed);
                            root = Self::rotate_right(root, x_parent, guard);
                        }
                        x = root;
                    }
                }
            }
        }
    }
    /// Checks invariants recursively for the tree of Nodes rootet at t.
    fn check_invariants<'g>(t: Shared<'g, BinEntry<K, V>>, guard: &'g Guard) -> bool {
        let t_deref = Self::get_tree_node(t);
        let t_parent = t_deref.parent.load(Ordering::SeqCst, guard);
        let t_left = t_deref.left.load(Ordering::SeqCst, guard);
        let t_right = t_deref.right.load(Ordering::SeqCst, guard);
        let t_back = t_deref.prev.load(Ordering::SeqCst, guard);
        let t_next = t_deref.node.next.load(Ordering::SeqCst, guard);

        if !t_back.is_null() {
            let t_back_deref = Self::get_tree_node(t_back);
            if t_back_deref.node.next.load(Ordering::SeqCst, guard) != t {
                return false;
            }
        }
        if !t_next.is_null() {
            let t_next_deref = Self::get_tree_node(t_next);
            if t_next_deref.prev.load(Ordering::SeqCst, guard) != t {
                return false;
            }
        }
        if !t_parent.is_null() {
            let t_parent_deref = Self::get_tree_node(t_parent);
            if t_parent_deref.left.load(Ordering::SeqCst, guard) != t
                && t_parent_deref.right.load(Ordering::SeqCst, guard) != t
            {
                return false;
            }
        }
        if !t_left.is_null() {
            let t_left_deref = Self::get_tree_node(t_left);
            if t_left_deref.parent.load(Ordering::SeqCst, guard) != t
                || t_left_deref.node.hash > t_deref.node.hash
            {
                return false;
            }
        }
        if !t_right.is_null() {
            let t_right_deref = Self::get_tree_node(t_right);
            if t_right_deref.parent.load(Ordering::SeqCst, guard) != t
                || t_right_deref.node.hash < t_deref.node.hash
            {
                return false;
            }
        }
        if t_deref.red.load(Ordering::Relaxed) && !t_left.is_null() && !t_right.is_null() {
            let t_left_deref = Self::get_tree_node(t_left);
            let t_right_deref = Self::get_tree_node(t_right);
            if t_left_deref.red.load(Ordering::Relaxed) && t_right_deref.red.load(Ordering::Relaxed)
            {
                return false;
            }
        }
        if !t_left.is_null() && !Self::check_invariants(t_left, guard) {
            false
        } else if !t_right.is_null() && !Self::check_invariants(t_right, guard) {
            false
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_epoch::Owned;
    use std::sync::atomic::Ordering;

    fn new_node(hash: u64, key: usize, value: usize) -> Node<usize, usize> {
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
        let mut table = Table::<usize, usize>::new(1);
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
        let mut table = Table::<usize, usize>::new(1);
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
        let mut table = Table::<usize, usize>::new(1);
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
        let mut table = Table::<usize, usize>::new(1);
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
