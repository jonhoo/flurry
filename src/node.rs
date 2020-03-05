use crate::raw::Table;
use core::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use parking_lot::Mutex;
use std::borrow::Borrow;
use std::thread::{current, park, Thread};

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
    pub(crate) fn as_tree_bin(&self) -> Option<&TreeBin<K, V>> {
        if let BinEntry::Tree(ref bin) = *self {
            Some(bin)
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

    /// Returns the `TreeNode` (or `Shared::null()` if not found) for the given
    /// key, starting at the given node.
    pub(crate) fn find_tree_node<'g, Q>(
        from: Shared<'g, BinEntry<K, V>>,
        hash: u64,
        key: &Q,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord,
    {
        // NOTE: in the Java code, this method is implemented on the `TreeNode`
        // instance directly, as they don't need to worry about shared pointers.
        // The Java code then uses a do-while loop here, as `self`/`this` always
        // exists so the condition below will always be satisfied on the first
        // iteration. We however _do_ have shared pointers and _do not_ have
        // do-while loops, so we end up with one extra check since we also need
        // to introduce some `continue` due to the extraction of local
        // assignments from checks.
        let mut p = from;
        while !p.is_null() {
            let p_deref = Self::get_tree_node(p);
            let p_left = p_deref.left.load(Ordering::SeqCst, guard);
            let p_right = p_deref.right.load(Ordering::SeqCst, guard);
            let p_hash = p_deref.node.hash;

            // first attempt to follow the tree order with the given hash
            if p_hash > hash {
                p = p_left;
                continue;
            }
            if p_hash < hash {
                p = p_right;
                continue;
            }

            // if the hash matches, check if the given key also matches. If so,
            // we have found the target node.
            let p_key = &p_deref.node.key;
            if p_key.borrow() == key {
                return p;
            }

            // If the key does not match, we need to descend down the tree.
            // If one of the children is empty, there is only one child to check.
            if p_left.is_null() {
                p = p_right;
                continue;
            }
            if p_right.is_null() {
                p = p_left;
                continue;
            }

            // Otherwise, we compare keys to find the next child to look at.
            let dir = match p_key.borrow().cmp(&key) {
                std::cmp::Ordering::Greater => -1,
                std::cmp::Ordering::Less => 1,
                std::cmp::Ordering::Equal => {
                    unreachable!("Ord and Eq have to match and Eq is checked above")
                }
            };
            p = if dir < 0 { p_left } else { p_right };

            // NOTE: the Java code has some addional cases here in case the keys
            // _are not_ equal (p_key != key and !key.equals(p_key)), but
            // _compare_ equal (k.compareTo(p_key) == 0). In this case, both
            // children are searched. Since `Eq` and `Ord` must match, these
            // cases cannot occur here.
        }

        return Shared::null();
    }
}

const WRITER: i64 = 1; // set while holding write lock
const WAITER: i64 = 2; // set when waiting for write lock
const READER: i64 = 4; // increment value for setting read lock

/// TreeNodes used at the heads of bins. TreeBins do not hold user keys or
/// values, but instead point to a list of TreeNodes and their root. They also
/// maintain a parasitic read-write lock forcing writers (who hold the bin lock)
/// to wait for readers (who do not) to complete before tree restructuring
/// operations.
#[derive(Debug)]
pub(crate) struct TreeBin<K, V> {
    pub(crate) root: Atomic<BinEntry<K, V>>,
    pub(crate) first: Atomic<BinEntry<K, V>>,
    pub(crate) waiter: Atomic<Thread>,
    pub(crate) lock: parking_lot::Mutex<()>,
    pub(crate) lock_state: AtomicI64,
}

impl<K, V> TreeBin<K, V>
where
    K: Ord,
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
                let p_key = &pd.node.key;
                let p_hash = pd.node.hash;
                if p_hash > hash {
                    dir = -1;
                } else if p_hash < hash {
                    dir = 1;
                } else {
                    dir = match p_key.cmp(&key) {
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
            waiter: Atomic::null(),
            lock: parking_lot::Mutex::new(()),
            lock_state: AtomicI64::new(0),
        }
    }
}

impl<K, V> TreeBin<K, V> {
    /// Acquires write lock for tree restucturing.
    fn lock_root(&self) {
        if self
            .lock_state
            .compare_and_swap(0, WRITER, Ordering::SeqCst)
            != 0
        {
            // the current lock state is non-zero, which means the lock is contended
            self.contended_lock();
        }
    }

    /// Releases write lock for tree restructuring.
    fn unlock_root(&self) {
        self.lock_state.store(0, Ordering::Release);
    }

    /// Possibly blocks awaiting root lock.
    fn contended_lock(&self) {
        let mut waiting = false;
        let mut state: i64;
        loop {
            state = self.lock_state.load(Ordering::Acquire);
            if state & !WAITER == 0 {
                // there are no writing or reading threads
                if self
                    .lock_state
                    .compare_and_swap(state, WRITER, Ordering::SeqCst)
                    == state
                {
                    // we won the race for the lock and get to return from blocking
                    if waiting {
                        self.waiter.store(Shared::null(), Ordering::SeqCst);
                    }
                    return;
                }
            } else if state & WAITER == 0 {
                // we have not indicated yet that we are waiting, so we need to
                // do that now
                if self
                    .lock_state
                    .compare_and_swap(state, state | WAITER, Ordering::SeqCst)
                    == state
                {
                    waiting = true;
                    self.waiter.store(Owned::new(current()), Ordering::SeqCst);
                }
            } else if waiting {
                park();
            }
        }
    }

    /// Returns matching node or `Shared::null()` if none. Tries to search using
    /// tree comparisons from root, but continues linear search when lock not
    /// available.
    pub(crate) fn find<'g, Q>(
        bin: Shared<'g, BinEntry<K, V>>,
        hash: u64,
        key: &Q,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Ord,
    {
        let bin_deref = bin.deref().as_tree_bin().unwrap();
        let mut element = bin_deref.first.load(Ordering::SeqCst, guard);
        while !element.is_null() {
            let s = bin_deref.lock_state.load(Ordering::SeqCst);
            if s & (WAITER | WRITER) != 0 {
                // another thread is modifying or wants to modify the tree
                // (write). As long as that's the case, we follow the `next`
                // pointers of the `TreeNode` linearly, as we cannot trust the
                // tree's structure.
                let element_deref = TreeNode::get_tree_node(element);
                let element_key = &element_deref.node.key;
                if element_deref.node.hash == hash && element_key.borrow() == key {
                    return element;
                }
                element = element_deref.node.next.load(Ordering::SeqCst, guard);
            } else if bin_deref
                .lock_state
                .compare_and_swap(s, s + READER, Ordering::SeqCst)
                == s
            {
                // the current lock state indicates no waiter or writer and we
                // acquired a read lock
                let root = bin_deref.root.load(Ordering::SeqCst, guard);
                let p = if root.is_null() {
                    Shared::null()
                } else {
                    TreeNode::find_tree_node(root, hash, key, guard)
                };
                if bin_deref.lock_state.fetch_add(-READER, Ordering::SeqCst) == (READER | WRITER) {
                    // check if another thread is waiting and, if so, unpark it
                    let waiter = &bin_deref.waiter.load(Ordering::SeqCst, guard);
                    if !waiter.is_null() {
                        waiter.deref().unpark();
                    }
                }
                return p;
            }
        }

        return Shared::null();
    }

    /// Removes the given node, which must be present before this call. This is
    /// messier than typical red-black deletion code because we cannot swap the
    /// contents of an interior node with a leaf successor that is pinned by
    /// `next` pointers that are accessible independently of the bin lock. So
    /// instead we swap the tree links.
    ///
    /// Returns `true` if the bin is now too small and should be untreeified.
    pub(crate) fn remove_tree_node<'g>(
        &'g self,
        p: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> bool {
        let p_deref = TreeNode::get_tree_node(p);
        let next = p_deref.node.next.load(Ordering::SeqCst, guard);
        let prev = p_deref.prev.load(Ordering::SeqCst, guard);
        // unlink traversal pointers
        if prev.is_null() {
            // the node to delete is the first node
            self.first.store(next, Ordering::SeqCst);
        } else {
            TreeNode::get_tree_node(prev)
                .node
                .next
                .store(next, Ordering::SeqCst);
        }
        if !next.is_null() {
            TreeNode::get_tree_node(next)
                .prev
                .store(prev, Ordering::SeqCst);
        }
        if self.first.load(Ordering::SeqCst, guard).is_null() {
            // since the bin was not empty previously (it contained p),
            // `self.first` is `null` only if we just stored `null` via `next`.
            // In that case, we have removed the last node from this bin and
            // don't have a tree anymore, so we reset `self.root`.
            self.root.store(Shared::null(), Ordering::SeqCst);
            return true;
        }

        // if we are now too small to be a `TreeBin`, we don't need to worry
        // about restructuring the tree since the bin will be untreeified
        // anyway, so we check that
        let mut root = self.root.load(Ordering::SeqCst, guard);
        if root.is_null()
            || TreeNode::get_tree_node(root)
                .right
                .load(Ordering::SeqCst, guard)
                .is_null()
        {
            return true;
        } else {
            let root_left = TreeNode::get_tree_node(root)
                .left
                .load(Ordering::SeqCst, guard);
            if root_left.is_null()
                || TreeNode::get_tree_node(root_left)
                    .left
                    .load(Ordering::SeqCst, guard)
                    .is_null()
            {
                return true;
            }
        }

        // if we get here, we know that we will still be a tree and have
        // unlinked the `next` and `prev` pointers, so it's time to restructure
        // the tree
        self.lock_root();
        let replacement;
        let p_left = p_deref.left.load(Ordering::Relaxed, guard);
        let p_right = p_deref.right.load(Ordering::Relaxed, guard);
        if !p_left.is_null() && !p_right.is_null() {
            // find the smalles successor of `p`
            let mut successor = p_right;
            let mut successor_deref = TreeNode::get_tree_node(successor);
            let mut successor_left = successor_deref.left.load(Ordering::Relaxed, guard);
            while !successor_left.is_null() {
                successor = successor_left;
                successor_deref = TreeNode::get_tree_node(successor);
                successor_left = successor_deref.left.load(Ordering::Relaxed, guard);
            }
            // swap colors
            let color = successor_deref.red.load(Ordering::Relaxed);
            successor_deref
                .red
                .store(p_deref.red.load(Ordering::Relaxed), Ordering::Relaxed);
            p_deref.red.store(color, Ordering::Relaxed);

            let successor_right = successor_deref.right.load(Ordering::Relaxed, guard);
            let p_parent = p_deref.parent.load(Ordering::Relaxed, guard);
            if successor == p_right {
                // `p` was the direct parent of the smallest successor
                p_deref.parent.store(successor, Ordering::Relaxed);
                successor_deref.right.store(p, Ordering::Relaxed);
            } else {
                let successor_parent = successor_deref.parent.load(Ordering::Relaxed, guard);
                p_deref.parent.store(successor_parent, Ordering::Relaxed);
                if !successor_parent.is_null() {
                    if successor
                        == TreeNode::get_tree_node(successor_parent)
                            .left
                            .load(Ordering::Relaxed, guard)
                    {
                        TreeNode::get_tree_node(successor_parent)
                            .left
                            .store(p, Ordering::Relaxed);
                    } else {
                        TreeNode::get_tree_node(successor_parent)
                            .right
                            .store(p, Ordering::Relaxed);
                    }
                }
                successor_deref.right.store(p_right, Ordering::Relaxed);
                if !p_right.is_null() {
                    TreeNode::get_tree_node(p_right)
                        .parent
                        .store(successor, Ordering::Relaxed);
                }
            }
            p_deref.left.store(Shared::null(), Ordering::Relaxed);
            p_deref.right.store(successor_right, Ordering::Relaxed);
            if !successor_right.is_null() {
                TreeNode::get_tree_node(successor_right)
                    .parent
                    .store(p, Ordering::Relaxed);
            }
            successor_deref.left.store(p_left, Ordering::Relaxed);
            if !p_left.is_null() {
                TreeNode::get_tree_node(p_left)
                    .parent
                    .store(successor, Ordering::Relaxed);
            }
            successor_deref.parent.store(p_parent, Ordering::Relaxed);
            if p_parent.is_null() {
                // the successor was swapped to the root as `p` was previously the root
                root = successor;
            } else if p
                == TreeNode::get_tree_node(p_parent)
                    .left
                    .load(Ordering::Relaxed, guard)
            {
                TreeNode::get_tree_node(p_parent)
                    .left
                    .store(successor, Ordering::Relaxed);
            } else {
                TreeNode::get_tree_node(p_parent)
                    .right
                    .store(successor, Ordering::Relaxed);
            }

            if !successor_right.is_null() {
                replacement = successor_right;
            } else {
                replacement = p;
            }
        } else if !p_left.is_null() {
            replacement = p_left;
        } else if !p_right.is_null() {
            replacement = p_right;
        } else {
            replacement = p;
        }

        if replacement != p {
            let p_parent = p_deref.parent.load(Ordering::Relaxed, guard);
            TreeNode::get_tree_node(replacement)
                .parent
                .store(p_parent, Ordering::Relaxed);
            if p_parent.is_null() {
                root = replacement;
            } else if p
                == TreeNode::get_tree_node(p_parent)
                    .left
                    .load(Ordering::Relaxed, guard)
            {
                TreeNode::get_tree_node(p_parent)
                    .left
                    .store(replacement, Ordering::Relaxed);
            } else {
                TreeNode::get_tree_node(p_parent)
                    .right
                    .store(replacement, Ordering::Relaxed);
            }

            p_deref.parent.store(Shared::null(), Ordering::Relaxed);
            p_deref.right.store(Shared::null(), Ordering::Relaxed);
            p_deref.left.store(Shared::null(), Ordering::Relaxed);
        }

        self.root.store(
            if p_deref.red.load(Ordering::Relaxed) {
                root
            } else {
                TreeNode::balance_deletion(root, replacement, guard)
            },
            Ordering::Relaxed,
        );

        if p == replacement {
            let p_parent = p_deref.parent.load(Ordering::Relaxed, guard);
            if !p_parent.is_null() {
                if p == TreeNode::get_tree_node(p_parent)
                    .left
                    .load(Ordering::Relaxed, guard)
                {
                    TreeNode::get_tree_node(p_parent)
                        .left
                        .store(Shared::null(), Ordering::Relaxed);
                } else if p
                    == TreeNode::get_tree_node(p_parent)
                        .right
                        .load(Ordering::Relaxed, guard)
                {
                    TreeNode::get_tree_node(p_parent)
                        .right
                        .store(Shared::null(), Ordering::Relaxed);
                }

                p_deref.parent.store(Shared::null(), Ordering::Relaxed);
            }
        }

        // mark the old node and its value for garbage collection
        guard.defer_destroy(p_deref.node.value.load(Ordering::Relaxed, guard));
        guard.defer_destroy(p);

        self.unlock_root();
        assert!(TreeNode::check_invariants(
            self.root.load(Ordering::SeqCst, guard),
            guard
        ));
        return false;
    }
}

impl<K, V> TreeBin<K, V>
where
    K: Ord,
{
    /// Finds or adds a node to the tree.
    /// If a node for the given key already exists, it is returned. Otherwise,
    /// returns `Shared::null()`.
    pub(crate) fn find_or_put_tree_val<'g>(
        &'g self,
        hash: u64,
        key: K,
        value: Shared<'g, V>,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>> {
        let mut p = self.root.load(Ordering::SeqCst, guard);
        if p.is_null() {
            // the current root is `null`, i.e. the tree is currently empty.
            // This, we simply insert the new entry as the root.
            let tree_node = Owned::new(BinEntry::TreeNode(TreeNode::new(
                key,
                Atomic::from(value),
                hash,
                Atomic::null(),
                Atomic::null(),
            )))
            .into_shared(guard);
            self.root.store(tree_node, Ordering::Release);
            self.first.store(tree_node, Ordering::Release);
            return Shared::null();
        }
        loop {
            let p_deref = TreeNode::get_tree_node(p);
            let p_hash = p_deref.node.hash;
            let dir: i8;
            if p_hash > hash {
                dir = -1;
            } else if p_hash < hash {
                dir = 1;
            } else {
                let p_key = &p_deref.node.key;
                if *p_key == key {
                    // a node with the given key already exists, so we return it
                    return p;
                }
                dir = match p_key.cmp(&key) {
                    std::cmp::Ordering::Greater => -1,
                    std::cmp::Ordering::Less => 1,
                    std::cmp::Ordering::Equal => {
                        unreachable!("Ord and Eq have to match and Eq is checked above")
                    }
                };
                // NOTE: the Java code has some addional cases here in case the
                // keys _are not_ equal (p_key != key and !key.equals(p_key)),
                // but _compare_ equal (k.compareTo(p_key) == 0). In this case,
                // both children are searched and if a matching node exists it
                // is returned. Since `Eq` and `Ord` must match, these cases
                // cannot occur here.
            }

            // proceed in direction `dir`
            let xp = p;
            p = if dir <= 0 {
                p_deref.left.load(Ordering::SeqCst, guard)
            } else {
                p_deref.right.load(Ordering::SeqCst, guard)
            };
            if p.is_null() {
                // we have reached a tree leaf, so the given key is not yet
                // contained in the tree and we can add it at the correct
                // position (which is here, since we arrived here by comparing
                // hash and key of the new entry)
                let first = self.first.load(Ordering::SeqCst, guard);
                let x = Owned::new(BinEntry::TreeNode(TreeNode::new(
                    key,
                    Atomic::from(value),
                    hash,
                    Atomic::from(first),
                    Atomic::from(xp),
                )))
                .into_shared(guard);
                self.first.store(x, Ordering::SeqCst);
                if !first.is_null() {
                    TreeNode::get_tree_node(first)
                        .prev
                        .store(x, Ordering::SeqCst);
                }
                if dir <= 0 {
                    TreeNode::get_tree_node(xp).left.store(x, Ordering::SeqCst);
                } else {
                    TreeNode::get_tree_node(xp).right.store(x, Ordering::SeqCst);
                }

                if !TreeNode::get_tree_node(xp).red.load(Ordering::SeqCst) {
                    TreeNode::get_tree_node(x).red.store(true, Ordering::SeqCst);
                } else {
                    self.lock_root();
                    self.root.store(
                        TreeNode::balance_insertion(
                            self.root.load(Ordering::Relaxed, guard),
                            x,
                            guard,
                        ),
                        Ordering::Relaxed,
                    );
                    self.unlock_root();
                }
                break;
            }
        }

        assert!(TreeNode::check_invariants(
            self.root.load(Ordering::SeqCst, guard),
            guard
        ));
        return Shared::null();
    }
}

impl<K, V> Drop for TreeBin<K, V> {
    fn drop(&mut self) {
        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references to
        // anything in the map.
        let guard = unsafe { crossbeam_epoch::unprotected() };

        // assume ownership of atomically shared references. note that it is
        // sufficient to follow the `next` pointers of the `first` element in
        // the bin, since the tree pointers point to the same nodes.
        let _waiter = unsafe { self.waiter.load(Ordering::SeqCst, guard).into_owned() };
        let p = self.first.load(Ordering::SeqCst, guard);
        if p.is_null() {
            // this TreeBin is empty
            return;
        }
        let mut p = unsafe { p.into_owned() };
        loop {
            let tree_node = if let BinEntry::TreeNode(tree_node) = *p.into_box() {
                tree_node
            } else {
                unreachable!("Trees can only ever contain TreeNodes");
            };
            // first, drop the value in this node
            let _ = unsafe { tree_node.node.value.into_owned() };
            // then we move to the next node
            if tree_node.node.next.load(Ordering::SeqCst, guard).is_null() {
                break;
            }
            p = unsafe { tree_node.node.next.into_owned() };
        }
    }
}

/* ----------------------------------------------------------------- */
// Red-black tree methods, all adapted from CLR

impl<K, V> TreeNode<K, V> {
    #[inline]
    pub(crate) fn get_tree_node<'g>(bin: Shared<'g, BinEntry<K, V>>) -> &'g TreeNode<K, V> {
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
