use crate::raw::Table;
use core::sync::atomic::{spin_loop_hint, AtomicBool, AtomicI64, Ordering};
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

impl<K, V> Node<K, V> {
    pub(crate) fn new<AV>(hash: u64, key: K, value: AV) -> Self
    where
        AV: Into<Atomic<V>>,
    {
        Node::with_next(hash, key, value, Atomic::null())
    }
    pub(crate) fn with_next<AV>(hash: u64, key: K, value: AV, next: Atomic<BinEntry<K, V>>) -> Self
    where
        AV: Into<Atomic<V>>,
    {
        Node {
            hash,
            key,
            value: value.into(),
            next,
            lock: Mutex::new(()),
        }
    }
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
    /// Constructs a new TreeNode with the given attributes to be inserted into a TreeBin.
    ///
    /// This does yet not arrange this node and its `next` nodes into a tree, since the tree
    /// structure is maintained globally by the TreeBin.
    pub(crate) fn new(
        hash: u64,
        key: K,
        value: Atomic<V>,
        next: Atomic<BinEntry<K, V>>,
        parent: Atomic<BinEntry<K, V>>,
    ) -> Self {
        TreeNode {
            node: Node::with_next(hash, key, value, next),
            parent,
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
            // safety: the containing TreeBin of all TreeNodes was read under our
            // guard, at which point the tree structure was valid. Since our guard
            // pins the current epoch, the TreeNodes remain valid for at least as
            // long as we hold onto the guard.
            // Structurally, TreeNodes always point to TreeNodes, so this is sound.
            let p_deref = unsafe { Self::get_tree_node(p) };
            let p_hash = p_deref.node.hash;

            // first attempt to follow the tree order with the given hash
            match p_hash.cmp(&hash) {
                std::cmp::Ordering::Greater => {
                    p = p_deref.left.load(Ordering::SeqCst, guard);
                    continue;
                }
                std::cmp::Ordering::Less => {
                    p = p_deref.right.load(Ordering::SeqCst, guard);
                    continue;
                }
                _ => {}
            }

            // if the hash matches, check if the given key also matches. If so,
            // we have found the target node.
            let p_key = &p_deref.node.key;
            if p_key.borrow() == key {
                return p;
            }

            // If the key does not match, we need to descend down the tree.
            let p_left = p_deref.left.load(Ordering::SeqCst, guard);
            let p_right = p_deref.right.load(Ordering::SeqCst, guard);
            // If one of the children is empty, there is only one child to check.
            if p_left.is_null() {
                p = p_right;
                continue;
            } else if p_right.is_null() {
                p = p_left;
                continue;
            }

            // Otherwise, we compare keys to find the next child to look at.
            p = match p_key.borrow().cmp(&key) {
                std::cmp::Ordering::Greater => p_left,
                std::cmp::Ordering::Less => p_right,
                std::cmp::Ordering::Equal => {
                    unreachable!("Ord and Eq have to match and Eq is checked above")
                }
            };
            // NOTE: the Java code has some addional cases here in case the keys
            // _are not_ equal (p_key != key and !key.equals(p_key)), but
            // _compare_ equal (k.compareTo(p_key) == 0). In this case, both
            // children are searched. Since `Eq` and `Ord` must match, these
            // cases cannot occur here.
        }

        Shared::null()
    }
}

const WRITER: i64 = 1; // set while holding write lock
const WAITER: i64 = 2; // set when waiting for write lock
const READER: i64 = 4; // increment value for setting read lock

/// Private representation for movement direction along tree successors.
enum Dir {
    Left,
    Right,
}

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
    /// Constructs a new bin from the given nodes.
    ///
    /// Nodes are arranged into an ordered red-black tree.
    pub(crate) fn new(bin: Owned<BinEntry<K, V>>, guard: &Guard) -> Self {
        let mut root = Shared::null();
        let bin = bin.into_shared(guard);

        // safety: We own the nodes for creating this new TreeBin, so they are
        // not shared with another thread and cannot get invalidated.
        // Structurally, TreeNodes always point to TreeNodes, so this is sound.
        let mut x = bin;
        while !x.is_null() {
            let x_deref = unsafe { TreeNode::get_tree_node(x) };
            let next = x_deref.node.next.load(Ordering::Relaxed, guard);
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
                let p_deref = unsafe { TreeNode::get_tree_node(p) };
                let p_key = &p_deref.node.key;
                let p_hash = p_deref.node.hash;

                // Select successor of p in the correct direction. We will continue
                // to descend the tree through this successor.
                let xp = p;
                let dir;
                p = match p_hash.cmp(&hash).then(p_key.cmp(&key)) {
                    std::cmp::Ordering::Greater => {
                        dir = Dir::Left;
                        &p_deref.left
                    }
                    std::cmp::Ordering::Less => {
                        dir = Dir::Right;
                        &p_deref.right
                    }
                    std::cmp::Ordering::Equal => unreachable!("one key references two nodes"),
                }
                .load(Ordering::Relaxed, guard);

                if p.is_null() {
                    x_deref.parent.store(xp, Ordering::Relaxed);
                    match dir {
                        Dir::Left => {
                            unsafe { TreeNode::get_tree_node(xp) }
                                .left
                                .store(x, Ordering::Relaxed);
                        }
                        Dir::Right => {
                            unsafe { TreeNode::get_tree_node(xp) }
                                .right
                                .store(x, Ordering::Relaxed);
                        }
                    }
                    root = TreeNode::balance_insertion(root, x, guard);
                    break;
                }
            }

            x = next;
        }

        if cfg!(debug_assertions) {
            TreeNode::check_invariants(root, guard);
        }
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
    fn lock_root(&self, guard: &Guard) {
        if self
            .lock_state
            .compare_and_swap(0, WRITER, Ordering::SeqCst)
            != 0
        {
            // the current lock state is non-zero, which means the lock is contended
            self.contended_lock(guard);
        }
    }

    /// Releases write lock for tree restructuring.
    fn unlock_root(&self) {
        self.lock_state.store(0, Ordering::Release);
    }

    /// Possibly blocks awaiting root lock.
    fn contended_lock(&self, guard: &Guard) {
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
                        let waiter = self.waiter.swap(Shared::null(), Ordering::SeqCst, guard);
                        // safety: we are the only thread that modifies the
                        // `waiter` thread handle (reading threads only use it
                        // to notify us). Thus, having stored a valid value
                        // below, `waiter` is a valid pointer.
                        // The reading thread that notifies us does so as its
                        // last action in `find` and then lets go of the
                        // reference immediately. _New_ reading threads already
                        // take the slow path since we are `WAITING`, so they do
                        // not obtain new references to our thread handle. Also,
                        // we just swapped out that handle, so it is no longer
                        // reachable.
                        //
                        // Now, we cannot safely drop this _immediately_, since
                        // we may have woken up and reached here _while_ some
                        // was trying to wake us up, so we defer_destroy instead.
                        unsafe { guard.defer_destroy(waiter) };
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
                    let current_thread = Owned::new(current());
                    let waiter = self.waiter.swap(current_thread, Ordering::SeqCst, guard);
                    assert!(waiter.is_null());
                }
            } else if waiting {
                park();
            }
            spin_loop_hint();
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
        // safety: bin is a valid pointer.
        //
        // there are three cases when a bin pointer is invalidated:
        //
        //  1. if the table was resized, bin is a move entry, and the resize has completed. in
        //     that case, the table (and all its heads) will be dropped in the next epoch
        //     following that.
        //  2. if the table is being resized, bin may be swapped with a move entry. the old bin
        //     will then be dropped in the following epoch after that happens.
        //  3. when elements are inserted into or removed from the map, bin may be changed into
        //     or from a TreeBin from or into a regular, linear bin. the old bin will also be
        //     dropped in the following epoch if that happens.
        //
        // in all cases, we held the guard when we got the reference to the bin. if any such
        // swap happened, it must have happened _after_ we read. since we did the read while
        // pinning the epoch, the drop must happen in the _next_ epoch (i.e., the one that we
        // are holding up by holding on to our guard).
        let bin_deref = unsafe { bin.deref() }.as_tree_bin().unwrap();
        let mut element = bin_deref.first.load(Ordering::SeqCst, guard);
        while !element.is_null() {
            let s = bin_deref.lock_state.load(Ordering::SeqCst);
            if s & (WAITER | WRITER) != 0 {
                // another thread is modifying or wants to modify the tree
                // (write). As long as that's the case, we follow the `next`
                // pointers of the `TreeNode` linearly, as we cannot trust the
                // tree's structure.
                //
                // safety: we were read under our guard, at which point the tree
                // structure was valid. Since our guard pins the current epoch,
                // the TreeNodes remain valid for at least as long as we hold
                // onto the guard.
                // Structurally, TreeNodes always point to TreeNodes, so this is sound.
                let element_deref = unsafe { TreeNode::get_tree_node(element) };
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
                if bin_deref.lock_state.fetch_add(-READER, Ordering::SeqCst) == (READER | WAITER) {
                    // we were the last reader holding up a waiting writer, so
                    // we unpark the waiting writer by granting it a token
                    let waiter = &bin_deref.waiter.load(Ordering::SeqCst, guard);
                    if !waiter.is_null() {
                        // safety: thread handles are only dropped by the thread
                        // they represent _after_ it acquires the write lock.
                        // Since the thread behind the `waiter` handle is
                        // currently _waiting_ on said lock, the handle will not
                        // yet be dropped.
                        unsafe { waiter.deref() }.unpark();
                    }
                }
                return p;
            }
        }

        Shared::null()
    }

    /// Unlinks the given node, which must be present before this call.
    ///
    /// This is messier than typical red-black deletion code because we cannot
    /// swap the contents of an interior node with a leaf successor that is
    /// pinned by `next` pointers that are accessible independently of the bin
    /// lock. So instead we swap the tree links.
    ///
    /// Returns `true` if the bin is now too small and should be untreeified.
    ///
    /// # Safety
    /// The given node is only marked for garbage collection if the bin remains
    /// large enough to be a `TreeBin`. If this method returns `true`, indicating
    /// that the bin should be untreeified, the given node is only unlinked from
    /// linear traversal, but not from the tree. This makes the node unreachable
    /// through linear reads and excludes it from being dropped when the bin is
    /// dropped. However, reading threads may still obtain a reference to until
    /// the bin is swapped out for a linear bin.
    ///
    /// The caller of this method _must_ ensure that the given node is properly
    /// marked for garbage collection _after_ this bin has been swapped out. If
    /// the value of the given node was supposed to get dropped as well
    /// (`drop_value` was true), the caller must do the same for the value.
    pub(crate) unsafe fn remove_tree_node<'g>(
        &'g self,
        p: Shared<'g, BinEntry<K, V>>,
        drop_value: bool,
        guard: &'g Guard,
    ) -> bool {
        // safety: we were read under our guard, at which point the tree
        // structure was valid. Since our guard pins the current epoch, the
        // TreeNodes remain valid for at least as long as we hold onto the
        // guard. Additionally, this method assumes `p` to be non-null.
        // Structurally, TreeNodes always point to TreeNodes, so this is sound.
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
        // TODO: Can `root` even be `null`?
        // The Java code has `NULL` checks for this, but in theory it should not be possible to
        // encounter a tree that has no root when we have its lock. It should always have at
        // least `UNTREEIFY_THRESHOLD` nodes. If it is indeed impossible we should replace
        // this with an assertion instead.
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
        self.lock_root(guard);
        // NOTE: since we have the write lock for the tree, we know that all
        // readers will read along the linear `next` pointers until we release
        // the lock (these pointers were adjusted above to exclude the removed
        // node and are synchronized as `SeqCst`). This means that we can
        // operate on the _other_ pointers of tree nodes that represent the tree
        // structure using a `Relaxed` ordering. The release of the write lock
        // will then synchronize with later readers who will see the new values.
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
                // `p` was the direct parent of the smallest successor.
                // the two nodes will be swapped
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
            debug_assert!(successor_left.is_null());
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

            // We have swapped `p` with `successor`, which is the next element
            // after `p` in `(hash, key)` order (the smallest element larger
            // than `p`). To actually remove `p`, we need to check if
            // `successor` has a right child (it cannot have a left child, as
            // otherwise _it_ would be the `successor`). If not, we can just
            // directly unlink `p`, since it is now a leaf. Otherwise, we have
            // to replace it with the right child of `successor` (which is now
            // also its right child), which preserves the ordering.
            if !successor_right.is_null() {
                replacement = successor_right;
            } else {
                replacement = p;
            }
        } else if !p_left.is_null() {
            // If `p` only has a left child, just replacing `p` with that child preserves the ordering.
            replacement = p_left;
        } else if !p_right.is_null() {
            // Symmetrically, we can use its right child.
            replacement = p_right;
        } else {
            // If `p` is _already_ a leaf, we can also just unlink it.
            replacement = p;
        }

        if replacement != p {
            // `p` (at its potentially new position) has a child, so we need to do a replacement.
            let p_parent = p_deref.parent.load(Ordering::Relaxed, guard);
            TreeNode::get_tree_node(replacement)
                .parent
                .store(p_parent, Ordering::Relaxed);
            if p_parent.is_null() {
                root = replacement;
            } else {
                let p_parent_deref = TreeNode::get_tree_node(p_parent);
                if p == p_parent_deref.left.load(Ordering::Relaxed, guard) {
                    p_parent_deref.left.store(replacement, Ordering::Relaxed);
                } else {
                    p_parent_deref.right.store(replacement, Ordering::Relaxed);
                }
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
            // `p` does _not_ have children, so we unlink it as a leaf.
            let p_parent = p_deref.parent.load(Ordering::Relaxed, guard);
            if !p_parent.is_null() {
                let p_parent_deref = TreeNode::get_tree_node(p_parent);
                if p == p_parent_deref.left.load(Ordering::Relaxed, guard) {
                    TreeNode::get_tree_node(p_parent)
                        .left
                        .store(Shared::null(), Ordering::Relaxed);
                } else if p == p_parent_deref.right.load(Ordering::Relaxed, guard) {
                    p_parent_deref
                        .right
                        .store(Shared::null(), Ordering::Relaxed);
                }
                p_deref.parent.store(Shared::null(), Ordering::Relaxed);
                debug_assert!(p_deref.left.load(Ordering::Relaxed, guard).is_null());
                debug_assert!(p_deref.right.load(Ordering::Relaxed, guard).is_null());
            }
        }
        self.unlock_root();

        // mark the old node and its value for garbage collection
        // safety: we just completely unlinked `p` from both linear and tree
        // traversal, making it and its value unreachable for any future thread.
        // Any existing references to one of them were obtained under a guard
        // that pins an epoch <= our epoch, and thus have to be released before
        // `p` is actually dropped.
        #[allow(unused_unsafe)]
        unsafe {
            if drop_value {
                guard.defer_destroy(p_deref.node.value.load(Ordering::Relaxed, guard));
            }
            guard.defer_destroy(p);
        }

        if cfg!(debug_assertions) {
            TreeNode::check_invariants(self.root.load(Ordering::SeqCst, guard), guard);
        }
        false
    }
}

impl<K, V> TreeBin<K, V>
where
    K: Ord + Send + Sync,
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
                hash,
                key,
                Atomic::from(value),
                Atomic::null(),
                Atomic::null(),
            )))
            .into_shared(guard);
            self.root.store(tree_node, Ordering::Release);
            self.first.store(tree_node, Ordering::Release);
            return Shared::null();
        }
        // safety: we were read under our guard, at which point the tree
        // structure was valid. Since our guard pins the current epoch, the
        // TreeNodes remain valid for at least as long as we hold onto the
        // guard.
        // Structurally, TreeNodes always point to TreeNodes, so this is sound.
        loop {
            let p_deref = unsafe { TreeNode::get_tree_node(p) };
            let p_hash = p_deref.node.hash;
            let xp = p;
            let dir;
            p = match p_hash.cmp(&hash) {
                std::cmp::Ordering::Greater => {
                    dir = Dir::Left;
                    &p_deref.left
                }
                std::cmp::Ordering::Less => {
                    dir = Dir::Right;
                    &p_deref.right
                }
                std::cmp::Ordering::Equal => {
                    let p_key = &p_deref.node.key;
                    if *p_key == key {
                        // a node with the given key already exists, so we return it
                        return p;
                    }
                    match p_key.cmp(&key) {
                        std::cmp::Ordering::Greater => {
                            dir = Dir::Left;
                            &p_deref.left
                        }
                        std::cmp::Ordering::Less => {
                            dir = Dir::Right;
                            &p_deref.right
                        }
                        std::cmp::Ordering::Equal => {
                            unreachable!("Ord and Eq have to match and Eq is checked above")
                        }
                    }
                    // NOTE: the Java code has some addional cases here in case the
                    // keys _are not_ equal (p_key != key and !key.equals(p_key)),
                    // but _compare_ equal (k.compareTo(p_key) == 0). In this case,
                    // both children are searched and if a matching node exists it
                    // is returned. Since `Eq` and `Ord` must match, these cases
                    // cannot occur here.
                }
            }
            .load(Ordering::SeqCst, guard);

            if p.is_null() {
                // we have reached a tree leaf, so the given key is not yet
                // contained in the tree and we can add it at the correct
                // position (which is here, since we arrived here by comparing
                // hash and key of the new entry)
                let first = self.first.load(Ordering::SeqCst, guard);
                let x = Owned::new(BinEntry::TreeNode(TreeNode::new(
                    hash,
                    key,
                    Atomic::from(value),
                    Atomic::from(first),
                    Atomic::from(xp),
                )))
                .into_shared(guard);
                self.first.store(x, Ordering::SeqCst);
                if !first.is_null() {
                    unsafe { TreeNode::get_tree_node(first) }
                        .prev
                        .store(x, Ordering::SeqCst);
                }
                match dir {
                    Dir::Left => {
                        unsafe { TreeNode::get_tree_node(xp) }
                            .left
                            .store(x, Ordering::SeqCst);
                    }
                    Dir::Right => {
                        unsafe { TreeNode::get_tree_node(xp) }
                            .right
                            .store(x, Ordering::SeqCst);
                    }
                }

                if !unsafe { TreeNode::get_tree_node(xp) }
                    .red
                    .load(Ordering::SeqCst)
                {
                    unsafe { TreeNode::get_tree_node(x) }
                        .red
                        .store(true, Ordering::SeqCst);
                } else {
                    self.lock_root(guard);
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

        if cfg!(debug_assertions) {
            TreeNode::check_invariants(self.root.load(Ordering::SeqCst, guard), guard);
        }
        Shared::null()
    }
}

impl<K, V> Drop for TreeBin<K, V> {
    fn drop(&mut self) {
        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references to
        // anything in the map.
        unsafe { self.drop_fields(true) };
    }
}

impl<K, V> TreeBin<K, V> {
    /// Defers dropping the given tree bin without its nodes' values.
    ///
    /// # Safety
    /// The given bin must be a valid, non-null BinEntry::TreeBin and the caller must ensure
    /// that no references to the bin can be obtained by other threads after the call to this
    /// method.
    pub(crate) unsafe fn defer_drop_without_values<'g>(
        bin: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard,
    ) {
        guard.defer_unchecked(move || {
            if let BinEntry::Tree(mut tree_bin) = *bin.into_owned().into_box() {
                tree_bin.drop_fields(false);
            } else {
                unreachable!("bin is a tree bin");
            }
        });
    }

    /// Drops the given tree bin, but only drops its nodes' values when specified.
    ///
    /// # Safety
    /// The pointer to the tree bin must be valid and the caller must be the single owner
    /// of the tree bin. If the nodes' values are to be dropped, there must be no outstanding
    /// references to these values in other threads and it must be impossible to obtain them.
    pub(crate) unsafe fn drop_fields(&mut self, drop_values: bool) {
        // assume ownership of atomically shared references. note that it is
        // sufficient to follow the `next` pointers of the `first` element in
        // the bin, since the tree pointers point to the same nodes.

        // swap out first pointer so nodes will not get dropped again when
        // `tree_bin` is dropped
        let guard = crossbeam_epoch::unprotected();
        let p = self.first.swap(Shared::null(), Ordering::Relaxed, guard);
        Self::drop_tree_nodes(p, drop_values, guard);
    }

    /// Drops the given list of tree nodes, but only drops their values when specified.
    ///
    /// # Safety
    /// The pointers to the tree nodes must be valid and the caller must be the single owner
    /// of the tree nodes. If the nodes' values are to be dropped, there must be no outstanding
    /// references to these values in other threads and it must be impossible to obtain them.
    pub(crate) unsafe fn drop_tree_nodes<'g>(
        from: Shared<'g, BinEntry<K, V>>,
        drop_values: bool,
        guard: &'g Guard,
    ) {
        let mut p = from;
        while !p.is_null() {
            if let BinEntry::TreeNode(tree_node) = *p.into_owned().into_box() {
                // if specified, drop the value in this node
                if drop_values {
                    let _ = tree_node.node.value.into_owned();
                }
                // then we move to the next node
                p = tree_node.node.next.load(Ordering::SeqCst, guard);
            } else {
                unreachable!("Trees can only ever contain TreeNodes");
            };
        }
    }
}

/* Helper impls to avoid code explosion */
impl<K, V> TreeNode<K, V> {
    /// Gets the `BinEntry::TreeNode(tree_node)` behind the given pointer and
    /// returns its `tree_node`.
    ///
    /// # Safety
    /// All safety concerns of [`deref`](Shared::deref) apply. In particular, the
    /// supplied pointer must be non-null and must point to valid memory.
    /// Additionally, it must point to an instance of BinEntry that is actually a
    /// TreeNode.
    #[inline]
    pub(crate) unsafe fn get_tree_node<'g>(bin: Shared<'g, BinEntry<K, V>>) -> &'g TreeNode<K, V> {
        bin.deref().as_tree_node().unwrap()
    }
}

/* ----------------------------------------------------------------- */

macro_rules! treenode {
    ($pointer:ident) => {
        unsafe { Self::get_tree_node($pointer) }
    };
}
// Red-black tree methods, all adapted from CLR
impl<K, V> TreeNode<K, V> {
    // NOTE: these functions can be executed only when creating a new TreeBin or
    // while holding the `write_lock`. Thus, we can use `Relaxed` memory
    // operations everywhere.
    fn rotate_left<'g>(
        mut root: Shared<'g, BinEntry<K, V>>,
        p: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>> {
        if p.is_null() {
            return root;
        }
        // safety: the containing TreeBin of all TreeNodes was read under our
        // guard, at which point the tree structure was valid. Since our guard
        // pins the current epoch, the TreeNodes remain valid for at least as
        // long as we hold onto the guard.
        // Structurally, TreeNodes always point to TreeNodes, so this is sound.
        let p_deref = treenode!(p);
        let right = p_deref.right.load(Ordering::Relaxed, guard);
        if right.is_null() {
            // there is no right successor to rotate left
            return root;
        }
        let right_deref = treenode!(right);
        let right_left = right_deref.left.load(Ordering::Relaxed, guard);
        p_deref.right.store(right_left, Ordering::Relaxed);
        if !right_left.is_null() {
            treenode!(right_left).parent.store(p, Ordering::Relaxed);
        }

        let p_parent = p_deref.parent.load(Ordering::Relaxed, guard);
        right_deref.parent.store(p_parent, Ordering::Relaxed);
        if p_parent.is_null() {
            root = right;
            right_deref.red.store(false, Ordering::Relaxed);
        } else {
            let p_parent_deref = treenode!(p_parent);
            if p_parent_deref.left.load(Ordering::Relaxed, guard) == p {
                p_parent_deref.left.store(right, Ordering::Relaxed);
            } else {
                p_parent_deref.right.store(right, Ordering::Relaxed);
            }
        }
        right_deref.left.store(p, Ordering::Relaxed);
        p_deref.parent.store(right, Ordering::Relaxed);

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
        // safety: the containing TreeBin of all TreeNodes was read under our
        // guard, at which point the tree structure was valid. Since our guard
        // pins the current epoch, the TreeNodes remain valid for at least as
        // long as we hold onto the guard.
        // Structurally, TreeNodes always point to TreeNodes, so this is sound.
        let p_deref = treenode!(p);
        let left = p_deref.left.load(Ordering::Relaxed, guard);
        if left.is_null() {
            // there is no left successor to rotate right
            return root;
        }
        let left_deref = treenode!(left);
        let left_right = left_deref.right.load(Ordering::Relaxed, guard);
        p_deref.left.store(left_right, Ordering::Relaxed);
        if !left_right.is_null() {
            treenode!(left_right).parent.store(p, Ordering::Relaxed);
        }

        let p_parent = p_deref.parent.load(Ordering::Relaxed, guard);
        left_deref.parent.store(p_parent, Ordering::Relaxed);
        if p_parent.is_null() {
            root = left;
            left_deref.red.store(false, Ordering::Relaxed);
        } else {
            let p_parent_deref = treenode!(p_parent);
            if p_parent_deref.right.load(Ordering::Relaxed, guard) == p {
                p_parent_deref.right.store(left, Ordering::Relaxed);
            } else {
                p_parent_deref.left.store(left, Ordering::Relaxed);
            }
        }
        left_deref.right.store(p, Ordering::Relaxed);
        p_deref.parent.store(left, Ordering::Relaxed);

        root
    }

    fn balance_insertion<'g>(
        mut root: Shared<'g, BinEntry<K, V>>,
        mut x: Shared<'g, BinEntry<K, V>>,
        guard: &'g Guard,
    ) -> Shared<'g, BinEntry<K, V>> {
        // safety: the containing TreeBin of all TreeNodes was read under our
        // guard, at which point the tree structure was valid. Since our guard
        // pins the current epoch, the TreeNodes remain valid for at least as
        // long as we hold onto the guard.
        // Structurally, TreeNodes always point to TreeNodes, so this is sound.
        treenode!(x).red.store(true, Ordering::Relaxed);

        let mut x_parent: Shared<'_, BinEntry<K, V>>;
        let mut x_parent_parent: Shared<'_, BinEntry<K, V>>;
        let mut x_parent_parent_left: Shared<'_, BinEntry<K, V>>;
        let mut x_parent_parent_right: Shared<'_, BinEntry<K, V>>;
        loop {
            x_parent = treenode!(x).parent.load(Ordering::Relaxed, guard);
            if x_parent.is_null() {
                treenode!(x).red.store(false, Ordering::Relaxed);
                return x;
            }
            x_parent_parent = treenode!(x_parent).parent.load(Ordering::Relaxed, guard);
            if !treenode!(x_parent).red.load(Ordering::Relaxed) || x_parent_parent.is_null() {
                return root;
            }
            x_parent_parent_left = treenode!(x_parent_parent)
                .left
                .load(Ordering::Relaxed, guard);
            if x_parent == x_parent_parent_left {
                x_parent_parent_right = treenode!(x_parent_parent)
                    .right
                    .load(Ordering::Relaxed, guard);
                if !x_parent_parent_right.is_null()
                    && treenode!(x_parent_parent_right).red.load(Ordering::Relaxed)
                {
                    treenode!(x_parent_parent_right)
                        .red
                        .store(false, Ordering::Relaxed);
                    treenode!(x_parent).red.store(false, Ordering::Relaxed);
                    treenode!(x_parent_parent)
                        .red
                        .store(true, Ordering::Relaxed);
                    x = x_parent_parent;
                } else {
                    if x == treenode!(x_parent).right.load(Ordering::Relaxed, guard) {
                        x = x_parent;
                        root = Self::rotate_left(root, x, guard);
                        x_parent = treenode!(x).parent.load(Ordering::Relaxed, guard);
                        x_parent_parent = if x_parent.is_null() {
                            Shared::null()
                        } else {
                            treenode!(x_parent).parent.load(Ordering::Relaxed, guard)
                        };
                    }
                    if !x_parent.is_null() {
                        treenode!(x_parent).red.store(false, Ordering::Relaxed);
                        if !x_parent_parent.is_null() {
                            treenode!(x_parent_parent)
                                .red
                                .store(true, Ordering::Relaxed);
                            root = Self::rotate_right(root, x_parent_parent, guard);
                        }
                    }
                }
            } else if !x_parent_parent_left.is_null()
                && treenode!(x_parent_parent_left).red.load(Ordering::Relaxed)
            {
                treenode!(x_parent_parent_left)
                    .red
                    .store(false, Ordering::Relaxed);
                treenode!(x_parent).red.store(false, Ordering::Relaxed);
                treenode!(x_parent_parent)
                    .red
                    .store(true, Ordering::Relaxed);
                x = x_parent_parent;
            } else {
                if x == treenode!(x_parent).left.load(Ordering::Relaxed, guard) {
                    x = x_parent;
                    root = Self::rotate_right(root, x, guard);
                    x_parent = treenode!(x).parent.load(Ordering::Relaxed, guard);
                    x_parent_parent = if x_parent.is_null() {
                        Shared::null()
                    } else {
                        treenode!(x_parent).parent.load(Ordering::Relaxed, guard)
                    };
                }
                if !x_parent.is_null() {
                    treenode!(x_parent).red.store(false, Ordering::Relaxed);
                    if !x_parent_parent.is_null() {
                        treenode!(x_parent_parent)
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
        // safety: the containing TreeBin of all TreeNodes was read under our
        // guard, at which point the tree structure was valid. Since our guard
        // pins the current epoch, the TreeNodes remain valid for at least as
        // long as we hold onto the guard.
        // Structurally, TreeNodes always point to TreeNodes, so this is sound.
        loop {
            if x.is_null() || x == root {
                return root;
            }
            x_parent = treenode!(x).parent.load(Ordering::Relaxed, guard);
            if x_parent.is_null() {
                treenode!(x).red.store(false, Ordering::Relaxed);
                return x;
            } else if treenode!(x).red.load(Ordering::Relaxed) {
                treenode!(x).red.store(false, Ordering::Relaxed);
                return root;
            }
            x_parent_left = treenode!(x_parent).left.load(Ordering::Relaxed, guard);
            if x_parent_left == x {
                x_parent_right = treenode!(x_parent).right.load(Ordering::Relaxed, guard);
                if !x_parent_right.is_null()
                    && treenode!(x_parent_right).red.load(Ordering::Relaxed)
                {
                    treenode!(x_parent_right)
                        .red
                        .store(false, Ordering::Relaxed);
                    treenode!(x_parent).red.store(true, Ordering::Relaxed);
                    root = Self::rotate_left(root, x_parent, guard);
                    x_parent = treenode!(x).parent.load(Ordering::Relaxed, guard);
                    x_parent_right = if x_parent.is_null() {
                        Shared::null()
                    } else {
                        treenode!(x_parent).right.load(Ordering::Relaxed, guard)
                    };
                }
                if x_parent_right.is_null() {
                    x = x_parent;
                    continue;
                }
                let s_left = treenode!(x_parent_right)
                    .left
                    .load(Ordering::Relaxed, guard);
                let mut s_right = treenode!(x_parent_right)
                    .right
                    .load(Ordering::Relaxed, guard);

                if (s_right.is_null() || !treenode!(s_right).red.load(Ordering::Relaxed))
                    && (s_left.is_null() || !treenode!(s_left).red.load(Ordering::Relaxed))
                {
                    treenode!(x_parent_right).red.store(true, Ordering::Relaxed);
                    x = x_parent;
                    continue;
                }
                if s_right.is_null() || !treenode!(s_right).red.load(Ordering::Relaxed) {
                    if !s_left.is_null() {
                        treenode!(s_left).red.store(false, Ordering::Relaxed);
                    }
                    treenode!(x_parent_right).red.store(true, Ordering::Relaxed);
                    root = Self::rotate_right(root, x_parent_right, guard);
                    x_parent = treenode!(x).parent.load(Ordering::Relaxed, guard);
                    x_parent_right = if x_parent.is_null() {
                        Shared::null()
                    } else {
                        treenode!(x_parent).right.load(Ordering::Relaxed, guard)
                    };
                }
                if !x_parent_right.is_null() {
                    treenode!(x_parent_right).red.store(
                        if x_parent.is_null() {
                            false
                        } else {
                            treenode!(x_parent).red.load(Ordering::Relaxed)
                        },
                        Ordering::Relaxed,
                    );
                    s_right = treenode!(x_parent_right)
                        .right
                        .load(Ordering::Relaxed, guard);
                    if !s_right.is_null() {
                        treenode!(s_right).red.store(false, Ordering::Relaxed);
                    }
                }
                if !x_parent.is_null() {
                    treenode!(x_parent).red.store(false, Ordering::Relaxed);
                    root = Self::rotate_left(root, x_parent, guard);
                }
                x = root;
            } else {
                // symmetric
                if !x_parent_left.is_null() && treenode!(x_parent_left).red.load(Ordering::Relaxed)
                {
                    treenode!(x_parent_left).red.store(false, Ordering::Relaxed);
                    treenode!(x_parent).red.store(true, Ordering::Relaxed);
                    root = Self::rotate_right(root, x_parent, guard);
                    x_parent = treenode!(x).parent.load(Ordering::Relaxed, guard);
                    x_parent_left = if x_parent.is_null() {
                        Shared::null()
                    } else {
                        treenode!(x_parent).left.load(Ordering::Relaxed, guard)
                    };
                }
                if x_parent_left.is_null() {
                    x = x_parent;
                    continue;
                }
                let mut s_left = treenode!(x_parent_left).left.load(Ordering::Relaxed, guard);
                let s_right = treenode!(x_parent_left)
                    .right
                    .load(Ordering::Relaxed, guard);

                if (s_left.is_null() || !treenode!(s_left).red.load(Ordering::Relaxed))
                    && (s_right.is_null() || !treenode!(s_right).red.load(Ordering::Relaxed))
                {
                    treenode!(x_parent_left).red.store(true, Ordering::Relaxed);
                    x = x_parent;
                    continue;
                }
                if s_left.is_null() || !treenode!(s_left).red.load(Ordering::Relaxed) {
                    if !s_right.is_null() {
                        treenode!(s_right).red.store(false, Ordering::Relaxed);
                    }
                    treenode!(x_parent_left).red.store(true, Ordering::Relaxed);
                    root = Self::rotate_left(root, x_parent_left, guard);
                    x_parent = treenode!(x).parent.load(Ordering::Relaxed, guard);
                    x_parent_left = if x_parent.is_null() {
                        Shared::null()
                    } else {
                        treenode!(x_parent).left.load(Ordering::Relaxed, guard)
                    };
                }
                if !x_parent_left.is_null() {
                    treenode!(x_parent_left).red.store(
                        if x_parent.is_null() {
                            false
                        } else {
                            treenode!(x_parent).red.load(Ordering::Relaxed)
                        },
                        Ordering::Relaxed,
                    );
                    s_left = treenode!(x_parent_left).left.load(Ordering::Relaxed, guard);
                    if !s_left.is_null() {
                        treenode!(s_left).red.store(false, Ordering::Relaxed);
                    }
                }
                if !x_parent.is_null() {
                    treenode!(x_parent).red.store(false, Ordering::Relaxed);
                    root = Self::rotate_right(root, x_parent, guard);
                }
                x = root;
            }
        }
    }
    /// Checks invariants recursively for the tree of Nodes rootet at t.
    fn check_invariants<'g>(t: Shared<'g, BinEntry<K, V>>, guard: &'g Guard) {
        // safety: the containing TreeBin of all TreeNodes was read under our
        // guard, at which point the tree structure was valid. Since our guard
        // pins the current epoch, the TreeNodes remain valid for at least as
        // long as we hold onto the guard.
        // Structurally, TreeNodes always point to TreeNodes, so this is sound.
        let t_deref = treenode!(t);
        let t_parent = t_deref.parent.load(Ordering::Relaxed, guard);
        let t_left = t_deref.left.load(Ordering::Relaxed, guard);
        let t_right = t_deref.right.load(Ordering::Relaxed, guard);
        let t_back = t_deref.prev.load(Ordering::Relaxed, guard);
        let t_next = t_deref.node.next.load(Ordering::Relaxed, guard);

        if !t_back.is_null() {
            let t_back_deref = treenode!(t_back);
            assert_eq!(
                t_back_deref.node.next.load(Ordering::Relaxed, guard),
                t,
                "A TreeNode's `prev` node did not point back to it as its `next` node"
            );
        }
        if !t_next.is_null() {
            let t_next_deref = treenode!(t_next);
            assert_eq!(
                t_next_deref.prev.load(Ordering::Relaxed, guard),
                t,
                "A TreeNode's `next` node did not point back to it as its `prev` node"
            );
        }
        if !t_parent.is_null() {
            let t_parent_deref = treenode!(t_parent);
            assert!(
                t_parent_deref.left.load(Ordering::Relaxed, guard) == t
                    || t_parent_deref.right.load(Ordering::Relaxed, guard) == t,
                "A TreeNode's `parent` node did not point back to it as either its `left` or `right` child"
            );
        }
        if !t_left.is_null() {
            let t_left_deref = treenode!(t_left);
            assert_eq!(
                t_left_deref.parent.load(Ordering::Relaxed, guard),
                t,
                "A TreeNode's `left` child did not point back to it as its `parent` node"
            );
            assert!(
                t_left_deref.node.hash <= t_deref.node.hash,
                "A TreeNode's `left` child had a greater hash value than it"
            );
        }
        if !t_right.is_null() {
            let t_right_deref = treenode!(t_right);
            assert_eq!(
                t_right_deref.parent.load(Ordering::Relaxed, guard),
                t,
                "A TreeNode's `right` child did not point back to it as its `parent` node"
            );
            assert!(
                t_right_deref.node.hash >= t_deref.node.hash,
                "A TreeNode's `right` child had a smaller hash value than it"
            );
        }
        if t_deref.red.load(Ordering::Relaxed) && !t_left.is_null() && !t_right.is_null() {
            // if we are red, at least one of our children must be black
            let t_left_deref = treenode!(t_left);
            let t_right_deref = treenode!(t_right);
            assert!(
                !(t_left_deref.red.load(Ordering::Relaxed)
                    && t_right_deref.red.load(Ordering::Relaxed)),
                "A red TreeNode's two children were both also red"
            );
        }
        if !t_left.is_null() {
            Self::check_invariants(t_left, guard)
        }
        if !t_right.is_null() {
            Self::check_invariants(t_right, guard)
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
