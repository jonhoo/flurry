use crossbeam::epoch::Guard;
use std::sync::atomic::Ordering;

use crate::NodeIter;

/// An iterator over the entries of a `FlurryHashMap`.
///
/// This `struct` is created by the [`iter`] method on [`FlurryHashMap`].
/// See its documentation for more.
///
/// [`iter`]: /flurry/struct.FlurryHashMap.html#method.iter
/// [`FlurryHashMap`]: /flurry/struct.FlurryHashMap.html
#[derive(Debug)]
pub struct Iter<'g, K, V> {
    pub(crate) node_iter: NodeIter<'g, K, V>,
    pub(crate) guard: &'g Guard,
}

impl<'g, K, V> Iterator for Iter<'g, K, V> {
    type Item = (&'g K, &'g V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.node_iter.next()?;
        let value = node.value.load(Ordering::SeqCst, self.guard);
        // safety: flurry does not drop or move until after guard drop
        let value = unsafe { value.deref() };
        return Some((&node.key, &value));
    }
}

/// An iterator over the keys of a `FlurryHashMap`.
///
/// This `struct` is created by the [`keys`] method on [`FlurryHashMap`].
/// See its documentation for more.
///
/// [`keys`]: /flurry/struct.FlurryHashMap.html#method.keys
/// [`FlurryHashMap`]: /flurry/struct.FlurryHashMap.html
#[derive(Debug)]
pub struct Keys<'g, K, V> {
    pub(crate) node_iter: NodeIter<'g, K, V>,
}

impl<'g, K, V> Iterator for Keys<'g, K, V> {
    type Item = &'g K;
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.node_iter.next()?;
        return Some(&node.key);
    }
}

/// An iterator over the values of a `FlurryHashMap`.
///
/// This `struct` is created by the [`values`] method on [`FlurryHashMap`].
/// See its documentation for more.
///
/// [`values`]: /flurry/struct.FlurryHashMap.html#method.values
/// [`FlurryHashMap`]: /flurry/struct.FlurryHashMap.html
#[derive(Debug)]
pub struct Values<'g, K, V> {
    pub(crate) node_iter: NodeIter<'g, K, V>,
    pub(crate) guard: &'g Guard,
}

impl<'g, K, V> Iterator for Values<'g, K, V> {
    type Item = &'g V;
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.node_iter.next()?;
        let value = node.value.load(Ordering::SeqCst, self.guard);
        // safety: flurry does not drop or move until after guard drop
        let value = unsafe { value.deref() };
        return Some(&value);
    }
}

#[cfg(test)]
mod tests {
    use crate::FlurryHashMap;
    use crossbeam::epoch::{self};
    use std::collections::HashSet;
    use std::iter::FromIterator;

    #[test]
    fn iter() {
        let map = FlurryHashMap::<usize, usize>::new();

        map.insert(1, 42);
        map.insert(2, 84);

        let guard = epoch::pin();
        assert_eq!(
            map.iter(&guard).collect::<HashSet<(&usize, &usize)>>(),
            HashSet::from_iter(vec![(&1, &42), (&2, &84)])
        );
    }

    #[test]
    fn keys() {
        let map = FlurryHashMap::<usize, usize>::new();

        map.insert(1, 42);
        map.insert(2, 84);

        let guard = epoch::pin();
        assert_eq!(
            map.keys(&guard).collect::<HashSet<&usize>>(),
            HashSet::from_iter(vec![&1, &2])
        );
    }

    #[test]
    fn values() {
        let map = FlurryHashMap::<usize, usize>::new();

        map.insert(1, 42);
        map.insert(2, 84);

        let guard = epoch::pin();
        assert_eq!(
            map.values(&guard).collect::<HashSet<&usize>>(),
            HashSet::from_iter(vec![&42, &84])
        );
    }
}
