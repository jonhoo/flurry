mod traverser;
pub(crate) use traverser::NodeIter;

use crate::reclaim::{Guard, Shared};
use std::sync::atomic::Ordering;

/// An iterator over a map's entries.
///
/// See [`HashMap::iter`](crate::HashMap::iter) for details.
#[derive(Debug)]
pub struct Iter<'g, K, V> {
    pub(crate) node_iter: NodeIter<'g, K, V>,
    pub(crate) guard: &'g Guard<'g>,
}

impl<'g, K, V> Iter<'g, K, V> {
    pub(crate) fn next_internal(&mut self) -> Option<(&'g K, Shared<'g, V>)> {
        let node = self.node_iter.next()?;
        let value = node.value.load(Ordering::SeqCst, self.guard);
        Some((&node.key, value))
    }
}

impl<'g, K, V> Iterator for Iter<'g, K, V> {
    type Item = (&'g K, &'g V);
    fn next(&mut self) -> Option<Self::Item> {
        // safety: flurry does not drop or move until after guard drop
        self.next_internal()
            .map(|(k, v)| unsafe { (k, &**v.deref()) })
    }
}

/// An iterator over a map's keys.
///
/// See [`HashMap::keys`](crate::HashMap::keys) for details.
#[derive(Debug)]
pub struct Keys<'g, K, V> {
    pub(crate) node_iter: NodeIter<'g, K, V>,
}

impl<'g, K, V> Iterator for Keys<'g, K, V> {
    type Item = &'g K;
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.node_iter.next()?;
        Some(&node.key)
    }
}

/// An iterator over a map's values.
///
/// See [`HashMap::values`](crate::HashMap::values) for details.
#[derive(Debug)]
pub struct Values<'g, K, V> {
    pub(crate) node_iter: NodeIter<'g, K, V>,
    pub(crate) guard: &'g Guard<'g>,
}

impl<'g, K, V> Iterator for Values<'g, K, V> {
    type Item = &'g V;
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.node_iter.next()?;
        let value = node.value.load(Ordering::SeqCst, self.guard);
        // safety: flurry does not drop or move until after guard drop
        let value = unsafe { value.deref() };
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::HashMap;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    #[test]
    fn iter() {
        let map = HashMap::<usize, usize>::new();

        let guard = map.guard();
        map.insert(1, 42, &guard);
        map.insert(2, 84, &guard);

        let guard = map.guard();
        assert_eq!(
            map.iter(&guard).collect::<HashSet<(&usize, &usize)>>(),
            HashSet::from_iter(vec![(&1, &42), (&2, &84)])
        );
    }

    #[test]
    fn keys() {
        let map = HashMap::<usize, usize>::new();

        let guard = map.guard();
        map.insert(1, 42, &guard);
        map.insert(2, 84, &guard);

        let guard = map.guard();
        assert_eq!(
            map.keys(&guard).collect::<HashSet<&usize>>(),
            HashSet::from_iter(vec![&1, &2])
        );
    }

    #[test]
    fn values() {
        let map = HashMap::<usize, usize>::new();

        let guard = map.guard();
        map.insert(1, 42, &guard);
        map.insert(2, 84, &guard);
        let guard = map.guard();

        assert_eq!(
            map.values(&guard).collect::<HashSet<&usize>>(),
            HashSet::from_iter(vec![&42, &84])
        );
    }
}
