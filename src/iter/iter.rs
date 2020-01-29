use super::NodeIter;
use core::sync::atomic::Ordering;
use crossbeam_epoch::Guard;

/// An iterator over a map's entries.
///
/// See [`HashMap::iter`](crate::HashMap::iter) for details.
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
        return Some(&node.key);
    }
}

/// An iterator over a map's values.
///
/// See [`HashMap::values`](crate::HashMap::values) for details.
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
    use crate::HashMap;
    use core::iter::FromIterator;
    use crossbeam_epoch as epoch;
    use std::collections::HashSet;

    #[test]
    fn iter() {
        let map = HashMap::<usize, usize>::new();

        let guard = epoch::pin();
        map.insert(1, 42, &guard);
        map.insert(2, 84, &guard);

        let guard = epoch::pin();
        assert_eq!(
            map.iter(&guard).collect::<HashSet<(&usize, &usize)>>(),
            HashSet::from_iter(vec![(&1, &42), (&2, &84)])
        );
    }

    #[test]
    fn keys() {
        let map = HashMap::<usize, usize>::new();

        let guard = epoch::pin();
        map.insert(1, 42, &guard);
        map.insert(2, 84, &guard);

        let guard = epoch::pin();
        assert_eq!(
            map.keys(&guard).collect::<HashSet<&usize>>(),
            HashSet::from_iter(vec![&1, &2])
        );
    }

    #[test]
    fn values() {
        let map = HashMap::<usize, usize>::new();

        let mut guard = epoch::pin();
        map.insert(1, 42, &guard);
        map.insert(2, 84, &guard);
        guard.repin();

        assert_eq!(
            map.values(&guard).collect::<HashSet<&usize>>(),
            HashSet::from_iter(vec![&42, &84])
        );
    }
}
