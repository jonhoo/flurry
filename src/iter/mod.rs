mod traverser;
pub(crate) use traverser::NodeIter;

use crate::ebr::Guard;
use std::sync::atomic::Ordering;

/// An iterator over a map's entries.
///
/// See [`HashMap::iter`](crate::HashMap::iter) for details.
pub struct Iter<'m, 'g, K, V, SH> {
    pub(crate) node_iter: NodeIter<'m, 'g, K, V, SH>,
    pub(crate) guard: &'g Guard<'m, SH>,
}

impl<'m, 'g, K, V, SH> std::fmt::Debug for Iter<'m, 'g, K, V, SH>
where
    K: std::fmt::Debug,
    V: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter")
            .field("node_iter", &self.node_iter)
            .finish()
    }
}

impl<'m, 'g, K, V, SH> Iterator for Iter<'m, 'g, K, V, SH>
where
    SH: flize::Shield<'m>,
{
    type Item = (&'g K, &'g V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.node_iter.next()?;
        let value = node.value.load(Ordering::SeqCst, &self.guard.shield);
        // safety: flurry does not drop or move until after guard drop
        let value = unsafe { value.as_ref_unchecked() };
        Some((&node.key, &value))
    }
}

/// An iterator over a map's keys.
///
/// See [`HashMap::keys`](crate::HashMap::keys) for details.
#[derive(Debug)]
pub struct Keys<'m, 'g, K, V, SH> {
    pub(crate) node_iter: NodeIter<'m, 'g, K, V, SH>,
}

impl<'m, 'g, K, V, SH> Iterator for Keys<'m, 'g, K, V, SH>
where
    SH: flize::Shield<'m>,
{
    type Item = &'g K;
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.node_iter.next()?;
        Some(&node.key)
    }
}

/// An iterator over a map's values.
///
/// See [`HashMap::values`](crate::HashMap::values) for details.
pub struct Values<'m, 'g, K, V, SH> {
    pub(crate) node_iter: NodeIter<'m, 'g, K, V, SH>,
    pub(crate) guard: &'g Guard<'m, SH>,
}

impl<'m, 'g, K, V, SH> std::fmt::Debug for Values<'m, 'g, K, V, SH>
where
    K: std::fmt::Debug,
    V: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Values")
            .field("node_iter", &self.node_iter)
            .finish()
    }
}

impl<'m, 'g, K, V, SH> Iterator for Values<'m, 'g, K, V, SH>
where
    SH: flize::Shield<'m>,
{
    type Item = &'g V;
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.node_iter.next()?;
        let value = node.value.load(Ordering::SeqCst, &self.guard.shield);
        // safety: flurry does not drop or move until after guard drop
        let value = unsafe { value.as_ref_unchecked() };
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::HashMap;
    use flize::Shield;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    #[test]
    fn iter() {
        let map = HashMap::<usize, usize>::new();

        let guard = map.guard();
        map.insert(1, 42, &guard);
        map.insert(2, 84, &guard);

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

        assert_eq!(
            map.keys(&guard).collect::<HashSet<&usize>>(),
            HashSet::from_iter(vec![&1, &2])
        );
    }

    #[test]
    fn values() {
        let map = HashMap::<usize, usize>::new();

        let mut guard = map.guard();
        map.insert(1, 42, &guard);
        map.insert(2, 84, &guard);
        guard.repin();

        assert_eq!(
            map.values(&guard).collect::<HashSet<&usize>>(),
            HashSet::from_iter(vec![&42, &84])
        );
    }
}
