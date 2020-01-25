use super::NodeIter;
use crossbeam_epoch::Guard;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::Ordering;

/// An iterator over a map's entries.
///
/// See [`HashMap::iter`] for details.
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
/// See [`HashMap::keys`] for details.
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
/// See [`HashMap::values`] for details.
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

/// A draining iterator over a map's entries.
#[derive(Debug)]
pub struct Drain<'g, K, V, S>
where
    K: Sync + Send + Clone + Eq + Hash,
    V: Sync + Send,
    S: BuildHasher,
{
    pub(crate) node_iter: NodeIter<'g, K, V>,
    pub(crate) map: &'g crate::HashMap<K, V, S>,
    pub(crate) guard: &'g Guard,
}

impl<'g, K, V, S> Iterator for Drain<'g, K, V, S>
where
    K: Sync + Send + Clone + Eq + Hash,
    V: Sync + Send,
    S: BuildHasher,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let node = self.node_iter.next()?;

            if let Some(value) = self.map.remove(&node.key, self.guard) {
                return Some((node.key.clone(), unsafe {
                    std::ptr::read(value as *const V)
                }));
            }
        }
    }
}

#[derive(Debug)]
/// An owned iterator over a map's entries.
pub struct IntoIter<'g, K, V, S>
where
    K: Sync + Send + Clone + Eq + Hash,
    V: Sync + Send,
    S: BuildHasher,
{
    pub(crate) node_iter: NodeIter<'g, K, V>,
    pub(crate) map: crate::HashMap<K, V, S>,
    pub(crate) guard: &'g Guard,
}

impl<'g, K, V, S> Iterator for IntoIter<'g, K, V, S>
where
    K: Sync + Send + Clone + Eq + Hash,
    V: Sync + Send,
    S: BuildHasher,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.node_iter.next()?;

        let key = node.key.clone();
        let value = node.value.load(Ordering::SeqCst, self.guard);
        let value = unsafe { std::ptr::read(value.as_ref().unwrap() as *const V) };

        return Some((key, value));
    }
}

#[cfg(test)]
mod tests {
    use crate::HashMap;
    use crossbeam_epoch as epoch;
    use std::collections::HashSet;
    use std::iter::FromIterator;

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

    #[test]
    fn drain() {
        let map: HashMap<usize, usize> = HashMap::new();
        let mut expected: HashSet<(usize, usize)> = HashSet::new();
        let mut guard = epoch::pin();
        for i in 0..=100 {
            let key = i;
            let value = 100 - i;
            map.insert(key, value, &guard);
            expected.insert((key, value));
        }
        guard.repin();

        let result: HashSet<(usize, usize)> = map.drain(&guard).collect();
        assert_eq!(result, expected);

        assert_eq!(map.len(), 0);
    }

    #[test]
    fn concurent_drain() {
        use std::sync::*;

        let map = Arc::new(HashMap::new());

        let mut expected: HashSet<(usize, usize)> = HashSet::new();
        {
            let guard = epoch::pin();
            for i in 0..=100 {
                let key = i;
                let value = 100 - i;
                map.insert(key, value, &guard);
                expected.insert((key, value));
            }
        }

        let barrier = Arc::new(Barrier::new(2));

        let map_clone = map.clone();
        let barrier_clone = barrier.clone();
        let a = std::thread::spawn(move || {
            barrier_clone.wait();
            let guard = epoch::pin();

            let drain = map_clone.drain(&guard);
            let result: HashSet<(usize, usize)> = drain.collect();

            result
        });

        let map_clone = map.clone();
        let barrier_clone = barrier.clone();
        let b = std::thread::spawn(move || {
            barrier_clone.wait();
            let guard = epoch::pin();

            let mut result: HashSet<(usize, usize)> = HashSet::new();

            for i in 0..=100 {
                if let Some(item) = map_clone.remove(&i, &guard) {
                    result.insert((i, *item));
                }
            }

            result
        });

        let result_a = a.join().unwrap();
        let result_b = b.join().unwrap();

        assert!(!result_a.is_empty());
        assert!(!result_b.is_empty());

        assert!(result_a.intersection(&result_b).next().is_none());

        let union: HashSet<(usize, usize)> = result_a.union(&result_b).cloned().collect();

        assert_eq!(union, expected);
    }

    #[test]
    fn into_iter() {
        let map: HashMap<usize, usize> = HashMap::new();
        let mut expected: HashSet<(usize, usize)> = HashSet::new();
        let mut guard = epoch::pin();
        for i in 0..=100 {
            let key = i;
            let value = 100 - i;
            map.insert(key, value, &guard);
            expected.insert((key, value));
        }
        guard.repin();

        let result: HashSet<(usize, usize)> = map.into_iter().collect();
        assert_eq!(result, expected);
    }
}
