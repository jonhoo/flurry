use super::NodeIter;
use crossbeam_epoch::{self as epoch, Guard, Shared};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::Ordering;

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
    type Item = (&'g K, &'g V);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let node = self.node_iter.next()?;

            if let Some(value) = self.map.remove(&node.key, self.guard) {
                return Some((&node.key, value));
            }
        }
    }
}

#[derive(Debug)]
/// An owned iterator over a map's entries.
pub struct IntoIter<K, V>
where
    K: Sync + Send + Clone + Eq + Hash,
    V: Sync + Send,
{
    pub(crate) table: Option<Box<crate::raw::Table<K, V>>>,
    pub(crate) bini: usize,
}

impl<K, V> Iterator for IntoIter<K, V>
where
    K: Sync + Send + Clone + Eq + Hash,
    V: Sync + Send,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        if self.table.is_none() {
            return None;
        }

        let table = self.table.as_mut().unwrap();

        let guard = unsafe { epoch::unprotected() };

        let bin = loop {
            if self.bini >= table.len() {
                return None;
            }

            let bin = table.bin(self.bini, guard);

            if !bin.is_null() {
                break bin;
            }

            self.bini += 1;
        };

        // safety: we own the map, so no other thread can have removed `bin`
        let node = unsafe { bin.deref() }.as_node().unwrap();
        let next = node.next.load(Ordering::SeqCst, &guard);

        // replace the `head` of the linked list of this bin with the next item in the list
        // or `null` if there is none
        table.swap_bin(self.bini, next, &guard);

        if next.is_null() {
            // since we know that this bin is now empty we can increment `bini` so
            // that we do not have to load it again on the next call to `next`
            self.bini += 1;
        }

        // take the node's value
        let value = node.value.swap(Shared::null(), Ordering::SeqCst, &guard);

        // safety: we just took removed node from the linked list and thus have exclusive acess to it
        let value = unsafe { std::ptr::read(value.as_ref().unwrap() as *const V) };

        // TODO: test if this actually `drop`s exactly once

        return Some((node.key.clone(), value));
    }
}

impl<K, V> Drop for IntoIter<K, V>
where
    K: Sync + Send + Clone + Eq + Hash,
    V: Sync + Send,
{
    fn drop(&mut self) {
        if let Some(table) = self.table.as_mut() {
            table.drop_bins();
        }
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

        let result = map
            .drain(&guard)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashSet<(usize, usize)>>();
        assert_eq!(result, expected);

        assert_eq!(map.len(), 0);
    }

    #[test]
    fn concurent_drain() {
        use std::sync::*;

        #[allow(non_snake_case)]
        let N = 50_000;

        let map = Arc::new(HashMap::new());

        let mut expected = HashSet::<(usize, usize)>::new();
        {
            let guard = epoch::pin();
            for i in 0..=N {
                let key = i;
                let value = N - i;
                map.insert(key, value, &guard);
                expected.insert((key, value));
            }
        }

        let barrier = Arc::new(Barrier::new(2));

        let map_clone = map.clone();
        let barrier_clone = barrier.clone();
        let a = std::thread::spawn(move || {
            let guard = epoch::pin();

            barrier_clone.wait();
            let drain = map_clone.drain(&guard);
            let result = drain
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashSet<(usize, usize)>>();

            result
        });

        let map_clone = map.clone();
        let barrier_clone = barrier.clone();
        let b = std::thread::spawn(move || {
            let guard = epoch::pin();
            let mut result: HashSet<(usize, usize)> = HashSet::new();

            barrier_clone.wait();
            for i in 0..=N {
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

    #[test]
    fn into_iter_uninit() {
        let map: HashMap<usize, usize> = HashMap::new();

        assert_eq!(map.into_iter().count(), 0);
    }
}
