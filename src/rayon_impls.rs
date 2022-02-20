use crate::{HashMap, HashMapRef, HashSet, HashSetRef};
use rayon::iter::{FromParallelIterator, IntoParallelIterator, ParallelExtend, ParallelIterator};
use std::hash::{BuildHasher, Hash};

impl<K, V, S> FromParallelIterator<(K, V)> for HashMap<K, V, S>
where
    K: Clone + Hash + Ord + Send + Sync,
    V: Send + Sync,
    S: BuildHasher + Default + Sync,
{
    fn from_par_iter<I>(par_iter: I) -> Self
    where
        I: IntoParallelIterator<Item = (K, V)>,
    {
        let mut created_map = HashMap::with_hasher(S::default());
        created_map.par_extend(par_iter);
        created_map
    }
}

impl<K, V, S> ParallelExtend<(K, V)> for HashMap<K, V, S>
where
    K: Clone + Hash + Ord + Send + Sync,
    V: Send + Sync,
    S: BuildHasher + Sync,
{
    fn par_extend<I>(&mut self, par_iter: I)
    where
        I: IntoParallelIterator<Item = (K, V)>,
    {
        (&*self).par_extend(par_iter);
    }
}

impl<K, V, S> ParallelExtend<(K, V)> for &HashMap<K, V, S>
where
    K: Clone + Hash + Ord + Send + Sync,
    V: Send + Sync,
    S: BuildHasher + Sync,
{
    fn par_extend<I>(&mut self, par_iter: I)
    where
        I: IntoParallelIterator<Item = (K, V)>,
    {
        par_iter.into_par_iter().for_each_init(
            || self.guard(),
            |guard, (k, v)| {
                self.insert(k, v, guard);
            },
        );
    }
}

impl<'map, K, V, S> ParallelExtend<(K, V)> for HashMapRef<'map, K, V, S>
where
    K: Clone + Hash + Ord + Send + Sync,
    V: Send + Sync,
    S: BuildHasher + Sync,
{
    fn par_extend<I>(&mut self, par_iter: I)
    where
        I: IntoParallelIterator<Item = (K, V)>,
    {
        self.map.par_extend(par_iter);
    }
}

impl<K, S> FromParallelIterator<K> for HashSet<K, S>
where
    K: Clone + Hash + Ord + Send + Sync,
    S: BuildHasher + Default + Sync,
{
    fn from_par_iter<I>(par_iter: I) -> Self
    where
        I: IntoParallelIterator<Item = K>,
    {
        let mut created_set = HashSet::with_hasher(S::default());
        created_set.par_extend(par_iter);
        created_set
    }
}

impl<K, S> ParallelExtend<K> for HashSet<K, S>
where
    K: Clone + Hash + Ord + Send + Sync,
    S: BuildHasher + Sync,
{
    fn par_extend<I>(&mut self, par_iter: I)
    where
        I: IntoParallelIterator<Item = K>,
    {
        (&*self).par_extend(par_iter);
    }
}

impl<K, S> ParallelExtend<K> for &HashSet<K, S>
where
    K: Clone + Hash + Ord + Send + Sync,
    S: BuildHasher + Sync,
{
    fn par_extend<I>(&mut self, par_iter: I)
    where
        I: IntoParallelIterator<Item = K>,
    {
        let tuple_iter = par_iter.into_par_iter().map(|k| (k, ()));
        (&self.map).par_extend(tuple_iter);
    }
}

impl<'set, K, S> ParallelExtend<K> for HashSetRef<'set, K, S>
where
    K: Clone + Hash + Ord + Send + Sync,
    S: BuildHasher + Sync,
{
    fn par_extend<I>(&mut self, par_iter: I)
    where
        I: IntoParallelIterator<Item = K>,
    {
        self.set.par_extend(par_iter);
    }
}

#[cfg(test)]
mod test {
    use crate::{HashMap, HashSet};
    use rayon::iter::{FromParallelIterator, IntoParallelIterator, ParallelExtend};

    #[test]
    fn hm_from_empty_parallel_iter() {
        let to_create_from: Vec<(i32, i32)> = Vec::new();
        let created_map: HashMap<i32, i32> = HashMap::from_par_iter(to_create_from.into_par_iter());
        assert_eq!(created_map.len(), 0);
    }

    #[test]
    fn hm_from_large_parallel_iter() {
        let mut to_create_from: Vec<(i32, i32)> = Vec::new();
        for i in 0..100 {
            to_create_from.push((i + 100, i * 10));
        }
        let created_map: HashMap<i32, i32> = HashMap::from_par_iter(to_create_from.into_par_iter());
        assert_eq!(created_map.len(), 100);

        let guard = created_map.guard();
        assert_eq!(created_map.get(&100, &guard), Some(&0));
        assert_eq!(created_map.get(&199, &guard), Some(&990));
    }

    #[test]
    fn hs_from_empty_parallel_iter() {
        let to_create_from: Vec<i32> = Vec::new();
        let created_set: HashSet<i32> = HashSet::from_par_iter(to_create_from.into_par_iter());
        assert_eq!(created_set.len(), 0);
    }

    #[test]
    fn hs_from_large_parallel_iter() {
        let mut to_create_from: Vec<(i32, i32)> = Vec::new();
        for i in 0..100 {
            to_create_from.push((i + 100, i * 10));
        }
        let created_map: HashSet<(i32, i32)> =
            HashSet::from_par_iter(to_create_from.into_par_iter());
        assert_eq!(created_map.len(), 100);

        let guard = created_map.guard();
        assert!(created_map.contains(&(100, 0), &guard));
        assert!(!created_map.contains(&(100, 10000), &guard));
    }

    #[test]
    fn hm_parallel_extend_by_nothing() {
        let to_extend_with = Vec::new();

        let mut map = HashMap::new();

        {
            let guard = map.guard();
            map.insert(1, 2, &guard);
            map.insert(3, 4, &guard);
        }

        map.par_extend(to_extend_with.into_par_iter());

        assert_eq!(map.len(), 2);

        let guard = map.guard();
        assert_eq!(map.get(&1, &guard), Some(&2));
        assert_eq!(map.get(&3, &guard), Some(&4));
    }

    #[test]
    fn hm_parallel_extend_by_a_bunch() {
        let mut to_extend_with = Vec::new();
        for i in 0..100 {
            to_extend_with.push((i + 100, i * 10));
        }

        let mut map = HashMap::new();

        {
            let guard = map.guard();
            map.insert(1, 2, &guard);
            map.insert(3, 4, &guard);
        }

        map.par_extend(to_extend_with.into_par_iter());
        assert_eq!(map.len(), 102);

        let guard = map.guard();
        assert_eq!(map.get(&1, &guard), Some(&2));
        assert_eq!(map.get(&3, &guard), Some(&4));
        assert_eq!(map.get(&100, &guard), Some(&0));
        assert_eq!(map.get(&199, &guard), Some(&990));
    }

    #[test]
    fn hm_ref_parallel_extend_by_nothing() {
        let to_extend_with = Vec::new();

        let map = HashMap::new();
        let guard = map.guard();
        map.insert(1, 2, &guard);
        map.insert(3, 4, &guard);

        map.pin().par_extend(to_extend_with.into_par_iter());

        assert_eq!(map.len(), 2);

        assert_eq!(map.get(&1, &guard), Some(&2));
        assert_eq!(map.get(&3, &guard), Some(&4));
    }

    #[test]
    fn hm_ref_parallel_extend_by_a_bunch() {
        let mut to_extend_with = Vec::new();
        for i in 0..100 {
            to_extend_with.push((i + 100, i * 10));
        }

        let map = HashMap::new();
        let guard = map.guard();
        map.insert(1, 2, &guard);
        map.insert(3, 4, &guard);

        map.pin().par_extend(to_extend_with.into_par_iter());
        assert_eq!(map.len(), 102);

        assert_eq!(map.get(&1, &guard), Some(&2));
        assert_eq!(map.get(&3, &guard), Some(&4));
        assert_eq!(map.get(&100, &guard), Some(&0));
        assert_eq!(map.get(&199, &guard), Some(&990));
    }

    #[test]
    fn hs_parallel_extend_by_nothing() {
        let to_extend_with = Vec::new();

        let mut set = HashSet::new();

        {
            let guard = set.guard();
            set.insert(1, &guard);
            set.insert(3, &guard);
        }

        set.par_extend(to_extend_with.into_par_iter());

        assert_eq!(set.len(), 2);

        let guard = set.guard();
        assert!(set.contains(&1, &guard));
        assert!(!set.contains(&17, &guard));
    }

    #[test]
    fn hs_parallel_extend_by_a_bunch() {
        let mut to_extend_with = Vec::new();
        for i in 0..100 {
            to_extend_with.push((i + 100, i * 10));
        }

        let mut set = HashSet::new();

        {
            let guard = set.guard();
            set.insert((1, 2), &guard);
            set.insert((3, 4), &guard);
        }

        set.par_extend(to_extend_with.into_par_iter());
        assert_eq!(set.len(), 102);

        let guard = set.guard();
        assert!(set.contains(&(1, 2), &guard));
        assert!(set.contains(&(199, 990), &guard));
        assert!(!set.contains(&(199, 167), &guard));
    }

    #[test]
    fn hs_ref_parallel_extend_by_nothing() {
        let to_extend_with = Vec::new();

        let mut set = HashSet::new();

        {
            let guard = set.guard();
            set.insert((1, 2), &guard);
            set.insert((3, 4), &guard);
        }

        set.par_extend(to_extend_with.into_par_iter());
        assert_eq!(set.len(), 2);

        let guard = set.guard();
        assert!(set.contains(&(1, 2), &guard));
        assert!(!set.contains(&(199, 990), &guard));
        assert!(!set.contains(&(199, 167), &guard));
    }

    #[test]
    fn hs_ref_parallel_extend_by_a_bunch() {
        let mut to_extend_with = Vec::new();
        for i in 0..100 {
            to_extend_with.push((i + 100, i * 10));
        }

        let set = HashSet::new();
        let mut set_ref = set.pin();
        set_ref.insert((1, 2));
        set_ref.insert((3, 4));

        set_ref.par_extend(to_extend_with.into_par_iter());
        assert_eq!(set.len(), 102);

        assert!(set_ref.contains(&(1, 2)));
        assert!(set_ref.contains(&(199, 990)));
        assert!(!set_ref.contains(&(199, 167)));
    }
}
