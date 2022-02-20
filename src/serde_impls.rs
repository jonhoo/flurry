use crate::{HashMap, HashMapRef, HashSet, HashSetRef};
use serde::{
    de::{MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::fmt::{self, Formatter};
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;

struct HashMapVisitor<K, V, S> {
    key_marker: PhantomData<K>,
    value_marker: PhantomData<V>,
    hash_builder_marker: PhantomData<S>,
}

impl<K, V, S> Serialize for HashMapRef<'_, K, V, S>
where
    K: Serialize,
    V: Serialize,
{
    fn serialize<Sr>(&self, serializer: Sr) -> Result<Sr::Ok, Sr::Error>
    where
        Sr: Serializer,
    {
        serializer.collect_map(self.iter())
    }
}

impl<K, V, S> Serialize for HashMap<K, V, S>
where
    K: Serialize,
    V: Serialize,
{
    fn serialize<Sr>(&self, serializer: Sr) -> Result<Sr::Ok, Sr::Error>
    where
        Sr: Serializer,
    {
        self.pin().serialize(serializer)
    }
}

impl<'de, K, V, S> Deserialize<'de> for HashMap<K, V, S>
where
    K: Deserialize<'de> + Send + Sync + Hash + Clone + Ord,
    V: Deserialize<'de> + Send + Sync + Ord,
    S: Default + BuildHasher,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(HashMapVisitor::new())
    }
}

impl<K, V, S> HashMapVisitor<K, V, S> {
    pub(crate) fn new() -> Self {
        Self {
            key_marker: PhantomData,
            value_marker: PhantomData,
            hash_builder_marker: PhantomData,
        }
    }
}

impl<'de, K, V, S> Visitor<'de> for HashMapVisitor<K, V, S>
where
    K: Deserialize<'de> + Send + Sync + Hash + Clone + Ord,
    V: Deserialize<'de> + Send + Sync + Ord,
    S: Default + BuildHasher,
{
    type Value = HashMap<K, V, S>;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "a map")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let map = match access.size_hint() {
            Some(n) => HashMap::with_capacity_and_hasher(n, S::default()),
            None => HashMap::with_hasher(S::default()),
        };

        {
            let guard = map.guard();

            while let Some((key, value)) = access.next_entry()? {
                if let Some(_old_value) = map.insert(key, value, &guard) {
                    unreachable!("Serialized map held two values with the same key");
                }
            }
        }

        Ok(map)
    }
}

impl<T, S> Serialize for HashSetRef<'_, T, S>
where
    T: Serialize,
{
    fn serialize<Sr>(&self, serilizer: Sr) -> Result<Sr::Ok, Sr::Error>
    where
        Sr: Serializer,
    {
        serilizer.collect_seq(self.iter())
    }
}

impl<T, S> Serialize for HashSet<T, S>
where
    T: Serialize,
{
    fn serialize<Sr>(&self, serializer: Sr) -> Result<Sr::Ok, Sr::Error>
    where
        Sr: Serializer,
    {
        self.pin().serialize(serializer)
    }
}

impl<'de, T, S> Deserialize<'de> for HashSet<T, S>
where
    T: Deserialize<'de> + Send + Sync + Hash + Clone + Ord,
    S: Default + BuildHasher,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(HashSetVisitor::new())
    }
}

struct HashSetVisitor<T, S> {
    type_marker: PhantomData<T>,
    hash_builder_marker: PhantomData<S>,
}

impl<T, S> HashSetVisitor<T, S> {
    pub(crate) fn new() -> Self {
        Self {
            type_marker: PhantomData,
            hash_builder_marker: PhantomData,
        }
    }
}

impl<'de, T, S> Visitor<'de> for HashSetVisitor<T, S>
where
    T: Deserialize<'de> + Send + Sync + Hash + Clone + Ord,
    S: Default + BuildHasher,
{
    type Value = HashSet<T, S>;

    fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "a set")
    }

    fn visit_seq<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let set = HashSet::default();

        {
            let guard = set.guard();

            while let Some(value) = access.next_element()? {
                let _ = set.insert(value, &guard);
            }
        }

        Ok(set)
    }
}

#[cfg(test)]
mod test {
    use crate::{HashMap, HashSet};

    #[test]
    fn test_map() {
        let map: HashMap<u8, u8> = HashMap::with_capacity(5);
        let guard = map.guard();

        let _ = map.insert(0, 4, &guard);
        let _ = map.insert(1, 3, &guard);
        let _ = map.insert(2, 2, &guard);
        let _ = map.insert(3, 1, &guard);
        let _ = map.insert(4, 0, &guard);

        let serialized = serde_json::to_string(&map).expect("Couldn't serialize map");

        let deserialized: HashMap<u8, u8> =
            serde_json::from_str(&serialized).expect("Couldn't deserialize map");

        assert_eq!(map, deserialized);
    }

    #[test]
    fn test_set() {
        let set: HashSet<u8> = HashSet::with_capacity(5);
        let guard = set.guard();

        let _ = set.insert(0, &guard);
        let _ = set.insert(1, &guard);
        let _ = set.insert(2, &guard);
        let _ = set.insert(3, &guard);
        let _ = set.insert(4, &guard);

        let serialized = serde_json::to_string(&set).expect("Couldn't serialize map");

        let deserialized: HashSet<u8> =
            serde_json::from_str(&serialized).expect("Couldn't deserialize map");

        assert_eq!(set, deserialized);
    }
}
