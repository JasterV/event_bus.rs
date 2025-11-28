//! This module provides an RcMap (Or ReferenceCountMap) which is a map that keeps track of how many references of an entry exist.
//!
/// This data structure keeps track of how many references of an entry exist and perform automatic cleanup when an entry has no references pointing to it.
use dashmap::DashMap;
use std::{
    fmt::Debug,
    hash::Hash,
    sync::{
        Arc, Weak,
        atomic::{AtomicIsize, Ordering},
    },
};

/// A smart reference around a key value pair.
///
/// Once it is dropped, it will decrease the reference counter of the pair and potentially remove the pair if the counter hits 0.
#[derive(Clone, Debug)]
pub struct ObjectRef<K, V>
where
    K: Hash + Eq + Clone,
{
    parent_ref: Weak<DashMap<K, (AtomicIsize, V)>>,
    key: K,
    value: V,
}

impl<K, V> ObjectRef<K, V>
where
    K: Hash + Eq + Clone,
{
    pub fn value(&self) -> &V {
        &self.value
    }
}

impl<K, V> Drop for ObjectRef<K, V>
where
    K: Hash + Eq + Clone,
{
    fn drop(&mut self) {
        let Some(map) = self.parent_ref.upgrade() else {
            return;
        };

        map.alter(&self.key, |_, (count, value)| {
            count.fetch_sub(1, Ordering::Relaxed);
            (count, value)
        });

        map.remove_if(&self.key, |_, (count, _)| {
            count.load(Ordering::Relaxed) <= 0
        });
    }
}

#[derive(thiserror::Error, Debug)]
pub enum InsertError<K, V>
where
    K: Hash + Eq + Clone + Debug,
{
    #[error(
        "An entry already exists with the given key: '{0:?}'. You must wait until all existing object references are dropped for the pair to be removed."
    )]
    AlreadyExists(K, ObjectRef<K, V>),
}

/// A ReferenceCountMap.
///
/// It keeps track of how many references of an entry exist and perform automatic cleanup when an entry has no references pointing to it.
///
/// Every time someone gets a value by key, that value's reference counter increases.
/// When a reference to a value is dropped, the reference counter decreases.
///
/// When the references counter of a value hits 0, the whole pair is removed from the map.
#[derive(Clone)]
pub struct RcMap<K, V> {
    inner: Arc<DashMap<K, (AtomicIsize, V)>>,
}

impl<K, V> RcMap<K, V>
where
    K: Hash + Eq + Clone + Debug,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, key: K) -> Option<ObjectRef<K, V>> {
        self.inner.alter(&key, |_, (count, value)| {
            count.fetch_add(1, Ordering::Relaxed);
            (count, value)
        });

        let option = self.inner.get(&key);

        match option {
            Some(value_ref) => {
                let (_count, value) = value_ref.value();

                Some(ObjectRef {
                    key,
                    parent_ref: Arc::downgrade(&self.inner),
                    value: value.clone(),
                })
            }
            None => None,
        }
    }

    /// Insert a new pair into the map.
    ///
    /// If an entry already exists for the given key, an error will be returned.
    ///
    /// For consistency reasons an entry must only be removed by the last `ObjectRef` being dropped.
    ///
    /// Otherwise, we could have unrelated old `ObjectRef` instances modifying the reference count of the new entry when being dropped.
    ///
    /// To prevent this from happening, we enforce this rule so that you must wait until all `ObjectRef` pointing to the current entry are dropped.
    pub fn insert(&self, key: K, value: V) -> Result<ObjectRef<K, V>, InsertError<K, V>> {
        if let Some(object_ref) = self.get(key.clone()) {
            return Err(InsertError::AlreadyExists(key, object_ref));
        }

        let _prev = self
            .inner
            .insert(key.clone(), (AtomicIsize::new(1), value.clone()));

        Ok(ObjectRef {
            key,
            parent_ref: Arc::downgrade(&self.inner),
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_can_insert_pairs_and_get_them() {
        let map = RcMap::new();
        let inserted_ref = map
            .insert("potatoe", "chair")
            .expect("No entry should exist");
        let obj_ref = map.get("potatoe").expect("A value should be inserted");

        assert_eq!(obj_ref.value(), inserted_ref.value())
    }

    #[test]
    fn it_removes_the_pair_when_the_only_existing_obj_ref_drops() {
        let map = RcMap::new();
        let inserted_ref = map
            .insert("potatoe", "chair")
            .expect("No entry should exist");

        drop(inserted_ref);

        let obj_ref = map.get("potatoe");

        assert!(obj_ref.is_none());
    }

    #[test]
    fn it_removes_the_pair_only_when_the_all_existing_refs_drop() {
        let map = RcMap::new();

        let inserted_ref = map
            .insert("potatoe", "chair")
            .expect("No entry should exist");
        let obj_ref1 = map.get("potatoe");
        let obj_ref2 = map.get("potatoe");
        let obj_ref3 = map.get("potatoe");

        drop(inserted_ref);
        assert!(map.get("potatoe").is_some());
        drop(obj_ref1);
        assert!(map.get("potatoe").is_some());
        drop(obj_ref2);
        assert!(map.get("potatoe").is_some());
        drop(obj_ref3);
        assert!(map.get("potatoe").is_none());
    }

    #[test]
    fn it_returns_an_error_if_trying_to_insert_a_key_that_already_exists() {
        let map = RcMap::new();

        let _ref = map
            .insert("potatoe", "chair")
            .expect("No entry should exist");

        let result = map.insert("potatoe", "table");

        assert!(matches!(result, Err(InsertError::AlreadyExists(_, _))));
    }

    #[test]
    fn it_can_insert_a_pair_after_the_old_one_has_been_removed() {
        let map = RcMap::new();

        let inserted_ref = map
            .insert("potatoe", "chair")
            .expect("No entry should exist");

        let result = map.insert("potatoe", "table");

        assert!(matches!(result, Err(InsertError::AlreadyExists(_, _))));

        // The error also contains an object ref so we must drop it
        drop(result);
        drop(inserted_ref);

        let result = map.insert("potatoe", "table");

        assert!(result.is_ok());
    }
}
