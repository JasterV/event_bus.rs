/// Work in progress
///
/// The goal is to build a map that maintains a reference counts of its pairs.
///
/// Once a pair doesn't have any more objects referencing it, the pair gets automatically removed.
use std::{
    hash::Hash,
    sync::{Arc, atomic::AtomicUsize},
};

use dashmap::DashMap;

pub struct Object<T> {
    count: AtomicUsize,
    elem: T,
}

impl<T> Object<T> {
    pub fn new(value: T) -> Self {
        Self {
            count: AtomicUsize::new(1),
            elem: value,
        }
    }
}

#[derive(Clone)]
pub struct RcMap<K, V> {
    inner: Arc<DashMap<K, Object<V>>>,
}

impl<K, V> RcMap<K, V>
where
    K: Hash + Eq,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, key: K) -> Option<Object<V>> {
        // TODO
        todo!()
    }

    pub fn insert(&self, key: K, value: V) -> Option<Object<V>> {
        todo!()
    }
}
