use dashmap::DashMap;
use std::{
    hash::Hash,
    sync::{
        Arc, Weak,
        atomic::{AtomicIsize, Ordering},
    },
};

#[derive(Clone)]
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

#[derive(Clone)]
pub struct RcMap<K, V> {
    inner: Arc<DashMap<K, (AtomicIsize, V)>>,
}

impl<K, V> RcMap<K, V>
where
    K: Hash + Eq + Clone,
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

    pub fn insert(&self, key: K, value: V) -> ObjectRef<K, V> {
        let _prev = self
            .inner
            .insert(key.clone(), (AtomicIsize::new(1), value.clone()));

        ObjectRef {
            key,
            parent_ref: Arc::downgrade(&self.inner),
            value,
        }
    }
}
