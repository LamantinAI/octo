use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::Arc,
};

/// Opaque host values, separate from model-authored payloads and provenance tags.
#[derive(Clone, Default)]
pub struct EnvelopeContext(HashMap<TypeId, Arc<dyn Any + Send + Sync>>);

impl EnvelopeContext {
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) {
        self.0.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.0.get(&TypeId::of::<T>())?.downcast_ref()
    }
}

impl Debug for EnvelopeContext {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter
            .debug_struct("EnvelopeContext")
            .field("extensions", &self.0.len())
            .finish_non_exhaustive()
    }
}
