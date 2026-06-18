//! Opaque payload — the body of an [`Envelope`](crate::Envelope).
//!
//! Octo follows the HTTP/NATS protocol pattern: the envelope header is fixed
//! and strongly typed; the payload is opaque to the bus and is accessed by
//! downcasting to a known type at the handler.
//!
//! `Payload` wraps `Arc<dyn Any + Send + Sync>` so it can be cheaply cloned
//! and passed across actor boundaries without payload-shape coupling.

use std::any::{Any, TypeId};
use std::fmt;
use std::sync::Arc;

/// Opaque, type-erased message body.
///
/// Construct with [`Payload::new`] (or implicitly via [`Envelope::new`](crate::Envelope::new)).
/// Read with [`Payload::downcast_ref`].
///
/// In-process there is no serialisation cost — it's just a downcast on
/// `TypeId`. Distributed transport is out of scope at this layer; when it lands,
/// it will be handled by a separate codec registry that maps `EventKind` to
/// encoder/decoder pairs.
#[derive(Clone)]
pub struct Payload {
    inner: Arc<dyn Any + Send + Sync>,
    type_name: &'static str,
    type_id: TypeId,
}

impl Payload {
    /// Wrap a value as a payload.
    pub fn new<T>(value: T) -> Self
    where
        T: Any + Send + Sync + 'static,
    {
        Self {
            type_name: std::any::type_name::<T>(),
            type_id: TypeId::of::<T>(),
            inner: Arc::new(value),
        }
    }

    /// Try to read the payload as `&T`. Returns `None` if the stored type
    /// doesn't match.
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: Any + 'static,
    {
        self.inner.downcast_ref::<T>()
    }

    /// `true` if the payload was constructed from a value of type `T`.
    pub fn is<T: Any + 'static>(&self) -> bool {
        self.type_id == TypeId::of::<T>()
    }

    /// Compile-time type name of the wrapped value (e.g. `"my_app::TelegramMessage"`).
    /// Useful for logs and panic messages — *not* for routing decisions
    /// (use [`EventKind`](crate::EventKind) for that).
    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    /// `TypeId` of the wrapped value — useful for keying registries.
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Payload")
            .field("type", &self.type_name)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_typed_value() {
        let p = Payload::new(42i32);
        assert!(p.is::<i32>());
        assert!(!p.is::<u64>());
        assert_eq!(p.downcast_ref::<i32>(), Some(&42));
        assert_eq!(p.downcast_ref::<u64>(), None);
    }

    #[test]
    fn type_name_and_id_set() {
        let p = Payload::new("hello".to_string());
        assert!(p.type_name().contains("String"));
        assert_eq!(p.type_id(), TypeId::of::<String>());
    }

    #[derive(Debug, Clone, PartialEq)]
    struct Custom {
        x: u32,
    }

    #[test]
    fn custom_struct() {
        let p = Payload::new(Custom { x: 7 });
        assert_eq!(p.downcast_ref::<Custom>(), Some(&Custom { x: 7 }));
    }

    #[test]
    fn clone_shares_storage() {
        let p1 = Payload::new(123u8);
        let p2 = p1.clone();
        assert_eq!(p2.downcast_ref::<u8>(), Some(&123));
    }
}
