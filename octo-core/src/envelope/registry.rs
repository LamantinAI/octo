//! Payload registry — a catalogue of `EventKind → format description`.
//!
//! For each registered kind it holds:
//! - **Rust type** (`TypeId` + `type_name`) — the in-process equivalent of a MIME type.
//! - **Optional JSON schema** — used when an LLM-driven cogitator builds its
//!   tool catalogue from connector capabilities.
//! - **Optional JSON codec** — used when the payload crosses a boundary that
//!   needs bytes (LLM tool-call decoding, persistence, future cross-process).
//!
//! The registry is **optional** at the runtime level. Without it, the bus
//! behaves as before. With it, the bus validates each published envelope:
//! the payload's `TypeId` must match the registry's entry for that kind. A
//! mismatch is a hard error — caught early instead of being a silent
//! `downcast_ref::<...>() = None` further downstream.
//!
//! Modularity is preserved at the type level: each connector crate is expected
//! to expose its own `register_payloads(reg: &mut PayloadRegistry)` helper.
//! Removing a connector = not calling its helper (and optionally dropping the
//! crate from Cargo.toml). No cross-crate type imports are required.

use std::any::{Any, TypeId};
use std::collections::HashMap;

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::{Envelope, EventKind, Payload};

/// Errors raised by the registry.
#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("payload type mismatch for kind '{kind}': expected {expected}, got {actual}")]
    TypeMismatch {
        kind: EventKind,
        expected: &'static str,
        actual: &'static str,
    },

    #[error("kind '{0}' not registered (strict mode)")]
    UnknownKind(EventKind),

    #[error("no codec registered for kind '{0}'")]
    NoCodec(EventKind),

    #[error("encode failed for kind '{kind}': {source}")]
    Encode {
        kind: EventKind,
        #[source]
        source: serde_json::Error,
    },

    #[error("decode failed for kind '{kind}': {source}")]
    Decode {
        kind: EventKind,
        #[source]
        source: serde_json::Error,
    },
}

type EncodeFn = Box<dyn Fn(&Payload) -> Result<Vec<u8>, serde_json::Error> + Send + Sync>;
type DecodeFn = Box<dyn Fn(&[u8]) -> Result<Payload, serde_json::Error> + Send + Sync>;

/// One entry in the registry — the format description for a single kind.
pub struct RegistryEntry {
    type_id: TypeId,
    type_name: &'static str,
    schema: Option<serde_json::Value>,
    encoder: Option<EncodeFn>,
    decoder: Option<DecodeFn>,
}

impl RegistryEntry {
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    pub fn schema(&self) -> Option<&serde_json::Value> {
        self.schema.as_ref()
    }

    pub fn has_codec(&self) -> bool {
        self.encoder.is_some() && self.decoder.is_some()
    }
}

impl std::fmt::Debug for RegistryEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryEntry")
            .field("type_name", &self.type_name)
            .field("schema", &self.schema.is_some())
            .field("has_codec", &self.has_codec())
            .finish_non_exhaustive()
    }
}

/// Catalogue of payload formats per event kind.
///
/// Constructed via [`PayloadRegistry::new`] and decorated with `register_*`
/// methods (chainable). Wrap in `Arc` and pass to
/// [`OctoBuilder::payload_registry`](crate::OctoBuilder::payload_registry).
#[derive(Default)]
pub struct PayloadRegistry {
    entries: HashMap<EventKind, RegistryEntry>,
    strict: bool,
}

impl PayloadRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Switch to strict mode: unknown kinds fail validation.
    /// Default (lenient): unknown kinds are passed through unvalidated.
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    pub fn is_strict(&self) -> bool {
        self.strict
    }

    /// Register a Rust type for a kind — type-only, no codec, no schema.
    ///
    /// Sufficient for in-process validation. Use [`Self::register_codec`] or
    /// [`Self::register_with_schema`] when you need to cross a boundary that
    /// requires bytes / JSON schemas (LLM cogitator, persistence).
    ///
    /// Panics if the kind is already registered with a different type.
    pub fn register_type<T>(mut self, kind: EventKind) -> Self
    where
        T: Any + Send + Sync + 'static,
    {
        self.insert(
            kind,
            RegistryEntry {
                type_id: TypeId::of::<T>(),
                type_name: std::any::type_name::<T>(),
                schema: None,
                encoder: None,
                decoder: None,
            },
        );
        self
    }

    /// Register a type with a default JSON codec (via serde).
    ///
    /// Required when payloads of this kind cross a JSON boundary — LLM tool
    /// calls, persistence, distributed bus.
    ///
    /// Panics if the kind is already registered with a different type.
    pub fn register_codec<T>(mut self, kind: EventKind) -> Self
    where
        T: Any + Send + Sync + Serialize + DeserializeOwned + 'static,
    {
        let encoder: EncodeFn = Box::new(|payload: &Payload| {
            // Safety: registry validation guarantees TypeId match before this is called.
            let typed = payload
                .downcast_ref::<T>()
                .expect("registry contract: payload type matches registered type");
            serde_json::to_vec(typed)
        });
        let decoder: DecodeFn = Box::new(|bytes: &[u8]| {
            let typed: T = serde_json::from_slice(bytes)?;
            Ok(Payload::new(typed))
        });
        self.insert(
            kind,
            RegistryEntry {
                type_id: TypeId::of::<T>(),
                type_name: std::any::type_name::<T>(),
                schema: None,
                encoder: Some(encoder),
                decoder: Some(decoder),
            },
        );
        self
    }

    /// Register a type with codec AND explicit JSON schema (e.g. authored
    /// manually for the LLM tool catalogue).
    ///
    /// Panics if the kind is already registered with a different type.
    pub fn register_with_schema<T>(mut self, kind: EventKind, schema: serde_json::Value) -> Self
    where
        T: Any + Send + Sync + Serialize + DeserializeOwned + 'static,
    {
        let encoder: EncodeFn = Box::new(|payload: &Payload| {
            let typed = payload
                .downcast_ref::<T>()
                .expect("registry contract: payload type matches registered type");
            serde_json::to_vec(typed)
        });
        let decoder: DecodeFn = Box::new(|bytes: &[u8]| {
            let typed: T = serde_json::from_slice(bytes)?;
            Ok(Payload::new(typed))
        });
        self.insert(
            kind,
            RegistryEntry {
                type_id: TypeId::of::<T>(),
                type_name: std::any::type_name::<T>(),
                schema: Some(schema),
                encoder: Some(encoder),
                decoder: Some(decoder),
            },
        );
        self
    }

    fn insert(&mut self, kind: EventKind, entry: RegistryEntry) {
        if let Some(existing) = self.entries.get(&kind) {
            if existing.type_id != entry.type_id {
                panic!(
                    "PayloadRegistry conflict: kind '{}' already registered as '{}', \
                     attempted to re-register as '{}'",
                    kind, existing.type_name, entry.type_name
                );
            }
            // Same type — keep the latest (allows schema/codec upgrades).
        }
        self.entries.insert(kind, entry);
    }

    pub fn lookup(&self, kind: &EventKind) -> Option<&RegistryEntry> {
        self.entries.get(kind)
    }

    pub fn registered_kinds(&self) -> impl Iterator<Item = &EventKind> {
        self.entries.keys()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Check that an envelope's payload type matches what's registered for
    /// its kind. Lenient mode (default): unknown kinds pass. Strict mode:
    /// unknown kinds fail.
    pub fn validate(&self, envelope: &Envelope) -> Result<(), RegistryError> {
        match self.lookup(&envelope.kind) {
            Some(entry) if entry.type_id == envelope.payload.type_id() => Ok(()),
            Some(entry) => Err(RegistryError::TypeMismatch {
                kind: envelope.kind.clone(),
                expected: entry.type_name,
                actual: envelope.payload.type_name(),
            }),
            None if self.strict => Err(RegistryError::UnknownKind(envelope.kind.clone())),
            None => Ok(()),
        }
    }

    /// Encode an envelope's payload as JSON bytes using the registered codec.
    pub fn encode_payload(&self, envelope: &Envelope) -> Result<Vec<u8>, RegistryError> {
        self.validate(envelope)?;
        let entry = self
            .lookup(&envelope.kind)
            .ok_or_else(|| RegistryError::UnknownKind(envelope.kind.clone()))?;
        let encoder = entry
            .encoder
            .as_ref()
            .ok_or_else(|| RegistryError::NoCodec(envelope.kind.clone()))?;
        encoder(&envelope.payload).map_err(|e| RegistryError::Encode {
            kind: envelope.kind.clone(),
            source: e,
        })
    }

    /// Decode JSON bytes into a typed payload, using the codec registered
    /// for the given kind.
    pub fn decode_payload(&self, kind: &EventKind, bytes: &[u8]) -> Result<Payload, RegistryError> {
        let entry = self
            .lookup(kind)
            .ok_or_else(|| RegistryError::UnknownKind(kind.clone()))?;
        let decoder = entry
            .decoder
            .as_ref()
            .ok_or_else(|| RegistryError::NoCodec(kind.clone()))?;
        decoder(bytes).map_err(|e| RegistryError::Decode {
            kind: kind.clone(),
            source: e,
        })
    }
}

impl std::fmt::Debug for PayloadRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadRegistry")
            .field("len", &self.entries.len())
            .field("strict", &self.strict)
            .field(
                "kinds",
                &self.entries.keys().map(|k| k.as_str()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConnectorId;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Alert {
        text: String,
        severity: u8,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Reading {
        sensor: String,
        value: f64,
    }

    fn env<T: Any + Send + Sync + 'static>(kind: &'static str, payload: T) -> Envelope {
        Envelope::new(
            ConnectorId::new("test"),
            EventKind::from_static(kind),
            payload,
        )
    }

    #[test]
    fn register_type_and_lookup() {
        let registry = PayloadRegistry::new()
            .register_type::<Alert>(EventKind::from_static("alert.text"));

        let entry = registry
            .lookup(&EventKind::from_static("alert.text"))
            .expect("kind registered");
        assert_eq!(entry.type_id(), TypeId::of::<Alert>());
        assert!(!entry.has_codec());
        assert!(entry.schema().is_none());
    }

    #[test]
    fn validate_matching_type_passes() {
        let registry = PayloadRegistry::new()
            .register_type::<Alert>(EventKind::from_static("alert.text"));
        let e = env("alert.text", Alert { text: "ok".into(), severity: 1 });
        assert!(registry.validate(&e).is_ok());
    }

    #[test]
    fn validate_mismatched_type_fails() {
        let registry = PayloadRegistry::new()
            .register_type::<Alert>(EventKind::from_static("alert.text"));
        let e = env("alert.text", "string-not-Alert".to_string());
        let err = registry.validate(&e).unwrap_err();
        assert!(matches!(err, RegistryError::TypeMismatch { .. }));
    }

    #[test]
    fn validate_unknown_kind_lenient_passes() {
        let registry = PayloadRegistry::new();
        let e = env("anything", 42i32);
        assert!(registry.validate(&e).is_ok());
    }

    #[test]
    fn validate_unknown_kind_strict_fails() {
        let registry = PayloadRegistry::new().strict();
        let e = env("anything", 42i32);
        let err = registry.validate(&e).unwrap_err();
        assert!(matches!(err, RegistryError::UnknownKind(_)));
    }

    #[test]
    fn codec_roundtrip() {
        let registry = PayloadRegistry::new()
            .register_codec::<Alert>(EventKind::from_static("alert.text"));

        let original = Alert {
            text: "intrusion".into(),
            severity: 9,
        };
        let envelope = env("alert.text", original.clone());

        let bytes = registry.encode_payload(&envelope).expect("encode works");
        let decoded = registry
            .decode_payload(&envelope.kind, &bytes)
            .expect("decode works");

        let recovered = decoded
            .downcast_ref::<Alert>()
            .expect("decoded payload is Alert");
        assert_eq!(*recovered, original);
    }

    #[test]
    fn encode_without_codec_fails() {
        let registry = PayloadRegistry::new()
            .register_type::<Alert>(EventKind::from_static("alert.text"));
        let envelope = env("alert.text", Alert { text: "x".into(), severity: 1 });
        let err = registry.encode_payload(&envelope).unwrap_err();
        assert!(matches!(err, RegistryError::NoCodec(_)));
    }

    #[test]
    fn schema_is_stored() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "text": { "type": "string" },
                "severity": { "type": "integer" }
            }
        });
        let registry = PayloadRegistry::new()
            .register_with_schema::<Alert>(EventKind::from_static("alert.text"), schema.clone());

        let stored = registry
            .lookup(&EventKind::from_static("alert.text"))
            .and_then(|e| e.schema())
            .cloned();
        assert_eq!(stored, Some(schema));
    }

    #[test]
    fn re_registering_same_type_is_idempotent() {
        let _registry = PayloadRegistry::new()
            .register_type::<Alert>(EventKind::from_static("alert.text"))
            .register_type::<Alert>(EventKind::from_static("alert.text"));
        // No panic.
    }

    #[test]
    #[should_panic(expected = "PayloadRegistry conflict")]
    fn re_registering_different_type_panics() {
        let _registry = PayloadRegistry::new()
            .register_type::<Alert>(EventKind::from_static("alert.text"))
            .register_type::<Reading>(EventKind::from_static("alert.text"));
    }

    #[test]
    fn registered_kinds_iterator() {
        let registry = PayloadRegistry::new()
            .register_type::<Alert>(EventKind::from_static("alert.text"))
            .register_type::<Reading>(EventKind::from_static("sensor.reading"));
        let mut kinds: Vec<&str> = registry.registered_kinds().map(|k| k.as_str()).collect();
        kinds.sort();
        assert_eq!(kinds, vec!["alert.text", "sensor.reading"]);
    }
}
