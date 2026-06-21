//! Route — a single entry in the router's table.
//!
//! A route is data: a predicate over envelopes (`when`) plus an action
//! (`then`) describing what envelope to emit when the predicate matches.
//! Routes are serialisable so they can live in TOML and be mutated by file
//! operations (see `runtime_config` vault draft).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{bus::KindPattern, ChannelId, ConnectorId, EventKind, Priority};

/// Stable identifier for a route; used for mutations (remove / disable / enable).
pub type RouteId = String;

/// One route in the router's table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Route {
    pub id: RouteId,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub strategy: RouteStrategy,
    pub when: RoutePredicate,
    pub then: RouteAction,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

impl Route {
    /// True if the predicate matches the envelope.
    pub fn matches(&self, envelope: &crate::Envelope) -> bool {
        if !self.enabled {
            return false;
        }
        self.when.matches(envelope)
    }
}

/// What a route matches.
///
/// All set fields must match (AND). Unset (`None` or empty) fields are
/// wildcards.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoutePredicate {
    /// Glob pattern over envelope.kind.
    pub kind: Option<KindPattern>,
    /// Match only envelopes from this source connector.
    pub source: Option<ConnectorId>,
    /// Match only envelopes from this channel.
    pub channel: Option<ChannelId>,
    /// All these tags (key = value) must be present in envelope.tags.
    #[serde(default)]
    pub tags_required: HashMap<String, String>,
    /// Match only envelopes whose target equals this connector.
    pub target: Option<ConnectorId>,
    /// Match on a numeric field inside a JSON (`serde_json::Value`) payload —
    /// e.g. "celsius > 80". Lets the reflex tier express thresholds declaratively
    /// (routine reading vs anomaly) without a custom classifier. Only matches
    /// when the envelope's payload is a `serde_json::Value` and the pointed-at
    /// field is numeric.
    pub payload: Option<PayloadPredicate>,
}

/// A numeric comparison against a field in a JSON payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadPredicate {
    /// JSON Pointer to the field (e.g. `/celsius`, `/reading/value`).
    pub pointer: String,
    /// Comparison operator.
    pub op: NumOp,
    /// Right-hand side of the comparison.
    pub value: f64,
}

/// Numeric comparison operators for [`PayloadPredicate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NumOp {
    /// `>`
    Gt,
    /// `>=`
    Gte,
    /// `<`
    Lt,
    /// `<=`
    Lte,
    /// `==`
    Eq,
}

impl PayloadPredicate {
    /// Evaluate against an envelope. `false` unless the payload is a
    /// `serde_json::Value` whose pointed-at field is a number.
    fn matches(&self, envelope: &crate::Envelope) -> bool {
        let Some(value) = envelope.payload_as::<serde_json::Value>() else {
            return false;
        };
        let Some(field) = value.pointer(&self.pointer).and_then(|v| v.as_f64()) else {
            return false;
        };
        match self.op {
            NumOp::Gt => field > self.value,
            NumOp::Gte => field >= self.value,
            NumOp::Lt => field < self.value,
            NumOp::Lte => field <= self.value,
            NumOp::Eq => field == self.value,
        }
    }
}

impl RoutePredicate {
    pub fn matches(&self, envelope: &crate::Envelope) -> bool {
        if let Some(kind) = &self.kind {
            if !kind.matches(&envelope.kind) {
                return false;
            }
        }
        if let Some(source) = &self.source {
            if &envelope.source != source {
                return false;
            }
        }
        if let Some(channel) = &self.channel {
            match &envelope.channel {
                Some(c) if c == channel => {}
                _ => return false,
            }
        }
        if let Some(target) = &self.target {
            match &envelope.target {
                Some(t) if t == target => {}
                _ => return false,
            }
        }
        for (key, value) in &self.tags_required {
            match envelope.tags.get(key) {
                Some(v) if v == value => {}
                _ => return false,
            }
        }
        if let Some(payload) = &self.payload {
            if !payload.matches(envelope) {
                return false;
            }
        }
        true
    }
}

/// What a route emits when matched.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteAction {
    /// Connector this emission is addressed to.
    pub target: ConnectorId,

    /// Optional: rewrite the envelope's `kind` on emission. If `None`, the
    /// original kind is preserved.
    pub override_kind: Option<EventKind>,

    /// Tags to add to the emitted envelope.
    #[serde(default)]
    pub add_tags: HashMap<String, String>,

    /// If true, the payload of the original envelope is reused on emission.
    /// If false, `static_payload` must be set.
    #[serde(default = "default_copy_payload")]
    pub copy_payload: bool,

    /// Optional static JSON payload, used when `copy_payload = false`.
    /// The payload type registered for the emitted kind must be
    /// `serde_json::Value` (or compatible).
    pub static_payload: Option<serde_json::Value>,
}

fn default_copy_payload() -> bool {
    true
}

/// How the matching of a route relates to subsequent routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteStrategy {
    /// Match → emit → stop processing further routes for this envelope.
    #[default]
    Terminate,
    /// Match → emit → continue with the rest of the routes.
    Enrich,
    /// Match → record in trail but do not emit an action.
    Observe,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectorId, Envelope, EventKind};

    fn env(kind: &'static str, source: &str) -> Envelope {
        Envelope::new(
            ConnectorId::new(source),
            EventKind::from_static(kind),
            42i32,
        )
    }

    #[test]
    fn predicate_matches_by_kind_glob() {
        let predicate = RoutePredicate {
            kind: Some(KindPattern::new("vision.incident.*")),
            ..Default::default()
        };
        assert!(predicate.matches(&env("vision.incident.fight", "fluxion")));
        assert!(predicate.matches(&env("vision.incident.loitering", "fluxion")));
        assert!(!predicate.matches(&env("vision.entity.entered_zone", "fluxion")));
        assert!(!predicate.matches(&env("telegram.message", "telegram")));
    }

    #[test]
    fn predicate_combines_kind_and_source() {
        let predicate = RoutePredicate {
            kind: Some(KindPattern::new("vision.**")),
            source: Some(ConnectorId::new("fluxion")),
            ..Default::default()
        };
        assert!(predicate.matches(&env("vision.incident.fight", "fluxion")));
        assert!(!predicate.matches(&env("vision.incident.fight", "camera-2")));
    }

    #[test]
    fn predicate_requires_all_tags() {
        let mut tags = HashMap::new();
        tags.insert("severity".into(), "high".into());
        tags.insert("scope".into(), "indoor".into());
        let predicate = RoutePredicate {
            tags_required: tags,
            ..Default::default()
        };

        let mut env_full = env("vision.incident.fight", "fluxion");
        env_full
            .tags
            .insert("severity".into(), "high".into());
        env_full.tags.insert("scope".into(), "indoor".into());
        assert!(predicate.matches(&env_full));

        let mut env_partial = env("vision.incident.fight", "fluxion");
        env_partial
            .tags
            .insert("severity".into(), "high".into());
        // missing "scope"
        assert!(!predicate.matches(&env_partial));
    }

    #[test]
    fn payload_predicate_matches_numeric_threshold() {
        let predicate = RoutePredicate {
            kind: Some(KindPattern::new("sensor.reading")),
            payload: Some(PayloadPredicate {
                pointer: "/celsius".to_string(),
                op: NumOp::Gt,
                value: 80.0,
            }),
            ..Default::default()
        };

        let hot = Envelope::new(
            ConnectorId::new("sensor"),
            EventKind::from_static("sensor.reading"),
            serde_json::json!({ "celsius": 95 }),
        );
        let cool = Envelope::new(
            ConnectorId::new("sensor"),
            EventKind::from_static("sensor.reading"),
            serde_json::json!({ "celsius": 21 }),
        );
        // Wrong payload type → no match (not a JSON Value).
        let typed = Envelope::new(
            ConnectorId::new("sensor"),
            EventKind::from_static("sensor.reading"),
            95i32,
        );

        assert!(predicate.matches(&hot));
        assert!(!predicate.matches(&cool));
        assert!(!predicate.matches(&typed));
    }

    #[test]
    fn predicate_disabled_route_does_not_match() {
        let route = Route {
            id: "test".into(),
            priority: Priority::default(),
            strategy: RouteStrategy::Terminate,
            when: RoutePredicate::default(),
            then: RouteAction {
                target: ConnectorId::new("out"),
                override_kind: None,
                add_tags: HashMap::new(),
                copy_payload: true,
                static_payload: None,
            },
            enabled: false,
        };
        // matches() returns false because enabled = false
        assert!(!route.matches(&env("anything", "any")));
    }
}
