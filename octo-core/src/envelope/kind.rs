//! Event kind — typed identifier with dot-separated namespace and glob matching.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

/// Event kind — dot-separated namespaced identifier.
///
/// Examples: `vision.incident.fight`, `telegram.command`, `mqtt.factory.temperature`.
///
/// Kinds support glob matching for filters and reflex predicates:
/// - `vision.*` matches any single segment after `vision`.
/// - `vision.**` matches any tail (including empty).
/// - Literal segments must match exactly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventKind(Cow<'static, str>);

impl EventKind {
    /// Construct from a `&'static str` without allocation.
    pub const fn from_static(s: &'static str) -> Self {
        Self(Cow::Borrowed(s))
    }

    /// Construct from an owned or borrowed string.
    pub fn new(s: impl Into<String>) -> Self {
        Self(Cow::Owned(s.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Match this kind against a glob pattern.
    ///
    /// Pattern segments:
    /// - `**` matches the rest of the kind (including zero segments).
    /// - `*` matches exactly one segment.
    /// - Literal segments match exactly.
    pub fn matches(&self, pattern: &str) -> bool {
        let kind_segs: Vec<&str> = self.0.split('.').collect();
        let pat_segs: Vec<&str> = pattern.split('.').collect();

        for (i, pat) in pat_segs.iter().enumerate() {
            if *pat == "**" {
                return true;
            }
            if i >= kind_segs.len() {
                return false;
            }
            if *pat == "*" {
                continue;
            }
            if *pat != kind_segs[i] {
                return false;
            }
        }
        pat_segs.len() == kind_segs.len()
    }
}

impl std::fmt::Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&'static str> for EventKind {
    fn from(s: &'static str) -> Self {
        Self::from_static(s)
    }
}

impl From<String> for EventKind {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_exact() {
        let k = EventKind::from_static("vision.entity.entered_zone");
        assert!(k.matches("vision.entity.entered_zone"));
        assert!(!k.matches("vision.entity.left_zone"));
        assert!(!k.matches("vision.entity"));
    }

    #[test]
    fn matches_single_wildcard() {
        let k = EventKind::from_static("vision.entity.entered_zone");
        assert!(k.matches("vision.*.entered_zone"));
        assert!(k.matches("vision.entity.*"));
        assert!(!k.matches("*.*"));
        assert!(!k.matches("vision.*"));
    }

    #[test]
    fn matches_double_wildcard_tail() {
        let k = EventKind::from_static("vision.entity.entered_zone");
        assert!(k.matches("vision.**"));
        assert!(k.matches("**"));
        assert!(k.matches("vision.entity.**"));

        let k2 = EventKind::from_static("vision");
        assert!(k2.matches("vision.**"));
    }

    #[test]
    fn no_match_different_root() {
        let k = EventKind::from_static("vision.entity.entered_zone");
        assert!(!k.matches("telegram.**"));
        assert!(!k.matches("vision.incident.*"));
    }
}
