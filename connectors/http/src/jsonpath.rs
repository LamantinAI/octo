//! Minimal JSONPath subset for extracting path/query parameters from a
//! command's `serde_json::Value` payload.
//!
//! This is deliberately *not* a full JSONPath engine. The [`petstore_case`]
//! vault draft notes that the engine choice (`serde_json_path`, `jsonpath-rust`,
//! ...) is a minor task orthogonal to the dyn-connector design — so the pilot
//! ships a tiny, dependency-free evaluator covering exactly what TOML
//! `path_params` / `query_params` need:
//!
//! - `$`            — the whole payload
//! - `$.field`      — object member
//! - `$.a.b.c`      — nested members
//! - `$.field[*]`   — array fan-out (each scalar element), used for repeated
//!                    query params like `?tags=a&tags=b`
//!
//! Indexing (`[0]`), filters (`[?(...)]`), recursive descent (`..`) and the
//! like are out of scope; swapping in a real engine later is a localized change.
//!
//! [`petstore_case`]: ../../../research/drafts/petstore_case.md

use serde_json::Value;

/// A parsed JSONPath expression (subset — see module docs).
#[derive(Debug, Clone, PartialEq)]
pub struct JsonPath {
    raw: String,
    segments: Vec<Segment>,
}

#[derive(Debug, Clone, PartialEq)]
enum Segment {
    /// `.field`
    Member(String),
    /// `[*]` — fan out over array elements.
    Wildcard,
}

/// Error from parsing a JSONPath expression at connector startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid JSONPath: {}", self.0)
    }
}

impl std::error::Error for ParseError {}

impl JsonPath {
    /// Parse a JSONPath expression. Must start with `$`.
    pub fn parse(expr: &str) -> Result<Self, ParseError> {
        let rest = expr
            .strip_prefix('$')
            .ok_or_else(|| ParseError(format!("'{expr}' must start with '$'")))?;

        let mut segments = Vec::new();
        let mut chars = rest.chars().peekable();
        while let Some(&c) = chars.peek() {
            match c {
                '.' => {
                    chars.next();
                    let mut name = String::new();
                    while let Some(&nc) = chars.peek() {
                        if nc == '.' || nc == '[' {
                            break;
                        }
                        name.push(nc);
                        chars.next();
                    }
                    if name.is_empty() {
                        return Err(ParseError(format!("empty member name in '{expr}'")));
                    }
                    segments.push(Segment::Member(name));
                }
                '[' => {
                    chars.next();
                    // Only `[*]` is supported.
                    let mut inner = String::new();
                    for nc in chars.by_ref() {
                        if nc == ']' {
                            break;
                        }
                        inner.push(nc);
                    }
                    if inner.trim() == "*" {
                        segments.push(Segment::Wildcard);
                    } else {
                        return Err(ParseError(format!(
                            "only '[*]' is supported, got '[{inner}]' in '{expr}'"
                        )));
                    }
                }
                other => {
                    return Err(ParseError(format!(
                        "unexpected character '{other}' in '{expr}'"
                    )));
                }
            }
        }

        Ok(Self {
            raw: expr.to_string(),
            segments,
        })
    }

    /// The original expression text.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Evaluate against a payload, returning every matched value as a URL-ready
    /// string. A single-valued path yields one element; a path ending in `[*]`
    /// (or crossing an array) yields one element per array entry. An empty
    /// result means the path did not resolve.
    pub fn extract(&self, root: &Value) -> Vec<String> {
        let mut frontier = vec![root];
        for seg in &self.segments {
            let mut next = Vec::new();
            for v in frontier {
                match seg {
                    Segment::Member(name) => {
                        if let Some(child) = v.get(name) {
                            next.push(child);
                        }
                    }
                    Segment::Wildcard => {
                        if let Some(arr) = v.as_array() {
                            next.extend(arr.iter());
                        }
                    }
                }
            }
            frontier = next;
        }
        frontier.iter().filter_map(|v| scalar_to_string(v)).collect()
    }

    /// Convenience: the first matched value, if any.
    pub fn extract_one(&self, root: &Value) -> Option<String> {
        self.extract(root).into_iter().next()
    }
}

/// Render a JSON scalar as a string suitable for a URL path/query segment.
/// Objects and arrays are rejected (return `None`) — only scalars are valid
/// path/query values.
fn scalar_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        Value::Array(_) | Value::Object(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_requires_dollar() {
        assert!(JsonPath::parse("id").is_err());
        assert!(JsonPath::parse("$.id").is_ok());
        assert!(JsonPath::parse("$").is_ok());
    }

    #[test]
    fn extract_simple_member() {
        let p = JsonPath::parse("$.id").unwrap();
        assert_eq!(p.extract_one(&json!({"id": 42})), Some("42".to_string()));
        assert_eq!(
            p.extract_one(&json!({"id": "abc"})),
            Some("abc".to_string())
        );
    }

    #[test]
    fn extract_nested_member() {
        let p = JsonPath::parse("$.pet.status").unwrap();
        assert_eq!(
            p.extract_one(&json!({"pet": {"status": "available"}})),
            Some("available".to_string())
        );
    }

    #[test]
    fn extract_missing_is_empty() {
        let p = JsonPath::parse("$.missing").unwrap();
        assert!(p.extract(&json!({"id": 1})).is_empty());
    }

    #[test]
    fn extract_wildcard_array() {
        let p = JsonPath::parse("$.tags[*]").unwrap();
        assert_eq!(
            p.extract(&json!({"tags": ["a", "b", "c"]})),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn whole_root_when_just_dollar() {
        let p = JsonPath::parse("$").unwrap();
        assert_eq!(p.extract_one(&json!("scalar")), Some("scalar".to_string()));
        // A whole object is not a scalar → no string.
        assert!(p.extract(&json!({"a": 1})).is_empty());
    }

    #[test]
    fn rejects_unsupported_index() {
        assert!(JsonPath::parse("$.tags[0]").is_err());
    }
}
