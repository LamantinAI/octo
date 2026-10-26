use serde::{Deserialize, Serialize};

/// Assembly-owned lifetime identity. It grants no principal or capability and
/// knows nothing about chats, sessions, transport kinds or storage backends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeBinding {
    pub opaque_id: String,
    pub epoch: String,
}

impl ScopeBinding {
    pub fn new(opaque_id: impl Into<String>, epoch: impl Into<String>) -> Result<Self, String> {
        let binding = Self {
            opaque_id: opaque_id.into(),
            epoch: epoch.into(),
        };
        binding.validate()?;
        Ok(binding)
    }
    pub(crate) fn validate(&self) -> Result<(), String> {
        if [&self.opaque_id, &self.epoch].iter().any(|value| {
            value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control)
        }) {
            Err("Invalid authority scope binding".into())
        } else {
            Ok(())
        }
    }
}

/// Synchronous cache checkpoint only: implementations must not query a database,
/// await async locks or acquire an assembly's lifecycle writer here. A successful
/// check linearizes admission before close; it cannot undo an admitted effect.
pub trait ScopeValidator: Send + Sync {
    fn allows(&self, binding: &ScopeBinding) -> bool;
}
