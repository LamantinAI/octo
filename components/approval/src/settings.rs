use std::time::Duration;

use serde::Deserialize;

use crate::Authority;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthoritySettings {
    pub ingress_ttl_seconds: u64,
    pub max_pending_invocations: usize,
    pub decision_timeout_seconds: u64,
}
impl Default for AuthoritySettings {
    fn default() -> Self {
        Self {
            ingress_ttl_seconds: 3600,
            max_pending_invocations: 4096,
            decision_timeout_seconds: 30,
        }
    }
}
impl AuthoritySettings {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=86_400).contains(&self.ingress_ttl_seconds)
            || !(1..=65_536).contains(&self.max_pending_invocations)
            || !(1..=300).contains(&self.decision_timeout_seconds)
        {
            return Err("authorization limits: ingress_ttl_seconds 1..86400, max_pending_invocations 1..65536, decision_timeout_seconds 1..300".into());
        }
        Ok(())
    }
    pub fn build(&self) -> Authority {
        Authority::new(
            Duration::from_secs(self.ingress_ttl_seconds),
            self.max_pending_invocations,
        )
    }
    pub fn decision_timeout(&self) -> Duration {
        Duration::from_secs(self.decision_timeout_seconds)
    }
}
