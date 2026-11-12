use std::env::var;

/// Resolve only explicit references. Implementations must not expose resolved
/// values in Debug, status metadata or errors.
pub trait Credentials: Send + Sync {
    fn resolve(&self, reference: &str) -> Option<String>;
}
pub struct EnvironmentCredentials;
impl Credentials for EnvironmentCredentials {
    fn resolve(&self, reference: &str) -> Option<String> {
        var(reference).ok().filter(|value| !value.is_empty())
    }
}
