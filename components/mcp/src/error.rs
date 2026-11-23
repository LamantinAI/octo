use thiserror::Error;

/// Categories intentionally omit raw URLs, credentials, subprocess output and
/// server errors that may repeat transport secrets.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum McpError {
    #[error("Invalid MCP server configuration.")]
    Configuration,
    #[error("An MCP credential reference is missing or empty.")]
    CredentialUnavailable,
    #[error("Could not establish the MCP session.")]
    Connection,
    #[error("MCP discovery exceeded its configured count, size or time limit.")]
    DiscoveryLimit,
    #[error("The MCP server advertised an invalid or duplicate tool definition.")]
    Protocol,
    #[error("The MCP tool contract changed; refresh the allowed tool snapshot before continuing.")]
    SchemaChanged,
    #[error("The requested MCP tool is not present in this snapshot.")]
    UnknownTool,
    #[error("MCP tool arguments must be an object within the configured request size.")]
    InvalidArguments,
    #[error("MCP invocation was cancelled before it started.")]
    Cancelled,
    #[error("MCP invocation timed out before it started.")]
    Timeout,
    #[error("The MCP connection is closed.")]
    Closed,
    #[error("The server rejected the MCP request; no automatic retry was attempted.")]
    Rejected,
    #[error(
        "The MCP invocation outcome is indeterminate; effects may already have happened. Do not replay it."
    )]
    Indeterminate,
    #[error(
        "An MCP result was received but exceeded the configured output limit. Do not repeat an effect to recover its output."
    )]
    ResultTooLarge,
    #[error("The MCP session did not finish shutting down within its configured deadline.")]
    ShutdownTimeout,
}
pub type Result<T> = std::result::Result<T, McpError>;
