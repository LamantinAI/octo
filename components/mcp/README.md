# octo-mcp

Reusable MCP client sessions for agent harnesses. This component uses the pinned
RMCP SDK for Streamable HTTP and stdio; it has no Octo bus, Connector, Rig or Albert
dependency. Tool schemas and full SDK results remain protocol data.

`ServerSession::connect` takes transport settings, a configuration base directory,
client limits, a credential resolver and a **server-lifecycle** cancellation token.
An HTTP bearer or stdio environment value is resolved from an explicit reference.
Stdio clears inherited environment except the configured baseline and mappings;
subprocess stderr is suppressed. HTTP redirects and expired-session call replay
are disabled. Connection failures expose categories, not raw headers or secrets.

`snapshot()` returns immutable tool definitions and a deterministic contract
revision. Discovery follows cursors with count, page, byte and deadline limits.
`call(revision, name, arguments, cancellation)` checks the advertised contract and
invokes a tool directly. A per-call token belongs to that invocation, rather than
the shared server lifecycle. The harness remains responsible for current actor
permissions and its enabled subset. MCP annotations do not grant authority.

Timeout, cancellation or dropping an already-started call can leave its effects
indeterminate. Such calls are never replayed. Later calls can reconnect, checking
the contract again; schema drift requires explicit `refresh()`. An invalidated
connection retires until its surviving calls settle, so interrupting one root call
does not cancel a child sharing the transport. A bounded retirement backlog can
reject reconnect rather than evict live calls. Explicit `close()` cancels every
generation and waits within the shutdown deadline.

`CallToolResult`, `JsonObject` and `Tool` deliberately follow this component's pinned
SDK version. Structured content, non-text content and `is_error` are preserved.
Argument and result ceilings are checked after serialization/decoding; the SDK
owns transport allocation. These checks are not a streaming memory bound. Large
received results have a distinct error from an indeterminate invocation; an effect
must not be repeated merely to recover an oversized result.

The local tests exercise HTTP, pagination, credentials, redirects, schema drift,
concurrent cancellation and a real stdio process. The stdio fixture requires
Python 3; process reaping is additionally checked on Linux.
