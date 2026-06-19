//! Control-plane envelope kinds.
//!
//! Signals an inhabitant (typically the cogitator) emits onto the bus to act on
//! the **environment itself** — restart one connector, or restart the whole
//! process. The runtime's control listener carries them out. This is what lets
//! an agent restart *itself* after applying config (the gap OpenClaw has): it
//! emits a normal envelope; the environment executes it.
//!
//! - [`RESTART_CONNECTOR`] — payload is the connector id (`String`). That
//!   connector is gracefully stopped and re-spawned by its supervisor (with
//!   whatever config it now loads).
//! - [`RESTART_PROCESS`] — the runtime shuts down cleanly; a process supervisor
//!   (systemd `Restart=always`) brings it back with fresh config.

/// Restart a single connector. Payload: its id as a `String`.
pub const RESTART_CONNECTOR: &str = "octo.control.restart_connector";

/// Restart the whole process (graceful shutdown → external supervisor revives).
pub const RESTART_PROCESS: &str = "octo.control.restart_process";

/// Glob matching every control kind — for the runtime's listener subscription.
pub const CONTROL_GLOB: &str = "octo.control.**";
