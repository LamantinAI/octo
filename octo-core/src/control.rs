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
//! - [`CANCEL`] — abort in-flight work carrying a matching [`CANCEL_SCOPE_TAG`].
//!   Unlike the restarts (executed by the runtime's control listener), a CANCEL is
//!   honoured by the *long-running connectors themselves* (forkd): each aborts the
//!   run whose scope matches and kills its process group. The runtime does not act
//!   on it — it is a connector-directed control signal that rides the same plane.

/// Restart a single connector. Payload: its id as a `String`.
pub const RESTART_CONNECTOR: &str = "octo.control.restart_connector";

/// Restart the whole process (graceful shutdown → external supervisor revives).
pub const RESTART_PROCESS: &str = "octo.control.restart_process";

/// Cancel in-flight work tagged with a scope. Payload: the scope id as a `String`.
/// A connector honouring this aborts every run it started carrying a
/// [`CANCEL_SCOPE_TAG`] equal to the payload, killing the process group. The scope
/// is an opaque token the emitter chooses (an agent turn id) — the runtime ascribes
/// no meaning to it; only matching connectors do.
pub const CANCEL: &str = "octo.control.cancel";

/// Envelope tag naming the cancellation scope a dispatched command belongs to. The
/// emitter stamps it on commands it may later cancel; a connector honouring [`CANCEL`]
/// registers in-flight work under this value and aborts it when a matching CANCEL
/// arrives. Absent → the run is simply not cancellable by scope.
pub const CANCEL_SCOPE_TAG: &str = "cancel_scope";

/// Glob matching every control kind — for the runtime's listener subscription.
pub const CONTROL_GLOB: &str = "octo.control.**";
