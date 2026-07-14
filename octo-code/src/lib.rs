//! `octo-code` — a file/code faculty for Octo cogitators, exposed as rig tools.
//!
//! These are **in-process, synchronous local faculties** (like scratchpad/kaeru),
//! not bus connectors: files are a local capability the cogitator reaches
//! directly. octo-rig opts them in behind its `code` feature.
//!
//! Phase 2 (now): file tools — `read` / `write` / `edit` / `list` / `glob` /
//! `grep` — jailed to a single workspace root ([`workspace`]). Folder
//! confinement is the safety model here; running untrusted code (`bash`/`exec`)
//! is a separate concern owned by forkd. See research `file_code_tooling` and
//! `forkd_isolation_architecture`.
//!
//! The file tools are ported from `llm-coding-tools-rig` (Apache-2.0) to rig
//! 0.35 and reshaped to Octo conventions, with attribution.

mod macros;
mod search;
mod tools;
mod workspace;

pub use search::{GlobTool, GrepTool};
pub use tools::{EditTool, ListTool, ReadTool, WriteTool};
pub use workspace::{
    read_within, resolve_in_root, workspace_root, write_atomic, WorkspaceError, WORKSPACE_ENV,
};

/// Crate-wide lock for tests that set the process-global `OCTO_CODE_WORKSPACE`.
/// One lock across all modules so tool tests never run concurrently and stomp on
/// each other's root.
#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
