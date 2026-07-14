//! Thin adapter over the shared [`octo_workspace`] jail. octo-code always reads
//! its root from the environment (`$OCTO_CODE_WORKSPACE`), so it exposes a
//! no-argument [`workspace_root`]; everything else is re-exported.

use std::path::{Path, PathBuf};

pub use octo_workspace::{resolve_in_root, WorkspaceError, TMP_PREFIX, WORKSPACE_ENV};

/// The workspace root from the environment (octo-code never pins it).
pub fn workspace_root() -> Result<PathBuf, WorkspaceError> {
    octo_workspace::workspace_root(None)
}

/// Read a file at `rel` within `root`.
pub fn read_within(root: &Path, rel: &str) -> Result<Vec<u8>, WorkspaceError> {
    octo_workspace::read_in_root(root, rel)
}

/// Atomically create-or-replace `rel` within `root`; returns the resolved path.
pub fn write_atomic(root: &Path, rel: &str, bytes: &[u8]) -> Result<PathBuf, WorkspaceError> {
    octo_workspace::write_in_root(root, rel, bytes)
}
