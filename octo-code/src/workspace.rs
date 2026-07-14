//! Workspace confinement — the safety foundation for every octo-code file tool.
//!
//! All file access resolves through a single **workspace root** and is refused if
//! it would escape that root. This folder-level jail *is* the safety model for
//! phase 2 (read/write/edit/list); running untrusted code is a separate concern
//! handled later by forkd (see research `forkd_isolation_architecture`).
//!
//! Two guarantees:
//! - **Path confinement** ([`resolve_in_root`]): a purely lexical check that
//!   rejects absolute paths and any `..` that would climb above the root. Lexical
//!   (no filesystem access) so it is safe on paths that don't exist yet.
//! - **Atomic, anti-symlink writes** ([`write_atomic`]): write to a unique temp
//!   sibling with `O_EXCL` + `O_NOFOLLOW` at `0o600`, then `rename` over the
//!   destination — replacing a symlink at the target rather than following it.
//!
//! Known v0 limitation: [`resolve_in_root`] is lexical, so a *symlink component
//! inside the root* pointing outward is not caught on read. Per-component
//! `O_NOFOLLOW` / `openat2(RESOLVE_BENEATH)` hardening is deferred until forkd,
//! where the real isolation lives.

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

/// Environment variable naming the workspace root. Tools read it per call
/// (they are stateless — see the `rig_tool` macro constraint), defaulting to a
/// scratch dir under the system temp directory.
pub const WORKSPACE_ENV: &str = "OCTO_CODE_WORKSPACE";

/// Prefix of the atomic-write temp files; tools hide these from listings/walks.
pub(crate) const TMP_PREFIX: &str = ".octo-code.";

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("path `{0}` is absolute; only paths relative to the workspace root are allowed")]
    Absolute(String),
    #[error("path `{0}` escapes the workspace root")]
    Escape(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// The workspace root: `$OCTO_CODE_WORKSPACE`, else `<tmp>/octo-code`. The
/// directory is created if missing and the path canonicalized so that the
/// confinement checks operate on a real, absolute base.
pub fn workspace_root() -> Result<PathBuf, WorkspaceError> {
    let root = std::env::var_os(WORKSPACE_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("octo-code"));
    std::fs::create_dir_all(&root)?;
    Ok(root.canonicalize()?)
}

/// Resolve `rel` against `root`, guaranteeing the result stays within `root`.
///
/// Purely lexical — does not touch the filesystem — so it is valid for a path
/// about to be created. Rejects absolute paths and any `..` that would climb
/// above the root; `.` and interior `..` that stay inside are normalized away.
pub fn resolve_in_root(root: &Path, rel: &str) -> Result<PathBuf, WorkspaceError> {
    let candidate = Path::new(rel);
    let mut out = PathBuf::new();
    for comp in candidate.components() {
        match comp {
            Component::Normal(c) => out.push(c),
            Component::CurDir => {}
            Component::ParentDir => {
                // Only pop a component we actually pushed; never climb above root.
                if !out.pop() {
                    return Err(WorkspaceError::Escape(rel.to_string()));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(WorkspaceError::Absolute(rel.to_string()));
            }
        }
    }
    Ok(root.join(out))
}

/// Read the file at `rel` within `root`. Confinement-checked.
pub fn read_within(root: &Path, rel: &str) -> Result<Vec<u8>, WorkspaceError> {
    let path = resolve_in_root(root, rel)?;
    Ok(std::fs::read(path)?)
}

/// Atomically create-or-replace the file at `rel` within `root`.
///
/// Writes to a unique temp sibling opened with `O_EXCL` (anti-race) +
/// `O_NOFOLLOW` (anti-symlink) at mode `0o600`, fsyncs, then `rename`s over the
/// destination. The rename replaces a pre-existing symlink at the target instead
/// of following it. Returns the resolved destination path.
pub fn write_atomic(root: &Path, rel: &str, contents: &[u8]) -> Result<PathBuf, WorkspaceError> {
    let dest = resolve_in_root(root, rel)?;
    let parent = dest.parent().map(Path::to_path_buf).unwrap_or_else(|| root.to_path_buf());
    std::fs::create_dir_all(&parent)?;

    let tmp = unique_tmp(&parent);
    let result = write_and_rename(&tmp, &dest, contents);
    if result.is_err() {
        // Best-effort cleanup of the temp file on any failure.
        let _ = std::fs::remove_file(&tmp);
    }
    result.map(|()| dest)
}

fn write_and_rename(tmp: &Path, dest: &Path, contents: &[u8]) -> Result<(), WorkspaceError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true) // O_CREAT | O_EXCL
        .custom_flags(libc::O_NOFOLLOW)
        .mode(0o600)
        .open(tmp)?;
    file.write_all(contents)?;
    file.sync_all()?;
    std::fs::rename(tmp, dest)?;
    Ok(())
}

fn unique_tmp(parent: &Path) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    parent.join(format!("{}{}.{}.tmp", TMP_PREFIX, std::process::id(), n))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn root() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        (dir, root)
    }

    #[test]
    fn rejects_absolute() {
        let (_g, root) = root();
        assert!(matches!(
            resolve_in_root(&root, "/etc/passwd"),
            Err(WorkspaceError::Absolute(_))
        ));
    }

    #[test]
    fn rejects_parent_escape() {
        let (_g, root) = root();
        assert!(matches!(
            resolve_in_root(&root, "../secret"),
            Err(WorkspaceError::Escape(_))
        ));
        assert!(matches!(
            resolve_in_root(&root, "a/../../secret"),
            Err(WorkspaceError::Escape(_))
        ));
    }

    #[test]
    fn allows_interior_paths() {
        let (_g, root) = root();
        assert_eq!(resolve_in_root(&root, "a/b.txt").unwrap(), root.join("a/b.txt"));
        // `.` and an interior `..` that stays inside normalize away.
        assert_eq!(resolve_in_root(&root, "./a/../b.txt").unwrap(), root.join("b.txt"));
    }

    #[test]
    fn write_then_read_roundtrips() {
        let (_g, root) = root();
        let dest = write_atomic(&root, "notes/todo.txt", b"hello").unwrap();
        assert!(dest.starts_with(&root));
        assert_eq!(read_within(&root, "notes/todo.txt").unwrap(), b"hello");
    }

    #[test]
    fn write_replaces_existing() {
        let (_g, root) = root();
        write_atomic(&root, "f.txt", b"one").unwrap();
        write_atomic(&root, "f.txt", b"two").unwrap();
        assert_eq!(read_within(&root, "f.txt").unwrap(), b"two");
    }

    #[test]
    fn write_is_mode_600() {
        let (_g, root) = root();
        let dest = write_atomic(&root, "f.txt", b"x").unwrap();
        let mode = std::fs::metadata(&dest).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn write_refuses_escape() {
        let (_g, root) = root();
        assert!(write_atomic(&root, "../evil.txt", b"x").is_err());
    }

    #[test]
    fn write_replaces_symlink_without_following() {
        let (_g, root) = root();
        // Plant a symlink at the destination name pointing outside the root.
        let outside = tempfile::NamedTempFile::new().unwrap();
        let link = root.join("link.txt");
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();

        write_atomic(&root, "link.txt", b"safe").unwrap();

        // The link name now holds our file; the outside target is untouched.
        assert_eq!(std::fs::read(&link).unwrap(), b"safe");
        assert!(!std::fs::symlink_metadata(&link).unwrap().file_type().is_symlink());
        assert_eq!(std::fs::read(outside.path()).unwrap(), b"");
    }
}
