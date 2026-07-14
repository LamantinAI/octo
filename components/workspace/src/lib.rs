//! Workspace confinement — the shared safety primitive for Octo's file
//! faculties (`octo-code`, `octo-connector-storage`, `octo-connector-telegram`).
//!
//! All of them read and write files under a single **workspace root** and must
//! never let a path escape it. Centralizing the jail here means one
//! implementation to audit, not three that can drift.
//!
//! Two guarantees:
//! - **Path confinement** ([`resolve_in_root`]): a purely lexical check that
//!   rejects absolute paths, any `..` that would climb above the root, and the
//!   empty path. Lexical (no filesystem access) so it is valid for a path about
//!   to be created.
//! - **Atomic, anti-symlink writes** ([`write_atomic`]): write to a unique temp
//!   sibling with `O_EXCL` + `O_NOFOLLOW` at `0o600`, then `rename` over the
//!   destination — replacing a symlink at the target rather than following it.
//!
//! Known limitation: [`resolve_in_root`] is lexical, so a *symlink component
//! inside the root* pointing outward is not caught on read. Per-component
//! `O_NOFOLLOW` / `openat2(RESOLVE_BENEATH)` hardening is deferred to where real
//! isolation lives (forkd).

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use thiserror::Error;

/// Environment variable naming the workspace root when none is pinned.
pub const WORKSPACE_ENV: &str = "OCTO_CODE_WORKSPACE";

/// Prefix of atomic-write temp files; consumers hide these from listings/walks.
pub const TMP_PREFIX: &str = ".octo-tmp.";

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("path `{0}` is absolute; only paths relative to the workspace root are allowed")]
    Absolute(String),
    #[error("path `{0}` escapes the workspace root")]
    Escape(String),
    #[error("empty path")]
    Empty,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Resolve the workspace root: the `pinned` path if given, else
/// `$OCTO_CODE_WORKSPACE`, else `<tmp>/octo-code`. Created if missing and
/// canonicalized so confinement checks operate on a real, absolute base.
pub fn workspace_root(pinned: Option<&Path>) -> Result<PathBuf, WorkspaceError> {
    let root = pinned
        .map(Path::to_path_buf)
        .or_else(|| std::env::var_os(WORKSPACE_ENV).map(PathBuf::from))
        .unwrap_or_else(|| std::env::temp_dir().join("octo-code"));
    std::fs::create_dir_all(&root)?;
    Ok(root.canonicalize()?)
}

/// Normalize `rel` to a root-relative path, rejecting absolute paths and any
/// `..` that climbs above the root. The result may be empty (meaning the root
/// itself); callers that address a *file* use [`resolve_file_in_root`], which
/// rejects that.
fn normalize(rel: &str) -> Result<PathBuf, WorkspaceError> {
    let mut out = PathBuf::new();
    for comp in Path::new(rel).components() {
        match comp {
            Component::Normal(c) => out.push(c),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    return Err(WorkspaceError::Escape(rel.to_string()));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(WorkspaceError::Absolute(rel.to_string()));
            }
        }
    }
    Ok(out)
}

/// Resolve `rel` against `root`, guaranteeing the result stays within `root`.
///
/// Purely lexical — does not touch the filesystem. Rejects absolute paths and
/// any `..` that would climb above the root; `.` and interior `..` normalize
/// away. An **empty** `rel` resolves to the root itself (for directory scoping);
/// use [`resolve_file_in_root`] when a file path is required.
pub fn resolve_in_root(root: &Path, rel: &str) -> Result<PathBuf, WorkspaceError> {
    Ok(root.join(normalize(rel)?))
}

/// Like [`resolve_in_root`], but also rejects the empty path — for addressing a
/// file (a read/write target, a storage key), which must have a name.
pub fn resolve_file_in_root(root: &Path, rel: &str) -> Result<PathBuf, WorkspaceError> {
    let out = normalize(rel)?;
    if out.as_os_str().is_empty() {
        return Err(WorkspaceError::Empty);
    }
    Ok(root.join(out))
}

/// Read the file at `rel` within `root`. Confinement-checked; a missing file is
/// [`WorkspaceError::NotFound`].
pub fn read_in_root(root: &Path, rel: &str) -> Result<Vec<u8>, WorkspaceError> {
    let path = resolve_file_in_root(root, rel)?;
    match std::fs::read(&path) {
        Ok(bytes) => Ok(bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(WorkspaceError::NotFound(rel.to_string()))
        }
        Err(e) => Err(e.into()),
    }
}

/// Confinement-checked [`write_atomic`] to `rel` within `root`; returns the
/// resolved destination.
pub fn write_in_root(root: &Path, rel: &str, bytes: &[u8]) -> Result<PathBuf, WorkspaceError> {
    let dest = resolve_file_in_root(root, rel)?;
    write_atomic(&dest, bytes)?;
    Ok(dest)
}

/// Atomically create-or-replace the file at `dest`.
///
/// Writes to a unique temp sibling opened with `O_EXCL` (anti-race) +
/// `O_NOFOLLOW` (anti-symlink) at mode `0o600`, fsyncs, then `rename`s over the
/// destination — replacing a pre-existing symlink at the target instead of
/// following it. Creates parent directories as needed.
pub fn write_atomic(dest: &Path, bytes: &[u8]) -> Result<(), WorkspaceError> {
    let parent = dest.parent().map(Path::to_path_buf).unwrap_or_default();
    std::fs::create_dir_all(&parent)?;
    let tmp = unique_tmp(&parent);
    let result = write_and_rename(&tmp, dest, bytes);
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn write_and_rename(tmp: &Path, dest: &Path, bytes: &[u8]) -> Result<(), WorkspaceError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true) // O_CREAT | O_EXCL
        .custom_flags(libc::O_NOFOLLOW)
        .mode(0o600)
        .open(tmp)?;
    file.write_all(bytes)?;
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
    fn rejects_absolute_and_escape() {
        let (_g, root) = root();
        assert!(matches!(resolve_in_root(&root, "/etc/passwd"), Err(WorkspaceError::Absolute(_))));
        assert!(matches!(resolve_in_root(&root, "../secret"), Err(WorkspaceError::Escape(_))));
        assert!(matches!(resolve_in_root(&root, "a/../../secret"), Err(WorkspaceError::Escape(_))));
    }

    #[test]
    fn empty_is_root_for_dirs_but_rejected_for_files() {
        let (_g, root) = root();
        // Directory scoping: empty resolves to the root itself.
        assert_eq!(resolve_in_root(&root, "").unwrap(), root);
        // A file target must have a name.
        assert!(matches!(resolve_file_in_root(&root, ""), Err(WorkspaceError::Empty)));
    }

    #[test]
    fn allows_interior_paths() {
        let (_g, root) = root();
        assert_eq!(resolve_in_root(&root, "a/b.txt").unwrap(), root.join("a/b.txt"));
        assert_eq!(resolve_in_root(&root, "./a/../b.txt").unwrap(), root.join("b.txt"));
    }

    #[test]
    fn write_read_roundtrips() {
        let (_g, root) = root();
        let dest = write_in_root(&root, "notes/todo.txt", b"hello").unwrap();
        assert!(dest.starts_with(&root));
        assert_eq!(read_in_root(&root, "notes/todo.txt").unwrap(), b"hello");
    }

    #[test]
    fn write_replaces_and_is_mode_600() {
        let (_g, root) = root();
        write_in_root(&root, "f.txt", b"one").unwrap();
        let dest = write_in_root(&root, "f.txt", b"two").unwrap();
        assert_eq!(read_in_root(&root, "f.txt").unwrap(), b"two");
        let mode = std::fs::metadata(&dest).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn read_missing_is_not_found() {
        let (_g, root) = root();
        assert!(matches!(read_in_root(&root, "nope.txt"), Err(WorkspaceError::NotFound(_))));
    }

    #[test]
    fn write_refuses_escape() {
        let (_g, root) = root();
        assert!(write_in_root(&root, "../evil.txt", b"x").is_err());
    }

    #[test]
    fn write_replaces_symlink_without_following() {
        let (_g, root) = root();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let link = root.join("link.txt");
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();

        write_in_root(&root, "link.txt", b"safe").unwrap();

        assert_eq!(std::fs::read(&link).unwrap(), b"safe");
        assert!(!std::fs::symlink_metadata(&link).unwrap().file_type().is_symlink());
        assert_eq!(std::fs::read(outside.path()).unwrap(), b"");
    }
}
