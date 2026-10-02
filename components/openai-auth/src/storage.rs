use std::path::{Path, PathBuf};

/// Write `bytes` to `path` atomically at `0600`: into a sibling temp file created at
/// `0600` (no world-readable window), flushed, then renamed over the store. A crash
/// mid-write leaves the old store intact — which matters, because a refresh can rotate the
/// single-use refresh token: a torn write would lose the new one after spending the old.
#[cfg(unix)]
pub(super) fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::{
        fs::{OpenOptions, rename, set_permissions},
        io::Write,
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
    };
    let tmp = temp_sibling(path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    // A stale temp from an earlier crash keeps its old mode; tighten before writing.
    set_permissions(&tmp, PermissionsExt::from_mode(0o600))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    rename(&tmp, path)
}

#[cfg(not(unix))]
pub(super) fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = temp_sibling(path);
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

/// `auth.json` -> `auth.json.tmp`, in the same directory (so the rename stays atomic).
pub(super) fn temp_sibling(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}
