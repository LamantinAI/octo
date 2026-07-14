//! Shared-workspace access for file transfer.
//!
//! Files move **by reference, not through the model**: an incoming document is
//! saved into the shared workspace and the cogitator is handed its path; an
//! outbound `chat.send_file` names a workspace path the connector loads and
//! sends. The workspace root is the SAME one octo-code / storage use (pinned via
//! the manifest `workspace`, else `$OCTO_CODE_WORKSPACE`, else `<tmp>/octo-code`),
//! so a file the agent edited with octo-code can be sent from here, and an
//! incoming file lands where octo-code can read it.
//!
//! NOTE: the path-jail + root resolution here is duplicated from `octo-code` and
//! `octo-connector-storage`; a shared `workspace` crate should absorb all three
//! (a security primitive worth centralizing).

use std::path::{Component, Path, PathBuf};

/// Subdirectory under the workspace where incoming files land.
pub(crate) const INBOX: &str = "inbox";

/// Resolve the workspace root: the pinned path, else `$OCTO_CODE_WORKSPACE`, else
/// the octo-code default `<tmp>/octo-code`. Created if missing, canonicalized.
pub(crate) fn workspace_root(pinned: &Option<PathBuf>) -> std::io::Result<PathBuf> {
    let root = pinned
        .clone()
        .or_else(|| std::env::var_os("OCTO_CODE_WORKSPACE").map(PathBuf::from))
        .unwrap_or_else(|| std::env::temp_dir().join("octo-code"));
    std::fs::create_dir_all(&root)?;
    root.canonicalize()
}

/// Resolve `rel` within `root`, rejecting absolute paths, `..` escapes and the
/// empty path. Purely lexical.
pub(crate) fn resolve_within(root: &Path, rel: &str) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for comp in Path::new(rel).components() {
        match comp {
            Component::Normal(c) => out.push(c),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    return None;
                }
            }
            Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    if out.as_os_str().is_empty() {
        return None;
    }
    Some(root.join(out))
}

/// Save an incoming file under `inbox/<basename>`, returning its workspace-
/// relative path. The filename is reduced to its basename so a crafted name
/// can't place the file outside `inbox/`.
pub(crate) fn save_incoming(
    root: &Path,
    filename: &str,
    bytes: &[u8],
) -> std::io::Result<String> {
    let base = Path::new(filename).file_name().and_then(|s| s.to_str()).unwrap_or("file");
    let rel = format!("{INBOX}/{base}");
    let dest = resolve_within(root, &rel)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad file name"))?;
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&dest, bytes)?;
    Ok(rel)
}

/// Load a workspace file by relative path, returning its bytes and basename.
pub(crate) fn load_outgoing(root: &Path, rel: &str) -> std::io::Result<(Vec<u8>, String)> {
    let path = resolve_within(root, rel)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad path"))?;
    let bytes = std::fs::read(&path)?;
    let name = Path::new(rel).file_name().and_then(|s| s.to_str()).unwrap_or("file").to_string();
    Ok((bytes, name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_then_load_roundtrips_under_inbox() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        let rel = save_incoming(&root, "report.pdf", b"pdf-bytes").unwrap();
        assert_eq!(rel, "inbox/report.pdf");
        let (bytes, name) = load_outgoing(&root, &rel).unwrap();
        assert_eq!(bytes, b"pdf-bytes");
        assert_eq!(name, "report.pdf");
    }

    #[test]
    fn incoming_filename_is_reduced_to_basename() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        // A path-y filename can't escape inbox/.
        let rel = save_incoming(&root, "../../etc/passwd", b"x").unwrap();
        assert_eq!(rel, "inbox/passwd");
        assert!(root.join("inbox/passwd").is_file());
    }

    #[test]
    fn resolve_within_rejects_escape() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        assert!(resolve_within(&root, "../secret").is_none());
        assert!(resolve_within(&root, "/etc/passwd").is_none());
        assert!(resolve_within(&root, "").is_none());
        assert!(resolve_within(&root, "ok/file.txt").is_some());
    }

    #[test]
    fn load_missing_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        assert!(load_outgoing(&root, "inbox/nope.txt").is_err());
    }
}
