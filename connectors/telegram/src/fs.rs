//! Shared-workspace access for file transfer, over the shared [`octo_workspace`]
//! jail.
//!
//! Files move **by reference, not through the model**: an incoming document is
//! saved into the workspace and the cogitator is handed its path; an outbound
//! `chat.send_file` names a workspace path the connector loads and sends. The
//! root is the SAME one octo-code / storage use (pinned via the manifest
//! `workspace`, else `$OCTO_CODE_WORKSPACE`, else `<tmp>/octo-code`).

use std::path::{Path, PathBuf};

use octo_workspace::WorkspaceError;

/// Subdirectory under the workspace where incoming files land.
pub(crate) const INBOX: &str = "inbox";

/// The workspace root: the pinned path, else the environment default.
pub(crate) fn workspace_root(pinned: &Option<PathBuf>) -> Result<PathBuf, WorkspaceError> {
    octo_workspace::workspace_root(pinned.as_deref())
}

/// Save an incoming file under `inbox/<basename>`, returning its workspace-
/// relative path. The filename is reduced to its basename so a crafted name
/// can't place the file outside `inbox/`.
pub(crate) fn save_incoming(
    root: &Path,
    filename: &str,
    bytes: &[u8],
) -> Result<String, WorkspaceError> {
    let base = Path::new(filename).file_name().and_then(|s| s.to_str()).unwrap_or("file");
    let rel = format!("{INBOX}/{base}");
    octo_workspace::write_in_root(root, &rel, bytes)?;
    Ok(rel)
}

/// Load a workspace file by relative path, returning its bytes and basename.
pub(crate) fn load_outgoing(root: &Path, rel: &str) -> Result<(Vec<u8>, String), WorkspaceError> {
    let bytes = octo_workspace::read_in_root(root, rel)?;
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
    fn load_missing_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().canonicalize().unwrap();
        assert!(load_outgoing(&root, "inbox/nope.txt").is_err());
    }
}
