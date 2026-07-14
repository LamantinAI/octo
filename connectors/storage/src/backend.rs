//! Storage backends behind one trait, so the connector is agnostic to where
//! durable artifacts live. [`LocalStorage`] (a directory) ships now; an
//! S3-compatible backend slots in later without touching the connector or its
//! command surface.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use octo_workspace::{write_atomic, WorkspaceError, TMP_PREFIX};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid key `{0}`: must be a non-empty relative path without `..`")]
    BadKey(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl From<WorkspaceError> for StorageError {
    fn from(e: WorkspaceError) -> Self {
        match e {
            WorkspaceError::Absolute(p) | WorkspaceError::Escape(p) => StorageError::BadKey(p),
            WorkspaceError::Empty => StorageError::BadKey(String::new()),
            WorkspaceError::NotFound(p) => StorageError::NotFound(p),
            WorkspaceError::Io(io) => StorageError::Io(io),
        }
    }
}

/// A durable object store addressed by string keys (`reports/2026/summary.md`).
/// Object semantics — keys are opaque paths; there are no directories to create.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Store `bytes` at `key`, creating or replacing it.
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError>;
    /// Fetch the bytes at `key`.
    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError>;
    /// List keys beginning with `prefix` (empty lists everything), sorted.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
    /// Delete `key`; returns whether it existed (idempotent).
    async fn delete(&self, key: &str) -> Result<bool, StorageError>;
}

/// A local-directory backend. Keys map to files under a root; writes are atomic
/// (temp + rename) and confined to the root (no absolute paths, no `..` escape).
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    /// Open (creating if needed) a local store rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root: root.canonicalize()? })
    }

    /// The store's root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve `key` to a path within the root, rejecting escapes. Purely lexical.
    fn resolve(&self, key: &str) -> Result<PathBuf, StorageError> {
        Ok(octo_workspace::resolve_file_in_root(&self.root, key)?)
    }

    /// Recursively collect keys (root-relative paths) under `dir`.
    fn collect(&self, dir: &Path, out: &mut Vec<String>) {
        let read = match std::fs::read_dir(dir) {
            Ok(r) => r,
            Err(_) => return,
        };
        for entry in read.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(ft) if ft.is_dir() => self.collect(&path, out),
                Ok(ft) if ft.is_file() => {
                    let name = entry.file_name();
                    if name.to_string_lossy().starts_with(TMP_PREFIX) {
                        continue;
                    }
                    if let Ok(rel) = path.strip_prefix(&self.root) {
                        out.push(rel.to_string_lossy().into_owned());
                    }
                }
                _ => {}
            }
        }
    }
}

#[async_trait]
impl StorageBackend for LocalStorage {
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        let dest = self.resolve(key)?;
        Ok(write_atomic(&dest, bytes)?)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let path = self.resolve(key)?;
        match std::fs::read(&path) {
            Ok(b) => Ok(b),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(key.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let mut keys = Vec::new();
        self.collect(&self.root, &mut keys);
        keys.retain(|k| k.starts_with(prefix));
        keys.sort();
        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        let path = self.resolve(key)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_list_delete_cycle() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalStorage::new(tmp.path()).unwrap();

        store.put("reports/a.md", b"alpha").await.unwrap();
        store.put("reports/sub/b.md", b"beta").await.unwrap();
        store.put("notes.txt", b"gamma").await.unwrap();

        assert_eq!(store.get("reports/a.md").await.unwrap(), b"alpha");

        let all = store.list("").await.unwrap();
        assert_eq!(all, vec!["notes.txt", "reports/a.md", "reports/sub/b.md"]);

        let scoped = store.list("reports/").await.unwrap();
        assert_eq!(scoped, vec!["reports/a.md", "reports/sub/b.md"]);

        assert!(store.delete("notes.txt").await.unwrap());
        assert!(!store.delete("notes.txt").await.unwrap()); // idempotent
        assert!(matches!(store.get("notes.txt").await, Err(StorageError::NotFound(_))));
    }

    #[tokio::test]
    async fn rejects_bad_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalStorage::new(tmp.path()).unwrap();
        assert!(matches!(store.put("../evil", b"x").await, Err(StorageError::BadKey(_))));
        assert!(matches!(store.put("/etc/passwd", b"x").await, Err(StorageError::BadKey(_))));
        assert!(matches!(store.put("", b"x").await, Err(StorageError::BadKey(_))));
    }
}
