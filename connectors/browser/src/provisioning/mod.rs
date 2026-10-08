//! Managed Chrome builds: stable resolution, process pinning and bounded disk use.
use std::{
    fs::{
        File, OpenOptions, create_dir_all, read, read_dir, remove_dir_all, remove_file, rename,
        write,
    },
    io::Error as IoError,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::OnceCell;
use tracing::warn;
use zendriver::{Fetcher, VersionSpec};

const CHANNELS: &str = "https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json";

#[derive(Clone, Deserialize)]
#[serde(default)]
pub(super) struct ChromeSettings {
    pub chrome_version: Option<String>,
    pub chrome_keep_builds: usize,
    pub chrome_update_interval_secs: u64,
    pub chrome_download_timeout_secs: u64,
}
impl Default for ChromeSettings {
    fn default() -> Self {
        Self {
            chrome_version: None,
            chrome_keep_builds: 2,
            chrome_update_interval_secs: 86400,
            chrome_download_timeout_secs: 300,
        }
    }
}
impl ChromeSettings {
    pub fn validate(&self) -> Result<(), String> {
        if self.chrome_keep_builds == 0
            || self.chrome_download_timeout_secs == 0
            || self
                .chrome_version
                .as_ref()
                .is_some_and(|s| version(s).is_none())
        {
            return Err("browser: require a four-part chrome_version and positive chrome_keep_builds/download timeout".into());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Selected {
    version: String,
    checked_at: u64,
}
struct Lease {
    _file: File,
    executable: PathBuf,
    marker: Option<Selected>,
    previous: Option<PathBuf>,
    rolled_back: AtomicBool,
}
pub(super) struct ChromeCache {
    root: PathBuf,
    settings: ChromeSettings,
    selected: OnceCell<Lease>,
}
impl ChromeCache {
    pub fn new(root: PathBuf, settings: ChromeSettings) -> Self {
        Self {
            root,
            settings,
            selected: OnceCell::new(),
        }
    }
    pub async fn executable(&self) -> Result<PathBuf, String> {
        let lease = self
            .selected
            .get_or_try_init(|| self.resolve(CHANNELS))
            .await?;
        Ok(if lease.rolled_back.load(Ordering::Relaxed) {
            lease.previous.as_ref().unwrap().clone()
        } else {
            lease.executable.clone()
        })
    }
    async fn resolve(&self, manifest_url: &str) -> Result<Lease, String> {
        create_dir_all(&self.root).map_err(|e| e.to_string())?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join(".lock"))
            .map_err(|e| e.to_string())?;
        lock.try_lock_exclusive().map_err(|_| {
            "browser: Chrome cache is already owned by another connector/process".to_owned()
        })?;
        clear_staging(&self.root).map_err(|e| e.to_string())?;
        let previous = read(self.root.join("selected.json"))
            .ok()
            .and_then(|s| serde_json::from_slice::<Selected>(&s).ok());
        let cached = previous
            .as_ref()
            .filter(|s| version(&s.version).is_some())
            .and_then(|s| find_binary(&self.root.join(&s.version)));
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if let (Some(s), Some(exe)) = (&previous, &cached) {
            let reuse = match &self.settings.chrome_version {
                Some(v) => *v == s.version,
                None => {
                    now.saturating_sub(s.checked_at) < self.settings.chrome_update_interval_secs
                }
            };
            if reuse {
                return Ok(Lease {
                    _file: lock,
                    executable: exe.clone(),
                    marker: None,
                    previous: None,
                    rolled_back: AtomicBool::new(false),
                });
            }
        }
        // Protect an explicit cached pin as well as the previous working build.
        let pinned = self
            .settings
            .chrome_version
            .as_ref()
            .and_then(|v| find_binary(&self.root.join(v)));
        prune(
            &self.root,
            pinned.as_deref().or(cached.as_deref()),
            self.settings
                .chrome_keep_builds
                .max(usize::from(pinned.is_some()) + usize::from(cached.is_some())),
            cached.as_deref(),
        )
        .map_err(|e| e.to_string())?;
        let result = tokio::time::timeout(
            Duration::from_secs(self.settings.chrome_download_timeout_secs),
            async {
                let chosen = match &self.settings.chrome_version {
                    Some(v) => v.clone(),
                    None => {
                        let value: Value = Client::builder()
                            .timeout(Duration::from_secs(30))
                            .build()
                            .map_err(|e| e.to_string())?
                            .get(manifest_url)
                            .send()
                            .await
                            .map_err(|e| e.to_string())?
                            .error_for_status()
                            .map_err(|e| e.to_string())?
                            .json()
                            .await
                            .map_err(|e| e.to_string())?;
                        stable_version(&value)?
                    }
                };
                let exe = match find_binary(&self.root.join(&chosen)) {
                    Some(exe) => exe,
                    None => Fetcher::new()
                        .cache_dir(&self.root)
                        .version(VersionSpec::Explicit(chosen.clone()))
                        .ensure_chrome()
                        .await
                        .map_err(|e| e.to_string())?,
                };
                let marker = Selected {
                    version: chosen,
                    checked_at: now,
                };
                Ok::<_, String>((exe, marker))
            },
        )
        .await
        .map_err(|_| "Chrome provisioning timed out".to_owned())
        .and_then(|r| r);
        if let Err(error) = clear_staging(&self.root) {
            warn!(%error, "could not clean Chrome staging files");
        }
        match result {
            Ok((executable, marker)) => Ok(Lease {
                _file: lock,
                executable,
                marker: Some(marker),
                previous: cached.clone(),
                rolled_back: AtomicBool::new(false),
            }),
            Err(error) if cached.is_some() && self.settings.chrome_version.is_none() => {
                warn!(%error, "Chrome update unavailable; using previously selected build");
                Ok(Lease {
                    _file: lock,
                    executable: cached.unwrap(),
                    marker: None,
                    previous: None,
                    rolled_back: AtomicBool::new(false),
                })
            }
            Err(error) => Err(error),
        }
    }
    pub fn fallback(&self) -> Option<PathBuf> {
        if self.settings.chrome_version.is_some() {
            return None;
        }
        let lease = self.selected.get()?;
        let previous = lease
            .previous
            .as_ref()
            .filter(|p| **p != lease.executable)?;
        if lease.rolled_back.swap(true, Ordering::Relaxed) {
            return None;
        }
        Some(previous.clone())
    }
    pub async fn prune(&self) {
        if let Some(lease) = self.selected.get() {
            if lease.rolled_back.load(Ordering::Relaxed) {
                return;
            }
            if let Some(marker) = &lease.marker {
                let saved = serde_json::to_vec(marker)
                    .map_err(IoError::other)
                    .and_then(|bytes| {
                        write(self.root.join("selected.json.tmp"), bytes)?;
                        rename(
                            self.root.join("selected.json.tmp"),
                            self.root.join("selected.json"),
                        )
                    });
                if let Err(error) = saved {
                    warn!(%error, "could not persist selected Chrome version");
                }
            }
            if let Err(error) = prune(
                &self.root,
                Some(&lease.executable),
                self.settings.chrome_keep_builds,
                lease.previous.as_deref(),
            ) {
                warn!(%error, "could not prune Chrome cache");
            }
        }
    }
}
fn clear_staging(root: &Path) -> Result<(), IoError> {
    for entry in read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name
            .strip_suffix(".tmp.zip")
            .or_else(|| name.strip_suffix(".tmp"))
            .is_some_and(|s| version(s).is_some())
        {
            let kind = entry.file_type()?;
            if kind.is_dir() {
                remove_dir_all(entry.path())?;
            } else if kind.is_file() {
                remove_file(entry.path())?;
            }
        }
    }
    Ok(())
}
fn stable_version(value: &Value) -> Result<String, String> {
    value
        .pointer("/channels/Stable/version")
        .and_then(Value::as_str)
        .filter(|s| version(s).is_some())
        .map(str::to_owned)
        .ok_or_else(|| "Chrome manifest has no valid Stable version".into())
}
fn version(s: &str) -> Option<Vec<u64>> {
    let parts: Vec<_> = s
        .split('.')
        .map(str::parse::<u64>)
        .collect::<Result<_, _>>()
        .ok()?;
    (parts.len() == 4).then_some(parts)
}
fn find_binary(dir: &Path) -> Option<PathBuf> {
    if dir.is_symlink() {
        return None;
    }
    [
        "chrome-linux64/chrome",
        "chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
        "chrome-mac-x64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
        "chrome-win64/chrome.exe",
        "chrome-win32/chrome.exe",
    ]
    .into_iter()
    .map(|p| dir.join(p))
    .find(|p| p.is_file())
}
fn prune(
    root: &Path,
    active: Option<&Path>,
    keep: usize,
    previous: Option<&Path>,
) -> Result<(), IoError> {
    let mut builds = Vec::new();
    for entry in read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(v) = version(&entry.file_name().to_string_lossy()) {
            builds.push((v, entry.path()));
        }
    }
    builds.sort_by(|a, b| {
        previous
            .is_some_and(|p| p.starts_with(&b.1))
            .cmp(&previous.is_some_and(|p| p.starts_with(&a.1)))
            .then(b.0.cmp(&a.0))
    });
    let mut retained = usize::from(active.is_some());
    for (_, path) in builds {
        if active.is_some_and(|a| a.starts_with(&path)) {
            continue;
        }
        if retained < keep {
            retained += 1;
        } else {
            remove_dir_all(path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn stable_comes_from_channel_not_newest_known_good() {
        assert_eq!(stable_version(&json!({"channels":{"Stable":{"version":"150.0.1.2"},"Canary":{"version":"157.0.9.0"}}})).unwrap(), "150.0.1.2");
        assert!(stable_version(&json!({"versions":[{"version":"157.0.9.0"}]})).is_err());
        assert!(stable_version(&json!({"channels":{"Stable":{"version":"../escape"}}})).is_err());
    }
    #[test]
    fn pruning_preserves_selected_build_and_unmanaged_paths() {
        let dir = tempdir().unwrap();
        for n in ["1.0.0.0", "2.0.0.0", "3.0.0.0", "profile"] {
            fs::create_dir(dir.path().join(n)).unwrap();
        }
        let active = dir.path().join("1.0.0.0/chrome-linux64/chrome");
        prune(dir.path(), Some(&active), 2, None).unwrap();
        assert!(dir.path().join("1.0.0.0").exists());
        assert!(dir.path().join("3.0.0.0").exists());
        assert!(!dir.path().join("2.0.0.0").exists());
        assert!(dir.path().join("profile").exists());
    }
    #[tokio::test]
    async fn cached_explicit_build_needs_no_network_and_cache_is_exclusive() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("1.0.0.0/chrome-linux64")).unwrap();
        fs::write(dir.path().join("1.0.0.0/chrome-linux64/chrome"), "fixture").unwrap();
        fs::create_dir_all(dir.path().join("9.0.0.0/chrome-linux64")).unwrap();
        fs::write(
            dir.path().join("9.0.0.0/chrome-linux64/chrome"),
            "newer fixture",
        )
        .unwrap();
        let settings = ChromeSettings {
            chrome_keep_builds: 1,
            chrome_version: Some("1.0.0.0".into()),
            ..Default::default()
        };
        let cache = ChromeCache::new(dir.path().into(), settings.clone());
        let exe = cache.executable().await.unwrap();
        assert_eq!(exe, cache.executable().await.unwrap());
        let competing = ChromeCache::new(dir.path().into(), settings);
        assert!(
            competing
                .executable()
                .await
                .unwrap_err()
                .contains("already owned")
        );
        assert!(!dir.path().join("selected.json").exists());
        cache.prune().await; // called only after a successful launch
        assert!(dir.path().join("selected.json").exists());
    }
    #[tokio::test]
    async fn update_resolves_stable_and_failed_launch_keeps_previous_selection() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            spawn,
        };
        let dir = tempdir().unwrap();
        for v in ["1.0.0.0", "2.0.0.0", "9.0.0.0"] {
            fs::create_dir_all(dir.path().join(v).join("chrome-linux64")).unwrap();
            fs::write(dir.path().join(v).join("chrome-linux64/chrome"), "fixture").unwrap();
        }
        fs::write(
            dir.path().join("selected.json"),
            br#"{"version":"1.0.0.0","checked_at":0}"#,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            let _ = socket.read(&mut request).await;
            let body =
                r#"{"channels":{"Stable":{"version":"2.0.0.0"},"Canary":{"version":"9.0.0.0"}}}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
        });
        let cache = ChromeCache::new(
            dir.path().into(),
            ChromeSettings {
                chrome_keep_builds: 3,
                ..Default::default()
            },
        );
        let lease = cache.resolve(&url).await.unwrap();
        assert!(lease.executable.starts_with(dir.path().join("2.0.0.0")));
        cache.selected.set(lease).ok().unwrap();
        assert!(
            cache
                .fallback()
                .unwrap()
                .starts_with(dir.path().join("1.0.0.0"))
        );
        cache.prune().await;
        assert!(
            fs::read_to_string(dir.path().join("selected.json"))
                .unwrap()
                .contains("1.0.0.0")
        );
        assert!(
            cache
                .executable()
                .await
                .unwrap()
                .starts_with(dir.path().join("1.0.0.0"))
        );
        assert!(cache.fallback().is_none());
        server.await.unwrap();
    }
}
