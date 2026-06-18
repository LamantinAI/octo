//! Runtime configuration loading — the `octo.toml` manifest, the
//! [`ConnectorFactory`] registry, and the startup loader that turns
//! `type = "..."` TOML entries into live connector instances.
//!
//! This is the **manual / startup-load** tier of the `runtime_config.md` vault
//! draft: the runtime reads `octo.toml` once at build time, scans the
//! configured connector directory (and any explicit files), and instantiates
//! each via its registered factory. Hot reload (a file watcher emitting
//! `octo.config.*` envelopes) is a separate, later step — not here.
//!
//! ## Two on-disk layouts for a dyn connector
//!
//! - **Folder** (`connectors/<id>/<id>.toml` + `models/`) — a whole multi-route
//!   API with its JSON-schema models. This is the primary layout
//!   (see `petstore_case.md`).
//! - **Flat file** (`connectors/<id>.toml`) — a simple single-purpose connector.
//!
//! Both are discovered when scanning `[connectors] dir`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;

use crate::router::{Route, Router, RuleBasedRouter};
use crate::{Connector, ConnectorId};

/// Builds a connector instance from a TOML `[connector]` section.
///
/// One factory per connector `type` (`http`, `scheduler`, ...). The
/// application registers factories in code via
/// [`OctoBuilder::register_connector_type`](crate::OctoBuilder::register_connector_type);
/// the runtime then resolves each TOML `type = "..."` to its factory.
pub trait ConnectorFactory: Send + Sync + 'static {
    /// The `type` string this factory handles in TOML.
    fn type_name(&self) -> &str;

    /// Construct a connector instance from its parsed manifest. `config` is the
    /// whole connector file (a `[connector]` table at its root); `ctx.base_dir`
    /// is the file's directory, for resolving relative paths (e.g. `models_dir`).
    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Context handed to a [`ConnectorFactory::create`] call.
pub struct FactoryContext<'a> {
    /// Directory of the connector's manifest file — anchor for relative paths.
    pub base_dir: &'a Path,
}

/// Errors raised while loading runtime configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading config {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parsing config {path}: {source}")]
    Toml {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("connector file {path} has no [connector] section with id and type")]
    MissingConnectorHeader { path: PathBuf },

    #[error("connector file {path}: unknown connector type '{type_name}'; registered types: {known}")]
    UnknownConnectorType {
        path: PathBuf,
        type_name: String,
        known: String,
    },

    #[error("duplicate connector id '{0}'")]
    DuplicateConnectorId(ConnectorId),

    #[error("factory for connector '{id}' ({path}) failed: {source}")]
    Factory {
        id: ConnectorId,
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// The `octo.toml` manifest.
#[derive(Debug, Default, Deserialize)]
pub struct RuntimeManifest {
    #[serde(default)]
    pub runtime: RuntimeSection,
    #[serde(default)]
    pub connectors: ConnectorsSection,
    /// Optional declarative router table. Absent or routeless → no router.
    pub router: Option<RouterSection>,
}

/// The `[router]` section: the hard, data-driven routing table.
///
/// This is the **static** layer of routing — `predicate → action` as data.
/// Dynamic, content-aware routing remains the cogitator's job (a deterministic
/// algorithm or an LLM emitting targets directly); see `router.md`.
#[derive(Debug, Default, Deserialize)]
pub struct RouterSection {
    /// Router id (used as the emission source `router/<id>`). Default `config`.
    pub id: Option<String>,
    /// Reserved for a future strict mode; currently unused.
    #[serde(default)]
    pub strict: bool,
    /// The route table (`[[router.routes]]`).
    #[serde(default)]
    pub routes: Vec<Route>,
}

#[derive(Debug, Default, Deserialize)]
pub struct RuntimeSection {
    pub bus_capacity: Option<usize>,
    /// `"file_watch"` | `"on_signal"` | `"manual"`. Only `"manual"` (load once
    /// at startup) is honoured today; the others are reserved.
    pub config_reload: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ConnectorsSection {
    /// Directory scanned for connector manifests (relative to `octo.toml`).
    pub dir: Option<String>,
    /// Explicit connector files outside `dir`.
    #[serde(default)]
    pub explicit: Vec<ExplicitConnector>,
}

#[derive(Debug, Deserialize)]
pub struct ExplicitConnector {
    pub path: String,
}

/// Outcome of loading `octo.toml`.
pub(crate) struct LoadedConfig {
    pub bus_capacity: Option<usize>,
    pub connectors: Vec<Arc<dyn Connector>>,
    pub router: Option<Arc<dyn Router>>,
}

/// Build a [`RuleBasedRouter`] from a `[router]` section, if it declares any
/// routes. Returns `None` for an absent or empty table.
fn build_router(section: Option<RouterSection>) -> Option<Arc<dyn Router>> {
    let section = section?;
    if section.routes.is_empty() {
        return None;
    }
    let mut builder = RuleBasedRouter::builder(section.id.unwrap_or_else(|| "config".to_string()));
    for route in section.routes {
        builder = builder.add_route(route);
    }
    Some(builder.build())
}

/// Read and parse the manifest, scan for connector files, and instantiate each
/// via its factory. `existing_ids` are connector ids already registered in the
/// builder (so duplicates across code + config are caught).
pub(crate) fn load_config(
    manifest_path: &Path,
    factories: &HashMap<String, Arc<dyn ConnectorFactory>>,
    existing_ids: &HashSet<String>,
) -> Result<LoadedConfig, ConfigError> {
    let text = std::fs::read_to_string(manifest_path).map_err(|source| ConfigError::Io {
        path: manifest_path.to_path_buf(),
        source,
    })?;
    let manifest: RuntimeManifest =
        toml::from_str(&text).map_err(|source| ConfigError::Toml {
            path: manifest_path.to_path_buf(),
            source,
        })?;

    let base_dir = manifest_path.parent().unwrap_or_else(|| Path::new("."));

    let files = collect_connector_files(base_dir, &manifest.connectors)?;

    let mut seen: HashSet<String> = existing_ids.clone();
    let mut connectors = Vec::new();

    for file in files {
        let connector = instantiate(&file, factories, &mut seen)?;
        connectors.push(connector);
    }

    Ok(LoadedConfig {
        bus_capacity: manifest.runtime.bus_capacity,
        connectors,
        router: build_router(manifest.router),
    })
}

/// Gather connector manifest paths from `dir` (scanned, non-recursive) and the
/// explicit list. A missing/absent `dir` is not an error — it just yields no
/// connectors.
fn collect_connector_files(
    base_dir: &Path,
    section: &ConnectorsSection,
) -> Result<Vec<PathBuf>, ConfigError> {
    let mut files = Vec::new();

    if let Some(dir) = &section.dir {
        let dir_path = base_dir.join(dir);
        if dir_path.is_dir() {
            let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir_path)
                .map_err(|source| ConfigError::Io {
                    path: dir_path.clone(),
                    source,
                })?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .collect();
            entries.sort();

            for entry in entries {
                if entry.is_dir() {
                    // Folder-style: connectors/<id>/<id>.toml.
                    if let Some(name) = entry.file_name().and_then(|n| n.to_str()) {
                        let manifest = entry.join(format!("{name}.toml"));
                        if manifest.is_file() {
                            files.push(manifest);
                        } else {
                            tracing::warn!(
                                dir = %entry.display(),
                                "connector folder has no <name>.toml manifest; skipping"
                            );
                        }
                    }
                } else if entry.extension().and_then(|e| e.to_str()) == Some("toml") {
                    // Flat-style: connectors/<id>.toml.
                    files.push(entry);
                }
            }
        } else {
            tracing::debug!(dir = %dir_path.display(), "connectors dir absent; no dyn connectors");
        }
    }

    for explicit in &section.explicit {
        files.push(base_dir.join(&explicit.path));
    }

    Ok(files)
}

/// Read one connector file, resolve its factory by `type`, and build it.
fn instantiate(
    path: &Path,
    factories: &HashMap<String, Arc<dyn ConnectorFactory>>,
    seen: &mut HashSet<String>,
) -> Result<Arc<dyn Connector>, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let value: toml::Value = toml::from_str(&text).map_err(|source| ConfigError::Toml {
        path: path.to_path_buf(),
        source,
    })?;

    let connector_tbl = value.get("connector");
    let id = connector_tbl
        .and_then(|c| c.get("id"))
        .and_then(|v| v.as_str());
    let type_name = connector_tbl
        .and_then(|c| c.get("type"))
        .and_then(|v| v.as_str());
    let (id, type_name) = match (id, type_name) {
        (Some(id), Some(t)) => (id.to_string(), t.to_string()),
        _ => {
            return Err(ConfigError::MissingConnectorHeader {
                path: path.to_path_buf(),
            })
        }
    };

    if !seen.insert(id.clone()) {
        return Err(ConfigError::DuplicateConnectorId(ConnectorId::new(id)));
    }

    let factory = factories.get(&type_name).ok_or_else(|| {
        let mut known: Vec<&str> = factories.keys().map(String::as_str).collect();
        known.sort_unstable();
        ConfigError::UnknownConnectorType {
            path: path.to_path_buf(),
            type_name: type_name.clone(),
            known: if known.is_empty() {
                "(none)".to_string()
            } else {
                known.join(", ")
            },
        }
    })?;

    let connector_id = ConnectorId::new(id);
    let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
    factory
        .create(
            connector_id.clone(),
            &value,
            FactoryContext { base_dir },
        )
        .map_err(|source| ConfigError::Factory {
            id: connector_id,
            path: path.to_path_buf(),
            source,
        })
}
