//! `octo-connector-forkd` — a sandboxed script-execution organ (forkd v0).
//!
//! The exec substrate for **executable skills**: a skill's scripts run *here*, in
//! forkd's own supervised task, not in the cogitator's process/tool-loop. The model
//! reaches it through the usual `dispatch_to_connector` bridge — `forkd.run`
//! publishes a command, forkd runs the script and replies with a correlated
//! `forkd.run.result` (`{ exit_code, stdout, stderr, timed_out }`).
//!
//! **v0 isolation** (subprocess; no mount namespace yet):
//! - `cwd = $OCTO_CODE_WORKSPACE` — the same directory the octo-code file tools use;
//! - **clean env** — the child never sees the agent's secrets (tokens/keys); only
//!   `PATH` / `HOME` / `LANG` / `TMPDIR` are set, so host binaries (`python3`,
//!   `curl`, `wget`, `bash`) still resolve;
//! - **dropped privileges** — when forkd runs as root and `run_as` names an
//!   unprivileged user, the child is spawned setuid/setgid to it;
//! - a **wall-clock timeout** kills the whole process group;
//! - **rlimits** cap CPU time and file size (memory is opt-in — an `RLIMIT_AS` set
//!   too low breaks interpreters).
//!
//! v0 keeps the host filesystem visible and the network inherited, so `curl`/`wget`/
//! `pip` work. The next step is full `bwrap` (unshare + rw-bind the workspace +
//! ro-bind the interpreter and skills) with per-skill capabilities gating it.

use std::{
    path::{Component, Path, PathBuf},
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventKind, FactoryContext, Filter, OctoResult, SubscribeOptions,
    control::{CANCEL, CANCEL_SCOPE_TAG, ScopedTasks},
};
use serde_json::{Value, json};
use tracing::{info, warn};

mod identity;
mod process;
use self::identity::{can_drop_uid, resolve_group, resolve_user};

const RUN: &str = "forkd.run";

/// Env var naming the skills root — set by the host app so a skill's bundled
/// scripts run **in place** (never copied through the model). The bwrap stage
/// later ro-binds this same root into the sandbox.
pub const SKILLS_ENV: &str = "OCTO_SKILLS_DIR";

const CATALOG: &str = "A sandboxed runner for scripts. Dispatch to this connector's id:
- forkd.run { skill_path? | path? | script?, interpreter?, args?, stdin?, timeout_secs? } -> { exit_code, stdout, stderr, timed_out }
  `skill_path` runs a skill's bundled script IN PLACE (relative to the skills root, e.g.
  \"fetch-url/scripts/fetch.sh\") — never copy a skill's script into the workspace or the chat.
  `path` runs a script from your workspace; `script` is an inline body. `interpreter`
  (python3 / bash / sh; default bash for inline) runs it; `args` follow the script. cwd is
  your workspace (outputs land there), clean environment, hard timeout. Network works
  (curl / wget / pip).";

/// rlimits applied to the child before exec (0 = leave unlimited).
#[derive(Clone, Copy)]
struct Limits {
    cpu_secs: u64,
    fsize_bytes: u64,
    mem_bytes: u64,
}

pub struct ForkdConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    /// Explicit workspace root; `None` -> `$OCTO_CODE_WORKSPACE`.
    workspace: Option<PathBuf>,
    /// Explicit skills root (bundled scripts run in place); `None` -> `$OCTO_SKILLS_DIR`.
    skills: Option<PathBuf>,
    /// `(uid, gid)` to drop to; `None` -> run as the current user.
    drop_to: Option<(u32, u32)>,
    default_timeout: Duration,
    max_timeout: Duration,
    max_output: usize,
    limits: Limits,
    /// Names of environment variables to forward from forkd's own process into the
    /// otherwise-cleared script env (manifest `env_passthrough`). A deploy uses it to
    /// hand scripts a path they need — e.g. `CODEX_HOME` so a skill can drive the Codex
    /// CLI on the host's login. Only these names pass; everything else stays cleared.
    env_passthrough: Vec<String>,
    seq: AtomicU64,
}

impl ForkdConnector {
    fn build(
        id: impl Into<String>,
        workspace: Option<PathBuf>,
        skills: Option<PathBuf>,
        drop_to: Option<(u32, u32)>,
        default_timeout: Duration,
        max_timeout: Duration,
        max_output: usize,
        limits: Limits,
        env_passthrough: Vec<String>,
    ) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([EventKind::from_static(RUN)])
            .with_description(CATALOG);
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            workspace,
            skills,
            drop_to,
            default_timeout,
            max_timeout,
            max_output,
            limits,
            env_passthrough,
            seq: AtomicU64::new(0),
        })
    }

    /// The skills root a `skill_path` resolves against: the manifest pin, else
    /// `$OCTO_SKILLS_DIR` (exported by the host app — one source of truth by name).
    fn skills_root(&self) -> Result<PathBuf, String> {
        if let Some(p) = &self.skills {
            return Ok(p.clone());
        }
        std::env::var_os(SKILLS_ENV).map(PathBuf::from).ok_or_else(|| {
            format!("skill_path given but no skills root configured (set {SKILLS_ENV} or the manifest `skills`)")
        })
    }
}

#[async_trait]
impl Connector for ForkdConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut events = ctx
            .subscribe(
                Filter::by_kind(RUN).with_kind(CANCEL),
                SubscribeOptions::default(),
            )
            .await?;
        let mut jobs = ScopedTasks::default();
        info!(connector = %self.id, "forkd ready");
        loop {
            tokio::select! {
                next = events.next() => match next {
                    Some(env) if env.kind.as_str() == CANCEL => {
                        if let Some(scope) = env.payload_as::<String>() { jobs.cancel(scope); }
                    }
                    Some(env) if env.target.as_ref() == Some(&self.id) => {
                        let me = self.clone(); let ctx = ctx.clone();
                        jobs.spawn(env.tags.get(CANCEL_SCOPE_TAG).cloned(), move |cancel| async move {
                            let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
                            let payload = me.run_script(&params, &cancel).await.unwrap_or_else(|error| json!({"error":error}));
                            let response = Envelope::new(me.id.clone(), EventKind::new(format!("{RUN}.result")), payload).with_correlation(env.id);
                            if let Err(error) = ctx.publish(response).await { warn!(%error, "forkd result publish failed"); }
                        });
                    }
                    Some(_) => {}, None => break,
                },
                _ = jobs.join_next(), if !jobs.is_empty() => {},
                _ = ctx.shutdown.cancelled() => break,
            }
        }
        jobs.cancel_all();
        while jobs.join_next().await.is_some() {}
        Ok(())
    }
}

fn prepend(target: &str, args: Vec<String>) -> Vec<String> {
    std::iter::once(target.to_string()).chain(args).collect()
}

fn ext_of(interpreter: Option<&str>) -> &'static str {
    match interpreter {
        Some(i) if i.contains("python") => "py",
        _ => "sh",
    }
}

/// Resolve `rel` under `root`, rejecting absolute paths and `..` escapes.
fn jailed(root: &Path, rel: &str) -> Result<PathBuf, String> {
    let p = Path::new(rel);
    if rel.is_empty()
        || p.is_absolute()
        || p.components().any(|c| matches!(c, Component::ParentDir))
    {
        return Err("path must be relative and stay within the workspace".into());
    }
    Ok(root.join(p))
}

/// UTF-8-lossy, truncated to `max` bytes with a marker when clipped.
fn cap(bytes: &[u8], max: usize) -> String {
    if bytes.len() <= max {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        format!(
            "{}\n…(truncated, {} bytes total)",
            String::from_utf8_lossy(&bytes[..max]),
            bytes.len()
        )
    }
}

fn kill_group(pid: u32) {
    // Kill the child's whole process group (it is the group leader via process_group(0)).
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
}

fn apply_limits(l: Limits) {
    let set = |res: libc::__rlimit_resource_t, val: u64| {
        if val == 0 {
            return;
        }
        let lim = libc::rlimit {
            rlim_cur: val as libc::rlim_t,
            rlim_max: val as libc::rlim_t,
        };
        unsafe {
            libc::setrlimit(res, &lim);
        }
    };
    set(libc::RLIMIT_CPU, l.cpu_secs);
    set(libc::RLIMIT_FSIZE, l.fsize_bytes);
    set(libc::RLIMIT_AS, l.mem_bytes);
}

// ── config-driven construction (`type = "forkd"`) ────────────────────────────

/// [`ConnectorFactory`] for `type = "forkd"`.
pub struct ForkdConnectorFactory;

impl ForkdConnectorFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ForkdConnectorFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorFactory for ForkdConnectorFactory {
    fn type_name(&self) -> &str {
        "forkd"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config
            .get("connector")
            .ok_or("forkd: manifest has no [connector] table")?;
        let u64_or = |k: &str, d: u64| {
            table
                .get(k)
                .and_then(toml::Value::as_integer)
                .map(|v| v as u64)
                .unwrap_or(d)
        };

        let workspace = table
            .get("workspace")
            .and_then(|v| v.as_str())
            .map(|ws| ctx.base_dir.join(ws));
        let default_timeout = Duration::from_secs(u64_or("timeout_secs", 30));
        let max_timeout = Duration::from_secs(u64_or("max_timeout_secs", 300));
        let max_output = u64_or("max_output_bytes", 65_536) as usize;
        let limits = Limits {
            cpu_secs: u64_or("cpu_secs", max_timeout.as_secs() + 5),
            fsize_bytes: u64_or("fsize_mb", 64) * 1024 * 1024,
            mem_bytes: u64_or("mem_mb", 0) * 1024 * 1024,
        };

        // Privilege drop: resolve `run_as` to (uid, gid); apply it when forkd can
        // actually setuid — as root, or as a non-root service granted CAP_SETUID/
        // CAP_SETGID (e.g. systemd AmbientCapabilities under a hardened unit).
        let can_drop = can_drop_uid();
        let run_group = table.get("run_group").and_then(|v| v.as_str());
        let drop_to = match table.get("run_as").and_then(|v| v.as_str()) {
            Some(user) => match resolve_user(user) {
                Some((uid, pw_gid)) if can_drop => {
                    // The child's PRIMARY gid must be the workspace-sharing group:
                    // setuid drops supplementary groups, so mere membership will not
                    // open a setgid 2770 workspace — chdir would fail EACCES.
                    let gid = match run_group {
                        Some(g) => resolve_group(g).unwrap_or_else(|| {
                            warn!(
                                group = g,
                                "forkd: run_group not found; using the run_as user's own group"
                            );
                            pw_gid
                        }),
                        None => pw_gid,
                    };
                    info!(user, uid, gid, "forkd: scripts run with dropped privileges");
                    Some((uid, gid))
                }
                Some(_) => {
                    warn!(
                        user,
                        "forkd: no setuid capability, cannot drop to run_as; scripts run as the current user"
                    );
                    None
                }
                None => {
                    warn!(
                        user,
                        "forkd: run_as user not found; scripts run as the current user"
                    );
                    None
                }
            },
            None => {
                if unsafe { libc::geteuid() } == 0 {
                    warn!(
                        "forkd: no run_as configured and running as ROOT — scripts run AS ROOT; set run_as to a dedicated unprivileged user"
                    );
                }
                None
            }
        };

        // Optional skills root pin; when absent, $OCTO_SKILLS_DIR is read at runtime.
        let skills = table
            .get("skills")
            .and_then(|v| v.as_str())
            .map(|s| ctx.base_dir.join(s));

        // Env vars to forward from our process into the cleared script env, by name.
        let env_passthrough: Vec<String> = table
            .get("env_passthrough")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(ForkdConnector::build(
            id.as_str(),
            workspace,
            skills,
            drop_to,
            default_timeout,
            max_timeout,
            max_output,
            limits,
            env_passthrough,
        ))
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(ForkdConnectorFactory::new())
}
