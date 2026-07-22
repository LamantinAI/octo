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
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventKind, FactoryContext, Filter, OctoResult, SubscribeOptions,
};
use octo_workspace::workspace_root;
use serde_json::{json, Value};
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};
use tracing::{info, warn};

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
        let mut cmds = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;
        info!(connector = %self.id, drop_uid = ?self.drop_to.map(|(u, _)| u), "forkd ready");
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

impl ForkdConnector {
    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        if env.kind.as_str() != RUN {
            return;
        }
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let payload = self.run_script(&params).await.unwrap_or_else(|e| json!({ "error": e }));
        let resp = Envelope::new(self.id.clone(), EventKind::new(format!("{RUN}.result")), payload)
            .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            warn!(error = %e, "forkd failed to publish result");
        }
    }

    async fn run_script(&self, params: &Value) -> Result<Value, String> {
        let workspace = workspace_root(self.workspace.as_deref()).map_err(|e| e.to_string())?;
        let interpreter = params.get("interpreter").and_then(Value::as_str);
        let inline = params.get("script").and_then(Value::as_str);
        let user_args: Vec<String> = params
            .get("args")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let stdin = params.get("stdin").and_then(Value::as_str).map(String::from);
        let dur = params
            .get("timeout_secs")
            .and_then(Value::as_u64)
            .map(Duration::from_secs)
            .unwrap_or(self.default_timeout)
            .min(self.max_timeout);

        // Resolve the file to run: a skill's bundled script (run IN PLACE from the
        // skills root — never copied), an inline script (written into the workspace),
        // or a workspace-relative path. All jailed against `..`/absolute escapes;
        // cwd stays the workspace either way, so outputs land there.
        let target = if let Some(rel) = params.get("skill_path").and_then(Value::as_str) {
            let root = self.skills_root()?;
            jailed(&root, rel)?
        } else if let Some(body) = inline {
            let n = self.seq.fetch_add(1, Ordering::Relaxed);
            let file = workspace.join(format!(".forkd/run-{n}.{}", ext_of(interpreter)));
            if let Some(parent) = file.parent() {
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            std::fs::write(&file, body).map_err(|e| e.to_string())?;
            file
        } else if let Some(rel) = params.get("path").and_then(Value::as_str) {
            jailed(&workspace, rel)?
        } else {
            return Err(
                "provide `skill_path` (a skill's bundled script), `path` (a workspace script) or `script` (inline body)"
                    .into(),
            );
        };
        let target = target.to_string_lossy().into_owned();

        // program + args: `interpreter target args…`, else run the target directly
        // (an inline script with no interpreter defaults to bash).
        let (program, full_args) = match (interpreter, inline.is_some()) {
            (Some(it), _) => (it.to_string(), prepend(&target, user_args)),
            (None, true) => ("bash".to_string(), prepend(&target, user_args)),
            (None, false) => (target.clone(), user_args),
        };

        let mut cmd = Command::new(&program);
        cmd.args(&full_args)
            .current_dir(&workspace)
            .env_clear()
            .env("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
            .env("HOME", &workspace)
            .env("TMPDIR", &workspace)
            .env("LANG", "C.UTF-8")
            .stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);
        if let Some((uid, gid)) = self.drop_to {
            cmd.uid(uid).gid(gid);
        }
        let limits = self.limits;
        // SAFETY: only async-signal-safe libc calls (setrlimit) run between fork and exec.
        unsafe {
            cmd.pre_exec(move || {
                apply_limits(limits);
                Ok(())
            });
        }

        let mut child = cmd.spawn().map_err(|e| format!("spawn {program}: {e}"))?;
        let pid = child.id();
        if let (Some(input), Some(mut sink)) = (stdin, child.stdin.take()) {
            let _ = sink.write_all(input.as_bytes()).await;
        }
        info!(program = %program, timeout_s = dur.as_secs(), "forkd: run");
        match timeout(dur, child.wait_with_output()).await {
            Ok(Ok(out)) => Ok(json!({
                "exit_code": out.status.code(),
                "stdout": cap(&out.stdout, self.max_output),
                "stderr": cap(&out.stderr, self.max_output),
                "timed_out": false,
            })),
            Ok(Err(e)) => Err(format!("run {program}: {e}")),
            Err(_) => {
                if let Some(p) = pid {
                    kill_group(p);
                }
                Ok(json!({
                    "exit_code": Value::Null,
                    "stdout": "",
                    "stderr": format!("(killed: exceeded {}s wall-clock)", dur.as_secs()),
                    "timed_out": true,
                }))
            }
        }
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
        format!("{}\n…(truncated, {} bytes total)", String::from_utf8_lossy(&bytes[..max]), bytes.len())
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
        let lim = libc::rlimit { rlim_cur: val as libc::rlim_t, rlim_max: val as libc::rlim_t };
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
        let table = config.get("connector").ok_or("forkd: manifest has no [connector] table")?;
        let u64_or = |k: &str, d: u64| table.get(k).and_then(toml::Value::as_integer).map(|v| v as u64).unwrap_or(d);

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
                            warn!(group = g, "forkd: run_group not found; using the run_as user's own group");
                            pw_gid
                        }),
                        None => pw_gid,
                    };
                    info!(user, uid, gid, "forkd: scripts run with dropped privileges");
                    Some((uid, gid))
                }
                Some(_) => {
                    warn!(user, "forkd: no setuid capability, cannot drop to run_as; scripts run as the current user");
                    None
                }
                None => {
                    warn!(user, "forkd: run_as user not found; scripts run as the current user");
                    None
                }
            },
            None => {
                if unsafe { libc::geteuid() } == 0 {
                    warn!("forkd: no run_as configured and running as ROOT — scripts run AS ROOT; set run_as to a dedicated unprivileged user");
                }
                None
            }
        };

        // Optional skills root pin; when absent, $OCTO_SKILLS_DIR is read at runtime.
        let skills = table
            .get("skills")
            .and_then(|v| v.as_str())
            .map(|s| ctx.base_dir.join(s));

        Ok(ForkdConnector::build(
            id.as_str(),
            workspace,
            skills,
            drop_to,
            default_timeout,
            max_timeout,
            max_output,
            limits,
        ))
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(ForkdConnectorFactory::new())
}

/// Whether this process can setuid/setgid children: root, or a non-root service
/// holding CAP_SETUID + CAP_SETGID in its effective set (systemd
/// `AmbientCapabilities=CAP_SETUID CAP_SETGID` under a hardened `User=` unit).
fn can_drop_uid() -> bool {
    if unsafe { libc::geteuid() } == 0 {
        return true;
    }
    // CapEff is a hex bitmask in /proc/self/status; CAP_SETGID = bit 6, CAP_SETUID = bit 7.
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return false;
    };
    const NEEDED: u64 = (1 << 6) | (1 << 7);
    status
        .lines()
        .find_map(|l| l.strip_prefix("CapEff:"))
        .and_then(|hex| u64::from_str_radix(hex.trim(), 16).ok())
        .is_some_and(|caps| caps & NEEDED == NEEDED)
}

/// Resolve a group name to its gid via `getgrnam` (called once, at config time).
fn resolve_group(name: &str) -> Option<u32> {
    let cname = std::ffi::CString::new(name).ok()?;
    // SAFETY: as with getpwnam — read the static buffer immediately, copy out.
    unsafe {
        let gr = libc::getgrnam(cname.as_ptr());
        if gr.is_null() {
            None
        } else {
            Some((*gr).gr_gid)
        }
    }
}

/// Resolve a username to `(uid, gid)` via `getpwnam` (called once, at config time).
fn resolve_user(name: &str) -> Option<(u32, u32)> {
    let cname = std::ffi::CString::new(name).ok()?;
    // SAFETY: getpwnam returns a pointer into a static buffer; we read it immediately
    // and copy the two fields out before any other libc call can clobber it.
    unsafe {
        let pw = libc::getpwnam(cname.as_ptr());
        if pw.is_null() {
            None
        } else {
            Some(((*pw).pw_uid, (*pw).pw_gid))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forkd(ws: &Path) -> Arc<ForkdConnector> {
        forkd_with_skills(ws, None)
    }

    fn forkd_with_skills(ws: &Path, skills: Option<&Path>) -> Arc<ForkdConnector> {
        ForkdConnector::build(
            "forkd",
            Some(ws.to_path_buf()),
            skills.map(Path::to_path_buf),
            None,
            Duration::from_secs(5),
            Duration::from_secs(10),
            8192,
            Limits { cpu_secs: 5, fsize_bytes: 0, mem_bytes: 0 },
        )
    }

    #[tokio::test]
    async fn a_skill_script_runs_in_place_with_workspace_cwd() {
        let ws = std::env::temp_dir().join("forkd-test-skill-ws");
        let skills = std::env::temp_dir().join("forkd-test-skills/demo/scripts");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::create_dir_all(&skills).unwrap();
        // The script proves in-place exec (its own path) and workspace cwd (pwd).
        std::fs::write(skills.join("hello.sh"), "echo \"skill says $1 from $(pwd)\"\n").unwrap();
        let out = forkd_with_skills(&ws, Some(skills.parent().unwrap().parent().unwrap()))
            .run_script(&json!({
                "skill_path": "demo/scripts/hello.sh",
                "interpreter": "bash",
                "args": ["hi"]
            }))
            .await
            .unwrap();
        assert_eq!(out["exit_code"], 0);
        let stdout = out["stdout"].as_str().unwrap();
        assert!(stdout.contains("skill says hi"));
        assert!(stdout.contains(ws.file_name().unwrap().to_str().unwrap()));
    }

    #[tokio::test]
    async fn a_skill_path_escape_is_rejected() {
        let ws = std::env::temp_dir().join("forkd-test-skill-jail");
        std::fs::create_dir_all(&ws).unwrap();
        let out = forkd_with_skills(&ws, Some(&ws))
            .run_script(&json!({ "skill_path": "../../etc/passwd" }))
            .await;
        assert!(out.is_err());
    }

    #[tokio::test]
    async fn runs_inline_bash_and_captures_output() {
        let ws = std::env::temp_dir().join("forkd-test-bash");
        std::fs::create_dir_all(&ws).unwrap();
        let out = forkd(&ws)
            .run_script(&json!({ "script": "echo hi from forkd; exit 3", "interpreter": "bash" }))
            .await
            .unwrap();
        assert_eq!(out["exit_code"], 3);
        assert!(out["stdout"].as_str().unwrap().contains("hi from forkd"));
        assert_eq!(out["timed_out"], false);
    }

    #[tokio::test]
    async fn a_hang_is_killed_by_the_timeout() {
        let ws = std::env::temp_dir().join("forkd-test-hang");
        std::fs::create_dir_all(&ws).unwrap();
        let out = forkd(&ws)
            .run_script(&json!({ "script": "sleep 30", "interpreter": "bash", "timeout_secs": 1 }))
            .await
            .unwrap();
        assert_eq!(out["timed_out"], true);
    }

    #[tokio::test]
    async fn a_path_escape_is_rejected() {
        let ws = std::env::temp_dir().join("forkd-test-jail");
        std::fs::create_dir_all(&ws).unwrap();
        let out = forkd(&ws).run_script(&json!({ "path": "../../etc/passwd" })).await;
        assert!(out.is_err());
    }
}
