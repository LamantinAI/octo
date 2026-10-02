use std::{process::Stdio, sync::atomic::Ordering, time::Duration};

use octo_workspace::workspace_root;
use serde_json::{Value, json};
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::{ForkdConnector, apply_limits, cap, ext_of, jailed, kill_group, prepend};

impl ForkdConnector {
    pub(super) async fn run_script(
        &self,
        params: &Value,
        cancel: &CancellationToken,
    ) -> Result<Value, String> {
        if cancel.is_cancelled() {
            return Ok(json!({"error":"cancelled before execution", "cancelled":true}));
        }
        let workspace = workspace_root(self.workspace.as_deref()).map_err(|e| e.to_string())?;
        let interpreter = params.get("interpreter").and_then(Value::as_str);
        let inline = params.get("script").and_then(Value::as_str);
        let user_args: Vec<String> = params
            .get("args")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let stdin = params
            .get("stdin")
            .and_then(Value::as_str)
            .map(String::from);
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
            .env(
                "PATH",
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            )
            .env("HOME", &workspace)
            .env("TMPDIR", &workspace)
            .env("LANG", "C.UTF-8")
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            });
        // Forward the manifest's allowlisted env vars (by name) from our own process.
        for name in &self.env_passthrough {
            if let Some(val) = std::env::var_os(name) {
                cmd.env(name, val);
            }
        }
        cmd.stdout(Stdio::piped())
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
        // The run ends on whichever comes first: the script finishing, the wall-clock
        // timeout, or an external cancel (octo.control.cancel for this run's scope).
        // Both the timeout and the cancel kill the whole process group.
        tokio::select! {
            res = timeout(dur, child.wait_with_output()) => match res {
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
            },
            _ = cancel.cancelled() => {
                if let Some(p) = pid {
                    kill_group(p);
                }
                Ok(json!({
                    "exit_code": Value::Null,
                    "stdout": "",
                    "stderr": "(cancelled)",
                    "timed_out": false,
                    "cancelled": true,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;
    use std::{path::Path, sync::Arc};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn command_then_cancel_on_the_bus_is_not_lost_before_registration() {
        use octo_core::{
            Connector, ConnectorContext, ConnectorId, Envelope, EventBus, EventKind, Filter,
            InProcessBus, SubscribeOptions,
            control::{CANCEL, CANCEL_SCOPE_TAG},
        };
        use std::{
            future::{Future, poll_fn},
            task::Poll,
        };
        use tokio::{spawn, time::timeout};

        let ws = std::env::temp_dir().join(format!("forkd-bus-cancel-{}", std::process::id()));
        std::fs::create_dir_all(&ws).unwrap();
        let bus = Arc::new(InProcessBus::new(32));
        let shutdown = CancellationToken::new();
        let ctx = ConnectorContext::new(shutdown.clone(), bus.clone());
        let mut run = Box::pin(forkd(&ws).run(ctx));
        // Poll to the receiving loop before publishing, without a timing-based sleep.
        poll_fn(|cx| {
            assert!(run.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let runner = spawn(run);
        let mut results = bus
            .subscribe(
                Filter::by_kind("forkd.run.result"),
                SubscribeOptions::default(),
            )
            .await
            .unwrap();
        let command = Envelope::new(
            ConnectorId::new("controller"),
            EventKind::new("forkd.run"),
            json!({"script":"sleep 30", "interpreter":"bash"}),
        )
        .with_target(ConnectorId::new("forkd"))
        .with_tag(CANCEL_SCOPE_TAG, "operation-1");
        let request_id = command.id;
        bus.publish(command).await.unwrap();
        bus.publish(Envelope::new(
            ConnectorId::new("controller"),
            EventKind::from_static(CANCEL),
            "operation-1".to_string(),
        ))
        .await
        .unwrap();
        let reply = timeout(Duration::from_secs(3), results.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reply.correlation_id, Some(request_id));
        assert_eq!(reply.payload_as::<Value>().unwrap()["cancelled"], true);
        shutdown.cancel();
        timeout(Duration::from_secs(3), runner)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

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
            Limits {
                cpu_secs: 5,
                fsize_bytes: 0,
                mem_bytes: 0,
            },
            Vec::new(),
        )
    }

    #[tokio::test]
    async fn env_passthrough_forwards_only_allowlisted_names() {
        let ws = std::env::temp_dir().join("forkd-test-env-ws");
        std::fs::create_dir_all(&ws).unwrap();
        // SAFETY: unique var names, set before this test spawns the child.
        unsafe {
            std::env::set_var("FORKD_PASS_ME", "kept");
            std::env::set_var("FORKD_DROP_ME", "gone");
        }
        let conn = ForkdConnector::build(
            "forkd",
            Some(ws.clone()),
            None,
            None,
            Duration::from_secs(5),
            Duration::from_secs(10),
            8192,
            Limits {
                cpu_secs: 5,
                fsize_bytes: 0,
                mem_bytes: 0,
            },
            vec!["FORKD_PASS_ME".to_string()],
        );
        let out = conn
            .run_script(
                &json!({
                    "script": "echo \"pass=[${FORKD_PASS_ME:-}] drop=[${FORKD_DROP_ME:-}]\"",
                    "interpreter": "bash",
                }),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        let stdout = out["stdout"].as_str().unwrap_or("");
        assert!(
            stdout.contains("pass=[kept]"),
            "allowlisted var should pass: {stdout}"
        );
        assert!(
            stdout.contains("drop=[]"),
            "non-allowlisted var must stay cleared: {stdout}"
        );
    }

    #[tokio::test]
    async fn a_skill_script_runs_in_place_with_workspace_cwd() {
        let ws = std::env::temp_dir().join("forkd-test-skill-ws");
        let skills = std::env::temp_dir().join("forkd-test-skills/demo/scripts");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::create_dir_all(&skills).unwrap();
        // The script proves in-place exec (its own path) and workspace cwd (pwd).
        std::fs::write(
            skills.join("hello.sh"),
            "echo \"skill says $1 from $(pwd)\"\n",
        )
        .unwrap();
        let out = forkd_with_skills(&ws, Some(skills.parent().unwrap().parent().unwrap()))
            .run_script(
                &json!({
                    "skill_path": "demo/scripts/hello.sh",
                    "interpreter": "bash",
                    "args": ["hi"]
                }),
                &CancellationToken::new(),
            )
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
            .run_script(
                &json!({ "skill_path": "../../etc/passwd" }),
                &CancellationToken::new(),
            )
            .await;
        assert!(out.is_err());
    }

    #[tokio::test]
    async fn runs_inline_bash_and_captures_output() {
        let ws = std::env::temp_dir().join("forkd-test-bash");
        std::fs::create_dir_all(&ws).unwrap();
        let out = forkd(&ws)
            .run_script(
                &json!({ "script": "echo hi from forkd; exit 3", "interpreter": "bash" }),
                &CancellationToken::new(),
            )
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
            .run_script(
                &json!({ "script": "sleep 30", "interpreter": "bash", "timeout_secs": 1 }),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(out["timed_out"], true);
    }

    #[tokio::test]
    async fn a_cancel_token_stops_a_running_script() {
        let ws = std::env::temp_dir().join("forkd-test-cancel");
        std::fs::create_dir_all(&ws).unwrap();
        let conn = forkd(&ws);
        let token = CancellationToken::new();
        // A script that would otherwise run for 30s; fire the token shortly after start.
        let fire = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            fire.cancel();
        });
        let out = conn
            .run_script(
                &json!({ "script": "sleep 30", "interpreter": "bash", "timeout_secs": 30 }),
                &token,
            )
            .await
            .unwrap();
        assert_eq!(out["cancelled"], true, "got: {out}");
        assert_eq!(out["timed_out"], false);
    }

    #[tokio::test]
    async fn a_path_escape_is_rejected() {
        let ws = std::env::temp_dir().join("forkd-test-jail");
        std::fs::create_dir_all(&ws).unwrap();
        let out = forkd(&ws)
            .run_script(
                &json!({ "path": "../../etc/passwd" }),
                &CancellationToken::new(),
            )
            .await;
        assert!(out.is_err());
    }
}
