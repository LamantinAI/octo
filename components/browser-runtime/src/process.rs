use std::{io::ErrorKind, net::SocketAddr, process::Stdio};

use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    spawn,
};

use super::MoliSettings;

pub(super) struct MoliProcess(Child);

impl MoliProcess {
    pub(super) async fn launch(settings: &MoliSettings) -> Result<(Self, SocketAddr), String> {
        let mut child = Command::new(&settings.executable)
            .arg("serve")
            .arg("--host")
            .arg(settings.bind.ip().to_string())
            .arg("--port")
            .arg(settings.bind.port().to_string())
            // Moli announces its chosen port on stderr at info level.
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| match error.kind() {
                ErrorKind::NotFound => {
                    "moli: executable not found; check browser_runtime.executable or the service's PATH".into()
                }
                _ => format!("moli: launch: {error}"),
            })?;
        let stderr = child.stderr.take().expect("piped Moli stderr");
        let mut lines = BufReader::new(stderr).lines();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| format!("moli: startup: {e}"))?
        {
            if let Some(endpoint) = listening_on(&line) {
                if endpoint.ip() != settings.bind.ip()
                    || (settings.bind.port() != 0 && endpoint.port() != settings.bind.port())
                {
                    return Err("moli: started on a different address than configured".into());
                }
                spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
                return Ok((Self(child), endpoint));
            }
        }
        Err("moli: exited before opening its control port".into())
    }

    pub(super) fn is_running(&mut self) -> Result<bool, String> {
        self.0
            .try_wait()
            .map(|state| state.is_none())
            .map_err(|e| format!("moli: check process: {e}"))
    }
}

pub(super) fn listening_on(line: &str) -> Option<SocketAddr> {
    let (_, addr) = line.split_once("protocol server listening addr=")?;
    let addr: SocketAddr = addr.trim().parse().ok()?;
    (addr.ip().is_loopback() && addr.port() != 0).then_some(addr)
}
