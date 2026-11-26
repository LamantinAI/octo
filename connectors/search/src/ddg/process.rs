use std::{io::ErrorKind, net::SocketAddr, process::Stdio};

#[cfg(unix)]
use libc::{SIGKILL, kill};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    spawn,
};

pub(super) struct DriverProcess(Child);

impl DriverProcess {
    pub(super) async fn start() -> Result<(Self, SocketAddr), String> {
        let mut command = Command::new("geckodriver");
        command
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                "0",
                "--websocket-port",
                "0",
                "--log",
                "info",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let child = command.spawn().map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                "ddg: install Firefox and geckodriver or configure webdriver_url".to_owned()
            } else {
                format!("ddg: could not start geckodriver: {error}")
            }
        })?;
        let mut process = Self(child);
        let stdout = process.0.stdout.take().expect("piped geckodriver stdout");
        let mut lines = BufReader::new(stdout).lines();
        while let Some(line) = lines.next_line().await.map_err(|error| error.to_string())? {
            if let Some(address) = listening_on(&line) {
                spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
                return Ok((process, address));
            }
        }
        Err("ddg: geckodriver exited before listening".into())
    }

    pub(super) async fn close(mut self) {
        self.terminate();
        let _ = self.0.wait().await;
    }

    fn terminate(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.0.id().and_then(|id| i32::try_from(id).ok()) {
            // This child leads a private group. Firefox can already have spawned
            // descendants when session creation is cancelled or times out.
            unsafe { kill(-pid, SIGKILL) };
        }
        let _ = self.0.start_kill();
    }
}

fn listening_on(line: &str) -> Option<SocketAddr> {
    let (_, addr) = line.split_once("geckodriver\tINFO\tListening on ")?;
    let addr: SocketAddr = addr.trim().parse().ok()?;
    (addr.ip().is_loopback() && addr.port() != 0).then_some(addr)
}

impl Drop for DriverProcess {
    fn drop(&mut self) {
        self.terminate();
    }
}
