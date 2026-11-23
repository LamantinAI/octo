use std::{env::var, path::Path, process::Stdio, time::Duration};

use mcp_http::{Client, redirect::Policy};
use rmcp::{
    ServiceExt, model::ClientInfo, model::Implementation, service::RoleClient,
    service::RunningService, transport::StreamableHttpClientTransport,
    transport::TokioChildProcess,
    transport::streamable_http_client::StreamableHttpClientTransportConfig,
};
use tokio::{process::Command, time::timeout};

use crate::{ClientLimits, Credentials, McpError, Result, ServerConfig};

pub(crate) type Session = RunningService<RoleClient, ClientInfo>;
pub(crate) async fn connect(
    config: &ServerConfig,
    base: &Path,
    limits: &ClientLimits,
    credentials: &dyn Credentials,
) -> Result<Session> {
    let duration = Duration::from_millis(limits.connect_timeout_ms);
    timeout(duration, async {
        let mut info = ClientInfo::default();
        info.client_info = Implementation::new("octo-mcp", env!("CARGO_PKG_VERSION"));
        match config {
            ServerConfig::Http { url, token_env } => {
                let mut transport = StreamableHttpClientTransportConfig::with_uri(url.clone())
                    .reinit_on_expired_session(false);
                if let Some(reference) = token_env {
                    transport = transport.auth_header(
                        credentials
                            .resolve(reference)
                            .filter(|value| !value.is_empty())
                            .ok_or(McpError::CredentialUnavailable)?,
                    );
                }
                let client = Client::builder()
                    .redirect(Policy::none())
                    .connect_timeout(duration)
                    .build()
                    .map_err(|_| McpError::Configuration)?;
                info.serve(StreamableHttpClientTransport::with_client(
                    client, transport,
                ))
                .await
                .map_err(|_| McpError::Connection)
            }
            ServerConfig::Stdio {
                command,
                args,
                env,
                inherited_env,
            } => {
                let path = Path::new(command);
                let command = if path.is_relative() && path.components().count() > 1 {
                    base.join(path)
                } else {
                    path.to_path_buf()
                };
                let mut child = Command::new(command);
                child
                    .args(args)
                    .current_dir(base)
                    .env_clear()
                    .stderr(Stdio::null())
                    .kill_on_drop(true);
                for name in inherited_env {
                    if let Ok(value) = var(name) {
                        child.env(name, value);
                    }
                }
                for (name, reference) in env {
                    child.env(
                        name,
                        credentials
                            .resolve(reference)
                            .filter(|value| !value.is_empty())
                            .ok_or(McpError::CredentialUnavailable)?,
                    );
                }
                let (transport, _) = TokioChildProcess::builder(child)
                    .stderr(Stdio::null())
                    .spawn()
                    .map_err(|_| McpError::Connection)?;
                info.serve(transport)
                    .await
                    .map_err(|_| McpError::Connection)
            }
        }
    })
    .await
    .map_err(|_| McpError::Timeout)?
}
