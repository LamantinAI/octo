//! A control restart of a manifest-declared connector rebuilds it from its manifest as the
//! file is NOW — so an edited manifest takes effect on `restart_connector`, without
//! restarting the process. (It used to re-run the instance built at startup, silently
//! keeping the old settings.)

use std::{
    fs::{create_dir_all, remove_dir_all, write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use octo_core::{
    control::RESTART_CONNECTOR, Connector, ConnectorCapabilities, ConnectorContext,
    ConnectorFactory, ConnectorId, Envelope, EventKind, FactoryContext, Octo, OctoResult,
};
use tokio_util::sync::CancellationToken;

type Log = Arc<Mutex<Vec<String>>>;

/// Records its `[connector] value` each time it starts, then idles until stopped.
struct Probe {
    id: ConnectorId,
    caps: ConnectorCapabilities,
    value: String,
    log: Log,
}

#[async_trait]
impl Connector for Probe {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.caps
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        self.log.lock().unwrap().push(self.value.clone());
        ctx.shutdown.cancelled().await;
        Ok(())
    }
}

struct ProbeFactory {
    log: Log,
}

impl ConnectorFactory for ProbeFactory {
    fn type_name(&self) -> &str {
        "probe"
    }
    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let value = config["connector"].get("value").and_then(|v| v.as_str()).unwrap_or("").to_string();
        Ok(Arc::new(Probe { id, caps: ConnectorCapabilities::bidirectional(), value, log: self.log.clone() }))
    }
}

/// Code-built: waits for the probe's first start, edits its manifest, asks for a restart of
/// the probe exactly as octo-rig's restart tool does, and ends the run once the rebuilt
/// probe has started (or on a deadline, so a regression fails instead of hanging).
struct Driver {
    id: ConnectorId,
    caps: ConnectorCapabilities,
    log: Log,
    manifest: PathBuf,
    stop: CancellationToken,
}

impl Driver {
    async fn wait_for(&self, value: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if self.log.lock().unwrap().iter().any(|v| v == value) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }
}

#[async_trait]
impl Connector for Driver {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.caps
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        if self.wait_for("cove").await {
            write(&self.manifest, "[connector]\nid = \"probe\"\ntype = \"probe\"\nvalue = \"spruce\"\n").unwrap();
            let restart = Envelope::new(
                self.id.clone(),
                EventKind::from_static(RESTART_CONNECTOR),
                "probe".to_string(),
            );
            ctx.publish(restart).await?;
            self.wait_for("spruce").await;
        }
        self.stop.cancel();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_connector_applies_an_edited_manifest() {
    let dir = std::env::temp_dir().join(format!("octo-restart-reload-{}", std::process::id()));
    let connectors = dir.join("connectors");
    create_dir_all(&connectors).unwrap();
    write(dir.join("octo.toml"), "[connectors]\ndir = \"connectors\"\n").unwrap();
    let manifest = connectors.join("probe.toml");
    write(&manifest, "[connector]\nid = \"probe\"\ntype = \"probe\"\nvalue = \"cove\"\n").unwrap();

    let log: Log = Arc::default();
    let stop = CancellationToken::new();
    let driver = Arc::new(Driver {
        id: ConnectorId::new("driver"),
        caps: ConnectorCapabilities::bidirectional(),
        log: log.clone(),
        manifest,
        stop: stop.clone(),
    });
    let octo = Octo::builder()
        .register_connector_type("probe", Arc::new(ProbeFactory { log: log.clone() }))
        .from_config_file(dir.join("octo.toml"))
        .expect("the manifest loads")
        .add_connector(driver)
        .shutdown_token(stop)
        .build();

    tokio::time::timeout(Duration::from_secs(10), octo.run())
        .await
        .expect("the runtime winds down")
        .expect("the runtime runs cleanly");
    remove_dir_all(&dir).ok();

    assert_eq!(*log.lock().unwrap(), vec!["cove".to_string(), "spruce".to_string()]);
}
