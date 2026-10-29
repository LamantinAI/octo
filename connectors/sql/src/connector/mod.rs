use std::{collections::HashMap, error::Error, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use octo_approval::{ApprovalAction, ApprovalPresentation, Authority};
use octo_core::{
    ChannelId, Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId,
    Envelope, EventId, EventKind, FactoryContext, Filter, OctoError, OctoResult, SubscribeOptions,
    control::{CANCEL, CANCEL_SCOPE_TAG},
};
use serde_json::{Value, json};
use tokio::{runtime::Handle, sync::Mutex, task::JoinSet, task::spawn_blocking};
use toml::Value as TomlValue;
use tracing::warn;

use crate::{
    Database, Limits,
    database::{Completion, Notice},
    engine::Cancellation,
};

const COMMANDS: &[&str] = &[
    "sql.store.create",
    "sql.store.list",
    "sql.store.search",
    "sql.store.describe",
    "sql.store.update_metadata",
    "sql.store.archive",
    "sql.store.drop",
    "sql.execute",
    "sql.schema.apply",
    "sql.operation.status",
    "sql.approval.list",
];
const CATALOG: &str = "SQLite application stores (separate from host memory/history). All calls require host-bound identity. \
Use sql.store.create {name,purpose,tags?,idempotency_key}; list/search {query?,cursor?,limit?}; describe {store_id,tables?}. \
sql.schema.apply {store_id,expected_revision,idempotency_key,table,columns:[{name,data_type,nullable?,primary_key?,semantics?}],semantics?} creates a table and semantic metadata. \
sql.execute {store_id,expected_revision,sql,parameters?:[{type,value}],idempotency_key} runs one native SQLite statement in the returned generated namespace. \
Types: null,integer,real,text,blob. Writes/schema need an idempotency key. Describe again after schema changes. \
sql.store.update_metadata {store_id,expected_revision,metadata:[{object_id,column_id?,semantics}]} updates meanings/units/null/time metadata by stable IDs. \
sql.store.archive/drop {store_id,expected_revision,idempotency_key}; archived stores count toward quota20. \
Destructive SQL returns approval_required; only the authenticated human may approve privately. \
sql.operation.status {operation_id,include_details?} retrieves persisted results or exact operation details; never repeat uncertain work with a new key. No automatic backups or undo.";

pub struct SqlConnectorFactory {
    authority: Authority,
    protected_paths: Vec<PathBuf>,
}
impl SqlConnectorFactory {
    pub fn new(authority: Authority, protected_paths: Vec<PathBuf>) -> Self {
        Self {
            authority,
            protected_paths,
        }
    }
}
pub fn factory(authority: Authority, protected_paths: Vec<PathBuf>) -> Arc<dyn ConnectorFactory> {
    Arc::new(SqlConnectorFactory::new(authority, protected_paths))
}
impl ConnectorFactory for SqlConnectorFactory {
    fn type_name(&self) -> &str {
        "sql"
    }
    fn create(
        &self,
        id: ConnectorId,
        config: &TomlValue,
        ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn Error + Send + Sync>> {
        let mut table = config
            .get("connector")
            .and_then(TomlValue::as_table)
            .cloned()
            .ok_or("sql: missing [connector]")?;
        table.remove("type");
        table.remove("id");
        let path = table
            .remove("path")
            .and_then(|v| v.as_str().map(str::to_owned))
            .ok_or("sql: connector path required")?;
        let limits: Limits = TomlValue::Table(table).try_into()?;
        limits.validate()?;
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds(COMMANDS.iter().map(|kind| EventKind::new(*kind)))
            .with_description(CATALOG);
        Ok(Arc::new(SqlConnector {
            id,
            path: ctx.base_dir.join(path),
            limits,
            authority: self.authority.clone(),
            protected_paths: self.protected_paths.clone(),
            capabilities,
        }))
    }
}
struct SqlConnector {
    id: ConnectorId,
    path: PathBuf,
    limits: Limits,
    authority: Authority,
    protected_paths: Vec<PathBuf>,
    capabilities: ConnectorCapabilities,
}
#[async_trait]
impl Connector for SqlConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let path = self.path.clone();
        let limits = self.limits.clone();
        let authority = self.authority.clone();
        let protected = self.protected_paths.clone();
        let runtime = Handle::current();
        let database = spawn_blocking(move || {
            runtime.block_on(Database::open(path, limits, authority, protected))
        })
        .await
        .map_err(OctoError::other)?
        .map_err(OctoError::other)?;
        let mut commands = ctx
            .subscribe(
                Filter::by_target(self.id.clone()),
                SubscribeOptions::default(),
            )
            .await?;
        let mut controls = ctx
            .subscribe(
                Filter::by_kind(EventKind::from_static(CANCEL)),
                SubscribeOptions::default(),
            )
            .await?;
        let running: Arc<Mutex<HashMap<EventId, (Option<String>, Cancellation)>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<HashMap<String, (Option<String>, Cancellation, i64)>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                _ = ctx.shutdown.cancelled() => {
                    for (_, flag) in running.lock().await.values() { flag.cancel(); }
                    for (_,flag,_) in pending.lock().await.values() { flag.cancel(); }
                    while tasks.join_next().await.is_some() {}
                    return Ok(());
                }
                message = controls.next() => if let Some(envelope) = message {
                    if let Some(scope) = envelope.payload_as::<String>() {
                        for (stored, flag) in running.lock().await.values() { if stored.as_ref() == Some(scope) { flag.cancel(); } }
                        let ids = {
                            let registry = pending.lock().await;
                            registry.iter().filter_map(|(id,(stored,flag,_))| {
                                if stored.as_ref() == Some(scope) { flag.cancel(); Some(id.clone()) } else { None }
                            }).collect::<Vec<_>>()
                        };
                        if let Err(error) = database.cancel_pending(&ids).await { warn!(%error,"SQL pending cancellation persistence failed"); }
                    }
                },
                message = commands.next() => {
                    let Some(envelope) = message else { return Ok(()); };
                    if tasks.len() >= self.limits.max_pending {
                        let reply = Envelope::new(self.id.clone(), EventKind::new(format!("{}.result", envelope.kind)), json!({"error":"SQL operation queue is full"})).with_correlation(envelope.id);
                        ctx.publish(reply).await?;
                        continue;
                    }
                    let database = database.clone();
                    let running = running.clone();
                    let pending = pending.clone();
                    let connector = self.clone();
                    let context = ctx.clone();
                    let cancellation = Cancellation::default();
                    let scope = envelope.tags.get(CANCEL_SCOPE_TAG).cloned();
                    running.lock().await.insert(envelope.id, (scope.clone(), cancellation.clone()));
                    tasks.spawn(async move {
                        let completed = database.handle_complete(&envelope, cancellation.clone()).await;
                        let Completion { value: payload, notice } = completed.unwrap_or_else(|e| Completion { value: json!({"error":e.to_string()}), notice: None });
                        if let Some(notice) = notice {
                            let mut registry = pending.lock().await;
                            registry.retain(|_,(_,_,expires)| *expires > Utc::now().timestamp());
                            if let Some(operation) = payload["operation_id"].as_str() { registry.entry(operation.into()).or_insert((scope, cancellation, payload["expires_at"].as_i64().unwrap_or(0))); }
                            drop(registry);
                            connector.prompt(&notice, &payload, &context).await;
                        } else if let Some(operation) = payload["operation_id"].as_str() { pending.lock().await.remove(operation); }
                        let reply = Envelope::new(connector.id.clone(), EventKind::new(format!("{}.result", envelope.kind)), payload).with_correlation(envelope.id);
                        if let Err(error) = context.publish(reply).await { warn!(%error, "SQL result delivery failed"); }
                        running.lock().await.remove(&envelope.id);
                    });
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
    }
}
impl SqlConnector {
    async fn prompt(&self, notice: &Notice, payload: &Value, ctx: &ConnectorContext) {
        let target = &notice.connector;
        let destination = &notice.destination;
        let operation = payload["operation_id"].as_str().unwrap_or("");
        let presentation = ApprovalPresentation {
            executor: self.id.to_string(),
            operation_id: operation.into(),
            operation_hash: payload["operation_hash"].as_str().unwrap_or("").into(),
            recipient_principal: notice.principal.clone(),
            private_destination: destination.clone(),
            expires_at: payload["expires_at"].as_i64().unwrap_or(0),
            resource: notice.summary.resource.clone(),
            action: notice.summary.action,
            objects: notice.summary.objects.clone(),
            decision_kind: "sql.approval.decide".into(),
            status_kind: "sql.operation.status".into(),
        };
        let name: String = presentation
            .resource
            .chars()
            .filter(|c| {
                !c.is_control() && !matches!(*c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
            })
            .take(120)
            .collect();
        let action = if presentation.action == ApprovalAction::DeleteStore {
            "Delete store"
        } else {
            "Change data in store"
        };
        let text = format!(
            "{action} “{name}”?\nThis operation may permanently remove data. The connector cannot undo it.\n/sqlapprove {} {}\n/sqldeny {} {}",
            self.id, operation, self.id, operation
        );
        let reply = Envelope::new(self.id.clone(), EventKind::from_static("chat.reply"), text)
            .with_target(ConnectorId::new(target))
            .with_channel(ChannelId::new(destination))
            .with_context(presentation);
        if let Err(error) = ctx.publish(reply).await {
            warn!(%error, "SQL private approval presentation failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use octo_approval::{Access, ApprovalAction, ApprovalPresentation, Authority, Origin};
    use octo_core::{
        Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventBus,
        EventKind, Filter, InProcessBus, SubscribeOptions, control::CancellationToken,
    };
    use serde_json::{Value, json};
    use tempfile::tempdir;
    use tokio::{
        spawn,
        time::{sleep, timeout},
    };

    use super::SqlConnector;
    use crate::{Database, Limits};

    fn signed(authority: &Authority, kind: &str, payload: Value) -> Envelope {
        let ingress = authority.issue(
            Origin {
                principal: "owner".into(),
                connector: "telegram".into(),
                conversation: "group-123".into(),
                private_destination: "42".into(),
                may_approve: true,
            },
            Access::owner(),
        );
        let envelope = Envelope::new(ConnectorId::new("agent"), EventKind::new(kind), payload)
            .with_target(ConnectorId::new("sql"));
        let proof = authority.bind(&ingress, &envelope).unwrap();
        envelope.with_context(proof)
    }
    #[tokio::test]
    async fn actor_publishes_friendly_native_card_only_to_authenticated_private_destination() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sql.db");
        let authority = Authority::new(Duration::from_secs(60), 100);
        let db = Database::open(&path, Limits::default(), authority.clone(), vec![])
            .await
            .unwrap();
        let store = db
            .handle(&signed(
                &authority,
                "sql.store.create",
                json!({"name":"test","idempotency_key":"create"}),
            ))
            .await
            .unwrap()["store_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let table = format!("s_{store}__data");
        db.handle(&signed(&authority,"sql.execute",json!({"store_id":store,"expected_revision":1,"sql":format!("CREATE TABLE {table}(id INTEGER)"),"idempotency_key":"schema"}))).await.unwrap();
        let bus = Arc::new(InProcessBus::new(32));
        let shutdown = CancellationToken::new();
        let context = ConnectorContext::new(shutdown.clone(), bus.clone());
        let mut private = bus
            .subscribe(
                Filter::by_target(ConnectorId::new("telegram")),
                SubscribeOptions::default(),
            )
            .await
            .unwrap();
        let connector = Arc::new(SqlConnector {
            id: ConnectorId::new("sql"),
            path: path.clone(),
            limits: Limits::default(),
            authority: authority.clone(),
            protected_paths: vec![],
            capabilities: ConnectorCapabilities::bidirectional(),
        });
        let task = spawn(connector.run(context));
        sleep(Duration::from_millis(40)).await;
        let reply=bus.publish_and_await_response(signed(&authority,"sql.execute",json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"delete","parameters":[]})),Duration::from_secs(2)).await.unwrap();
        assert_eq!(
            reply.payload_as::<Value>().unwrap()["status"],
            "approval_required"
        );
        let prompt = timeout(Duration::from_secs(1), private.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prompt.kind.as_str(), "chat.reply");
        assert_eq!(prompt.channel.as_ref().unwrap().as_str(), "42");
        let text = prompt
            .payload_as::<String>()
            .expect("Telegram/console consume String messages");
        assert!(text.contains("/sqlapprove sql "));
        assert!(text.contains("/sqldeny sql "));
        assert!(text.contains("test"));
        assert!(!text.contains(path.to_str().unwrap()));
        assert!(!text.contains("Hash:"));
        assert!(!text.contains("expected_revision"));
        let card = prompt.context.get::<ApprovalPresentation>().unwrap();
        assert_eq!(card.action, ApprovalAction::DeleteRecords);
        assert_eq!(card.recipient_principal, "owner");
        assert_eq!(card.private_destination, "42");
        assert_eq!(card.objects, vec!["data"]);
        shutdown.cancel();
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
