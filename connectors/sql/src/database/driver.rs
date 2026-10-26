use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use octo_approval::{Authority, VerifiedInvocation};
use octo_core::Envelope;
use serde_json::{Value, from_value, to_vec};
use sqlx::SqliteConnection;
use tokio::{
    runtime::Handle,
    sync::{Mutex, Semaphore},
    task::spawn_blocking,
};

use super::presentation::PresentationSnapshot;
use super::service::{
    check_protected, database_identity, migrate, prepare_path, validate_ownership,
};
use crate::{
    config::Limits,
    engine::{Cancellation, Engine},
    error::{SqlError, invalid},
};

pub(crate) struct Notice {
    pub connector: String,
    pub destination: String,
    pub principal: String,
    pub summary: PresentationSnapshot,
}
pub(crate) struct Completion {
    pub value: Value,
    pub notice: Option<Notice>,
}

pub struct Database {
    pub(super) engine: Engine,
    pub(super) limits: Limits,
    pub(super) authority: Authority,
    pub(super) connection: Mutex<SqliteConnection>,
    pub(super) queue: Arc<Semaphore>,
    pub(super) identity: String,
    pub(super) pending_cancellations: Mutex<HashMap<String, Cancellation>>,
}
impl Database {
    pub async fn open(
        path: impl AsRef<Path>,
        limits: Limits,
        authority: Authority,
        protected_paths: Vec<PathBuf>,
    ) -> Result<Arc<Self>, SqlError> {
        limits.validate()?;
        let path = prepare_path(path.as_ref(), &protected_paths)?;
        let engine = Engine::open(path, limits.engine()).await?;
        check_protected(engine.path(), &protected_paths)?;
        let mut connection = engine.connect().await?;
        validate_ownership(&mut connection).await?;
        migrate(&mut connection).await?;
        let identity = format!(
            "{}:{}",
            database_identity(&mut connection).await?,
            engine.path().display()
        );
        Ok(Arc::new(Self {
            queue: Arc::new(Semaphore::new(limits.max_pending)),
            engine,
            limits,
            authority,
            connection: Mutex::new(connection),
            identity,
            pending_cancellations: Mutex::new(HashMap::new()),
        }))
    }
    pub fn path(&self) -> &Path {
        self.engine.path()
    }
    pub async fn handle(self: &Arc<Self>, envelope: &Envelope) -> Result<Value, SqlError> {
        self.handle_with_cancellation(envelope, Cancellation::default())
            .await
    }
    pub async fn handle_with_cancellation(
        self: &Arc<Self>,
        envelope: &Envelope,
        cancellation: Cancellation,
    ) -> Result<Value, SqlError> {
        Ok(self.handle_complete(envelope, cancellation).await?.value)
    }
    pub(crate) async fn handle_complete(
        self: &Arc<Self>,
        envelope: &Envelope,
        cancellation: Cancellation,
    ) -> Result<Completion, SqlError> {
        let decision = envelope.kind.as_str() == "sql.approval.decide";
        let mut invocation = if decision {
            self.authority.consume_decision(envelope)
        } else {
            self.authority.consume(envelope)
        }
        .map_err(SqlError::Authority)?;
        let payload = envelope
            .payload_as::<Value>()
            .cloned()
            .ok_or_else(|| invalid("JSON payload required"))?;
        if to_vec(&payload).map_err(invalid)?.len() > self.limits.max_bytes {
            return Err(SqlError::Limit("request bytes".into()));
        }
        let kind = envelope.kind.as_str().to_owned();
        let permit = self
            .queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| SqlError::Limit("operation queue".into()))?;
        let database = self.clone();
        let runtime = Handle::current();
        let deadline = Instant::now() + invocation.remaining;
        let cancellation = cancellation.with_deadline(deadline);
        spawn_blocking(move || {
            runtime.block_on(async move {
                let _permit = permit;
                let mut conn = database.connection.lock().await;
                if cancellation.is_cancelled() || Instant::now() >= deadline {
                    return Err(SqlError::Authority(
                        "invocation expired/cancelled in queue".into(),
                    ));
                }
                invocation.remaining = deadline.saturating_duration_since(Instant::now());
                database
                    .authority
                    .check_scope(invocation.scope_binding.as_ref())
                    .map_err(SqlError::Authority)?;
                database.engine.validate_identity()?;
                let value = database
                    .dispatch(&mut conn, &kind, payload, invocation, cancellation.clone())
                    .await?;
                if value.to_string().len() > database.limits.max_bytes {
                    return Err(SqlError::Limit("response bytes".into()));
                }
                let notice = if value["status"] == "approval_required" {
                    let operation = database
                        .operation(
                            &mut conn,
                            value["operation_id"]
                                .as_str()
                                .ok_or_else(|| invalid("operation missing"))?,
                        )
                        .await?;
                    let mut pending = database.pending_cancellations.lock().await;
                    pending.retain(|_, token| !token.deadline_elapsed());
                    pending
                        .entry(operation.prepared.id.clone())
                        .or_insert(cancellation);
                    Some(Notice {
                        principal: operation.prepared.principal,
                        summary: operation.scope.presentation,
                        connector: operation.scope.connector,
                        destination: operation.scope.destination,
                    })
                } else {
                    if let Some(operation) = value["operation_id"].as_str() {
                        database
                            .pending_cancellations
                            .lock()
                            .await
                            .remove(operation);
                    }
                    None
                };
                Ok(Completion { value, notice })
            })
        })
        .await
        .map_err(|_| SqlError::Worker)?
    }
    async fn dispatch(
        &self,
        conn: &mut SqliteConnection,
        kind: &str,
        payload: Value,
        invocation: VerifiedInvocation,
        cancellation: Cancellation,
    ) -> Result<Value, SqlError> {
        match kind {
            "sql.store.create" => {
                self.create_store(conn, from_value(payload).map_err(invalid)?, &invocation)
                    .await
            }
            "sql.store.list" | "sql.store.search" => {
                self.list_stores(conn, from_value(payload).map_err(invalid)?, &invocation)
                    .await
            }
            "sql.store.describe" => {
                self.describe_store(conn, from_value(payload).map_err(invalid)?, &invocation)
                    .await
            }
            "sql.store.update_metadata" => {
                self.update_metadata(conn, from_value(payload).map_err(invalid)?, &invocation)
                    .await
            }
            "sql.store.archive" => {
                self.archive_store(conn, from_value(payload).map_err(invalid)?, &invocation)
                    .await
            }
            "sql.store.drop" => {
                self.prepare_drop(
                    conn,
                    from_value(payload).map_err(invalid)?,
                    &invocation,
                    cancellation,
                )
                .await
            }
            "sql.execute" => {
                self.prepare_sql(
                    conn,
                    from_value(payload).map_err(invalid)?,
                    &invocation,
                    cancellation,
                )
                .await
            }
            "sql.schema.apply" => {
                self.apply_schema(
                    conn,
                    from_value(payload).map_err(invalid)?,
                    &invocation,
                    cancellation,
                )
                .await
            }
            "sql.approval.decide" => {
                self.decide(
                    conn,
                    from_value(payload).map_err(invalid)?,
                    &invocation,
                    cancellation,
                )
                .await
            }
            "sql.operation.status" => {
                self.operation_status(conn, from_value(payload).map_err(invalid)?, &invocation)
                    .await
            }
            _ => Err(invalid("unknown SQL command")),
        }
    }
}
