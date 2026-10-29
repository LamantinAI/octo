//! Protected catalog, approval journal, receipts and serialized file ownership.
mod apply;
mod catalog;
mod execute;
mod operations;
mod presentation;
mod schema;
mod service;

#[cfg(test)]
mod scopes;

mod driver;
pub use driver::Database;
pub(crate) use driver::{Completion, Notice};

#[cfg(test)]
mod tests {
    use std::{
        fs::{hard_link, read, write},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use chrono::Utc;
    use octo_approval::{Access, Authority, Origin, ScopeBinding, ScopeValidator, Stores};
    use octo_core::{ConnectorId, Envelope, EventKind};
    use serde_json::{Value, json};
    use sqlx::{Row, query};
    use tempfile::{TempDir, tempdir};
    use tokio::{spawn, time::sleep};

    use super::Database;
    use crate::{Limits, SqlError};

    pub(super) struct Fixture {
        pub(super) dir: TempDir,
        pub(super) authority: Authority,
        pub(super) db: Arc<Database>,
    }
    impl Fixture {
        pub(super) async fn new(limits: Limits) -> Self {
            let dir = tempdir().unwrap();
            let authority = Authority::new(Duration::from_secs(120), 1000);
            let db = Database::open(dir.path().join("sql.db"), limits, authority.clone(), vec![])
                .await
                .unwrap();
            Self { dir, authority, db }
        }
        pub(super) fn envelope(
            &self,
            kind: &str,
            payload: Value,
            decision: bool,
            principal: &str,
        ) -> Envelope {
            let origin = Origin {
                principal: principal.into(),
                connector: "telegram".into(),
                conversation: "1".into(),
                private_destination: "1".into(),
                may_approve: true,
            };
            let ingress = self.authority.issue(origin, Access::owner());
            let envelope = Envelope::new(ConnectorId::new("test"), EventKind::new(kind), payload)
                .with_target(ConnectorId::new("sql"));
            if decision {
                let proof = self.authority.bind_decision(&ingress, &envelope).unwrap();
                envelope.with_context(proof)
            } else {
                let proof = self.authority.bind(&ingress, &envelope).unwrap();
                envelope.with_context(proof)
            }
        }
        pub(super) async fn call(&self, kind: &str, payload: Value) -> Result<Value, SqlError> {
            self.db
                .handle(&self.envelope(kind, payload, kind == "sql.approval.decide", "owner"))
                .await
        }
        pub(super) async fn store(&self) -> String {
            self.call(
                "sql.store.create",
                json!({"name":"test","idempotency_key":"create"}),
            )
            .await
            .unwrap()["store_id"]
                .as_str()
                .unwrap()
                .into()
        }
        pub(super) async fn table(&self, store: &str) -> String {
            let result = self.call("sql.schema.apply", json!({"store_id":store,"expected_revision":1,"idempotency_key":"schema",
                "table":"data","columns":[{"name":"id","data_type":"INTEGER","primary_key":true},{"name":"amount","data_type":"REAL","semantics":{"units":"kg"}}]})).await.unwrap();
            assert_eq!(result["status"], "committed", "{result}");
            format!("s_{store}__data")
        }
    }
    #[tokio::test]
    async fn durable_exact_approval_receipts_idempotency_and_forgery() {
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let insert = json!({"store_id":store,"expected_revision":2,"sql":format!("INSERT INTO {table} VALUES (?, ?)"),
            "parameters":[{"type":"integer","value":1},{"type":"real","value":4.5}],"idempotency_key":"insert"});
        let first = f.call("sql.execute", insert.clone()).await.unwrap();
        assert_eq!(first["status"], "committed", "{first}");
        assert_eq!(f.call("sql.execute", insert).await.unwrap(), first);
        let delete = json!({"store_id":store,"expected_revision":3,"sql":format!("DELETE FROM {table} WHERE id=?"),
            "parameters":[{"type":"integer","value":1}],"idempotency_key":"delete"});
        let pending = f.call("sql.execute", delete.clone()).await.unwrap();
        assert_eq!(pending["status"], "approval_required", "{pending}");
        let operation = pending["operation_id"].as_str().unwrap();
        let mut changed = delete.clone();
        changed["parameters"][0]["value"] = json!(2);
        assert!(f.call("sql.execute", changed).await.is_err());
        let forged = f.envelope(
            "sql.approval.decide",
            json!({"operation_id":operation,"approve":true}),
            false,
            "owner",
        );
        assert!(f.db.handle(&forged).await.is_err());
        assert!(
            f.db.handle(&f.envelope(
                "sql.approval.decide",
                json!({"operation_id":operation,"approve":true}),
                true,
                "other"
            ))
            .await
            .is_err()
        );
        let approved = f
            .call(
                "sql.approval.decide",
                json!({"operation_id":operation,"approve":true}),
            )
            .await
            .unwrap();
        assert_eq!(approved["status"], "committed", "{approved}");
        assert_eq!(
            f.call(
                "sql.approval.decide",
                json!({"operation_id":operation,"approve":true})
            )
            .await
            .unwrap(),
            approved
        );
        let reopened = Database::open(
            f.dir.path().join("sql.db"),
            Limits::default(),
            f.authority.clone(),
            vec![],
        )
        .await
        .unwrap();
        let status = reopened
            .handle(&f.envelope(
                "sql.operation.status",
                json!({"operation_id":operation}),
                false,
                "owner",
            ))
            .await
            .unwrap();
        assert_eq!(status, approved);
        let mut conn = f.db.connection.lock().await;
        let count: i64 = query("SELECT count(*) FROM __octo_sql_receipts WHERE operation_id=?")
            .bind(operation)
            .fetch_one(&mut *conn)
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 1);
    }
    #[tokio::test]
    async fn quota_is_atomic_and_archive_does_not_release_space() {
        let f = Fixture::new(Limits::default()).await;
        let mut jobs = Vec::new();
        for i in 0..21 {
            let db = f.db.clone();
            let envelope = f.envelope(
                "sql.store.create",
                json!({"name":format!("store{i}"),"idempotency_key":format!("key{i}")}),
                false,
                "owner",
            );
            jobs.push(spawn(async move { db.handle(&envelope).await }));
        }
        let mut success = 0;
        for job in jobs {
            if job.await.unwrap().is_ok() {
                success += 1;
            }
        }
        assert_eq!(success, 20);
        let listed = f.call("sql.store.list", json!({})).await.unwrap();
        let first = listed["stores"][0]["store_id"].as_str().unwrap();
        f.call(
            "sql.store.archive",
            json!({"store_id":first,"expected_revision":1,"idempotency_key":"archive"}),
        )
        .await
        .unwrap();
        assert!(
            f.call(
                "sql.store.create",
                json!({"name":"21","idempotency_key":"21"})
            )
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn semantic_revision_schema_drift_and_cross_store_isolation() {
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let described = f
            .call("sql.store.describe", json!({"store_id":store}))
            .await
            .unwrap();
        assert_eq!(
            described["objects"][0]["columns"][1]["semantics"]["units"],
            "kg"
        );
        let object = &described["objects"][0];
        f.call("sql.store.update_metadata",json!({"store_id":store,"expected_revision":2,"metadata":[{"object_id":object["id"],"column_id":object["columns"][1]["id"],"semantics":{"units":"g"}}]})).await.unwrap();
        assert!(f.call("sql.execute",json!({"store_id":store,"expected_revision":2,"sql":format!("SELECT * FROM {table}")})).await.is_err());
        for sql in [
            "SELECT * FROM __octo_sql_operations",
            "SELECT * FROM sqlite_schema",
            "SELECT * FROM s_other__data",
        ] {
            assert!(
                f.call(
                    "sql.execute",
                    json!({"store_id":store,"expected_revision":3,"sql":sql})
                )
                .await
                .is_err()
            );
        }
        assert!(
            f.db.handle(&f.envelope(
                "sql.store.describe",
                json!({"store_id":store}),
                false,
                "other"
            ))
            .await
            .is_err()
        );
        let mut external = f.db.engine.connect().await.unwrap();
        query(&format!("ALTER TABLE {table} ADD COLUMN external TEXT"))
            .execute(&mut external)
            .await
            .unwrap();
        assert!(f.call("sql.execute",json!({"store_id":store,"expected_revision":3,"sql":format!("SELECT * FROM {table}")})).await.is_err());
        let drift = f
            .call("sql.store.describe", json!({"store_id":store}))
            .await
            .unwrap();
        assert!(drift["revision"].as_u64().unwrap() > 3);
        assert_eq!(drift["objects"][0]["columns"][1]["semantics"], json!({}));
    }
    #[tokio::test]
    async fn protected_paths_rejected_before_any_internal_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history.db");
        write(&path, b"internal").unwrap();
        let authority = Authority::new(Duration::from_secs(60), 100);
        assert!(
            Database::open(
                &path,
                Limits::default(),
                authority.clone(),
                vec![path.clone()]
            )
            .await
            .is_err()
        );
        let alias = dir.path().join("alias.db");
        hard_link(&path, &alias).unwrap();
        assert!(
            Database::open(
                &alias,
                Limits::default(),
                authority.clone(),
                vec![path.clone()]
            )
            .await
            .is_err()
        );
        let candidate = dir.path().join("new.db");
        assert!(
            Database::open(
                &candidate,
                Limits::default(),
                authority,
                vec![dir.path().into()]
            )
            .await
            .is_err()
        );
        assert!(!candidate.exists());
        assert_eq!(read(&path).unwrap(), b"internal");
    }
    #[tokio::test]
    async fn queued_preparation_cannot_extend_authenticated_ingress_lifetime() {
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let origin = Origin {
            principal: "owner".into(),
            connector: "telegram".into(),
            conversation: "1".into(),
            private_destination: "1".into(),
            may_approve: true,
        };
        let started = Utc::now().timestamp_millis();
        let ingress = f
            .authority
            .issue(origin, Access::owner())
            .limit_lifetime(Duration::from_secs(3));
        let envelope=Envelope::new(ConnectorId::new("test"),EventKind::new("sql.execute"),json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"queued"})).with_target(ConnectorId::new("sql"));
        let proof = f.authority.bind(&ingress, &envelope).unwrap();
        let envelope = envelope.with_context(proof);
        let guard = f.db.connection.lock().await;
        let db = f.db.clone();
        let work = spawn(async move { db.handle(&envelope).await });
        sleep(Duration::from_secs(2)).await;
        drop(guard);
        let pending = work.await.unwrap().unwrap();
        assert_eq!(pending["status"], "approval_required", "{pending}");
        assert!(pending["expires_at"].as_i64().unwrap() * 1000 <= started + 3000);
    }

    struct QueueScope(AtomicBool);
    impl ScopeValidator for QueueScope {
        fn allows(&self, binding: &ScopeBinding) -> bool {
            binding.opaque_id == "queue-test"
                && binding.epoch == "epoch"
                && self.0.load(Ordering::SeqCst)
        }
    }

    async fn list_with(
        f: &Fixture,
        origin: Origin,
        access: Access,
        delegated: bool,
    ) -> Result<Value, SqlError> {
        let ingress = f.authority.issue(origin, access);
        let ingress = if delegated {
            ingress.attenuate(Vec::new(), false, false).unwrap()
        } else {
            ingress
        };
        let envelope = Envelope::new(
            ConnectorId::new("test"),
            EventKind::new("sql.approval.list"),
            json!({}),
        )
        .with_target(ConnectorId::new("sql"));
        let proof = f.authority.bind(&ingress, &envelope).unwrap();
        f.db.handle(&envelope.with_context(proof)).await
    }

    #[tokio::test]
    async fn approval_queue_requires_human_private_read_and_current_store_grants() {
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let pending = f.call("sql.execute", json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"queue"})).await.unwrap();
        assert_eq!(pending["status"], "approval_required");
        let origin = Origin {
            principal: "owner".into(),
            connector: "web".into(),
            conversation: "private".into(),
            private_destination: "private".into(),
            may_approve: true,
        };
        let listing = list_with(&f, origin.clone(), Access::owner(), false)
            .await
            .unwrap();
        assert_eq!(listing["items"][0]["operation_id"], pending["operation_id"]);
        let mut no_read = Access::owner();
        no_read.read = false;
        assert!(matches!(
            list_with(&f, origin.clone(), no_read, false).await,
            Err(SqlError::Authority(_))
        ));
        let mut no_approval = origin.clone();
        no_approval.may_approve = false;
        assert!(matches!(
            list_with(&f, no_approval, Access::owner(), false).await,
            Err(SqlError::Authority(_))
        ));
        let mut group = origin.clone();
        group.conversation = "group".into();
        assert!(matches!(
            list_with(&f, group, Access::owner(), false).await,
            Err(SqlError::Authority(_))
        ));
        assert!(matches!(
            list_with(&f, origin.clone(), Access::owner(), true).await,
            Err(SqlError::Authority(_))
        ));
        let mut restricted = Access::owner();
        restricted.stores = Stores::Only(vec!["another-store".into()]);
        assert_eq!(
            list_with(&f, origin.clone(), restricted, false)
                .await
                .unwrap()["items"],
            json!([])
        );
        let mut granted = Access::owner();
        granted.stores = Stores::Only(vec![store]);
        assert_eq!(
            list_with(&f, origin, granted, false).await.unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let unproved = Envelope::new(
            ConnectorId::new("test"),
            EventKind::new("sql.approval.list"),
            json!({}),
        )
        .with_target(ConnectorId::new("sql"));
        assert!(matches!(
            f.db.handle(&unproved).await,
            Err(SqlError::Authority(_))
        ));
    }

    #[tokio::test]
    async fn approval_queue_is_principal_scoped_and_excludes_decided_expired_or_closed_work() {
        let f = Fixture::new(Limits::default()).await;
        let live = Arc::new(QueueScope(AtomicBool::new(true)));
        f.authority.clone().with_scope_validator(live.clone());
        let store = f.store().await;
        let table = f.table(&store).await;
        let pending = f.call("sql.execute", json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"decided"})).await.unwrap();
        assert_eq!(
            f.db.handle(&f.envelope("sql.approval.list", json!({}), false, "another-owner"))
                .await
                .unwrap()["items"],
            json!([])
        );
        assert_eq!(
            f.call("sql.approval.list", json!({})).await.unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        f.call(
            "sql.approval.decide",
            json!({"operation_id":pending["operation_id"],"approve":false}),
        )
        .await
        .unwrap();
        assert_eq!(
            f.call("sql.approval.list", json!({})).await.unwrap()["items"],
            json!([])
        );

        let ingress = f.authority.issue(
            Origin {
                principal: "owner".into(),
                connector: "web".into(),
                conversation: "private".into(),
                private_destination: "private".into(),
                may_approve: true,
            },
            Access::owner(),
        );
        let ingress = f
            .authority
            .bind_scope(&ingress, ScopeBinding::new("queue-test", "epoch").unwrap())
            .unwrap();
        let envelope = Envelope::new(ConnectorId::new("test"), EventKind::new("sql.execute"), json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"closed"})).with_target(ConnectorId::new("sql"));
        let proof = f.authority.bind(&ingress, &envelope).unwrap();
        let closed = f.db.handle(&envelope.with_context(proof)).await.unwrap();
        assert_eq!(closed["status"], "approval_required");
        assert_eq!(
            f.call("sql.approval.list", json!({})).await.unwrap()["items"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        live.0.store(false, Ordering::SeqCst);
        assert_eq!(
            f.call("sql.approval.list", json!({})).await.unwrap()["items"],
            json!([])
        );
        live.0.store(true, Ordering::SeqCst);
        let mut connection = f.db.connection.lock().await;
        query("UPDATE __octo_sql_operations SET expires_at=0 WHERE id=?")
            .bind(closed["operation_id"].as_str().unwrap())
            .execute(&mut *connection)
            .await
            .unwrap();
        drop(connection);
        assert_eq!(
            f.call("sql.approval.list", json!({})).await.unwrap()["items"],
            json!([])
        );
    }
}
