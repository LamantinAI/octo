//! SQLite safety foundation. The host retains transaction and approval authority.
mod ast;
mod authorization;
mod driver;
mod native;

pub use driver::Engine;

mod types;
pub use types::{
    Analysis, Cancellation, EngineError, ExecutionPermit, NativeLimits, Policy, QueryResult,
    Settings, SqlParam,
};

#[cfg(test)]
mod tests {
    use std::{
        fs::rename,
        thread::{sleep, spawn},
        time::{Duration, Instant},
    };

    use sqlx::{Connection, SqliteConnection, query};
    use tempfile::{TempDir, tempdir};

    use super::*;

    fn policy() -> Policy {
        Policy {
            namespace: "s_test__".into(),
            read: true,
            write: true,
            schema: true,
        }
    }
    async fn setup(settings: Settings) -> (TempDir, Engine, SqliteConnection) {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path().join("connector.db"), settings)
            .await
            .unwrap();
        let conn = engine.connect().await.unwrap();
        (dir, engine, conn)
    }
    async fn execute(
        engine: &Engine,
        conn: &mut SqliteConnection,
        sql: &str,
        approved: bool,
    ) -> Result<QueryResult, EngineError> {
        let mut transaction = conn.begin().await?;
        let permit = if approved {
            ExecutionPermit::approved()
        } else {
            ExecutionPermit::ordinary()
        };
        let result = engine
            .execute(
                &mut transaction,
                &policy(),
                sql,
                &[],
                permit,
                Cancellation::default(),
            )
            .await;
        if result.is_ok() {
            transaction.commit().await?;
        } else {
            let _ = transaction.rollback().await;
        }
        result
    }

    #[tokio::test]
    async fn engine_safe_schema_crud_typed_params() {
        let (_dir, engine, mut conn) = setup(Settings::default()).await;
        execute(
            &engine,
            &mut conn,
            "CREATE TABLE s_test__items(id INTEGER PRIMARY KEY, value TEXT)",
            false,
        )
        .await
        .unwrap();
        let mut transaction = conn.begin().await.unwrap();
        let result = engine
            .execute(
                &mut transaction,
                &policy(),
                "INSERT INTO s_test__items VALUES (?, ?)",
                &[SqlParam::Integer(1), SqlParam::Text("a\0b".into())],
                ExecutionPermit::ordinary(),
                Cancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.affected_rows, 1);
        transaction.commit().await.unwrap();
        let result = execute(
            &engine,
            &mut conn,
            "SELECT id,value FROM s_test__items",
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlParam::Integer(1), SqlParam::Text("a\0b".into())]]
        );
        assert!(matches!(
            execute(
                &engine,
                &mut conn,
                "DELETE FROM s_test__items WHERE id=1",
                false
            )
            .await,
            Err(EngineError::ApprovalRequired)
        ));
        execute(
            &engine,
            &mut conn,
            "DELETE FROM s_test__items WHERE id=1",
            true,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn engine_rejects_control_protected_and_stacked_sql() {
        let (_dir, engine, mut conn) = setup(Settings::default()).await;
        for sql in [
            "SELECT * FROM __octo_sql_catalog",
            "SELECT * FROM s_other__items",
            "SELECT * FROM sqlite_schema",
            "ATTACH DATABASE '/tmp/other.db' AS other",
            "DETACH DATABASE main",
            "PRAGMA writable_schema=ON",
            "BEGIN",
            "COMMIT",
            "SAVEPOINT model",
            "VACUUM",
            "SELECT load_extension('x')",
            "SELECT randomblob(1000000)",
            "SELECT 1; SELECT 2",
            "CREATE TABLE s_test__bad(id REFERENCES __octo_sql_catalog(id))",
            "CREATE TABLE s_test__bad(id REFERENCES s_other__data(id))",
            "CREATE VIEW s_test__bad AS SELECT * FROM sqlite_schema",
        ] {
            assert!(
                engine.analyze(&mut conn, &policy(), sql).await.is_err(),
                "accepted {sql}"
            );
        }
        query("CREATE TABLE __octo_sql_catalog(secret TEXT); INSERT INTO __octo_sql_catalog VALUES ('secret')").execute(&mut conn).await.unwrap();
        query("CREATE VIEW s_test__evil AS SELECT * FROM __octo_sql_catalog")
            .execute(&mut conn)
            .await
            .unwrap();
        assert!(
            engine
                .analyze(&mut conn, &policy(), "SELECT * FROM s_test__evil")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn engine_trigger_body_and_indirect_delete_approval() {
        let (_dir, engine, mut conn) = setup(Settings::default()).await;
        execute(
            &engine,
            &mut conn,
            "CREATE TABLE s_test__items(id INTEGER PRIMARY KEY)",
            false,
        )
        .await
        .unwrap();
        execute(
            &engine,
            &mut conn,
            "CREATE TABLE s_test__log(id INTEGER)",
            false,
        )
        .await
        .unwrap();
        let trigger = "CREATE TRIGGER s_test__clean AFTER INSERT ON s_test__items BEGIN INSERT INTO s_test__log VALUES (new.id); DELETE FROM s_test__log WHERE id < new.id; END";
        execute(&engine, &mut conn, trigger, true).await.unwrap();
        let analysis = engine
            .analyze(&mut conn, &policy(), "INSERT INTO s_test__items VALUES (2)")
            .await
            .unwrap();
        assert!(analysis.requires_approval);
        assert!(analysis.objects.contains("s_test__log"));
        assert!(matches!(
            execute(
                &engine,
                &mut conn,
                "INSERT INTO s_test__items VALUES (2)",
                false
            )
            .await,
            Err(EngineError::ApprovalRequired)
        ));
        assert_eq!(
            execute(
                &engine,
                &mut conn,
                "INSERT INTO s_test__items VALUES (2)",
                true
            )
            .await
            .unwrap()
            .affected_rows,
            2
        );
        assert!(engine.analyze(&mut conn, &policy(), "CREATE TRIGGER s_test__evil AFTER INSERT ON s_test__items BEGIN DELETE FROM __octo_sql_catalog; END").await.is_err());
    }

    #[tokio::test]
    async fn engine_replace_and_fk_cascade_count_all_effects() {
        let (_dir, engine, mut conn) = setup(Settings::default()).await;
        execute(
            &engine,
            &mut conn,
            "CREATE TABLE s_test__parent(id INTEGER PRIMARY KEY)",
            false,
        )
        .await
        .unwrap();
        execute(&engine, &mut conn, "CREATE TABLE s_test__child(id INTEGER REFERENCES s_test__parent(id) ON DELETE CASCADE)", false).await.unwrap();
        execute(
            &engine,
            &mut conn,
            "INSERT INTO s_test__parent VALUES (1)",
            false,
        )
        .await
        .unwrap();
        execute(
            &engine,
            &mut conn,
            "INSERT INTO s_test__child VALUES (1)",
            false,
        )
        .await
        .unwrap();
        let result = execute(
            &engine,
            &mut conn,
            "INSERT OR REPLACE INTO s_test__parent VALUES (1)",
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            result.affected_rows, 3,
            "parent DELETE + child cascade DELETE + parent INSERT"
        );
        let analysis = engine
            .analyze(
                &mut conn,
                &policy(),
                "INSERT OR REPLACE INTO s_test__parent VALUES (1)",
            )
            .await
            .unwrap();
        assert!(analysis.requires_approval);
    }

    #[tokio::test]
    async fn engine_limits_rollback_complete_statement() {
        let (_dir, engine, mut conn) = setup(Settings {
            max_affected_rows: 2,
            ..Settings::default()
        })
        .await;
        execute(
            &engine,
            &mut conn,
            "CREATE TABLE s_test__items(id INTEGER)",
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            execute(
                &engine,
                &mut conn,
                "INSERT INTO s_test__items VALUES (1),(2),(3)",
                false
            )
            .await,
            Err(EngineError::Limit(_))
        ));
        let result = execute(
            &engine,
            &mut conn,
            "SELECT count(*) FROM s_test__items",
            false,
        )
        .await
        .unwrap();
        assert_eq!(result.rows[0][0], SqlParam::Integer(0));
    }

    #[tokio::test]
    async fn engine_cancellation_and_deadline_interrupt_native_work() {
        let (_dir, engine, mut conn) = setup(Settings {
            timeout: Duration::from_millis(5),
            ..Settings::default()
        })
        .await;
        let cancellation = Cancellation::default();
        cancellation.cancel();
        let mut transaction = conn.begin().await.unwrap();
        assert!(matches!(
            engine
                .execute(
                    &mut transaction,
                    &policy(),
                    "SELECT 1",
                    &[],
                    ExecutionPermit::ordinary(),
                    cancellation
                )
                .await,
            Err(EngineError::Cancelled)
        ));
        transaction.rollback().await.unwrap();
        let query = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1000000000) SELECT sum(x) FROM c";
        assert!(matches!(
            execute(&engine, &mut conn, query, false).await,
            Err(EngineError::Cancelled)
        ));
        assert!(execute(&engine, &mut conn, "SELECT 1", false).await.is_ok());
    }

    #[tokio::test]
    async fn engine_callbacks_do_not_leak_across_policies_or_errors() {
        let (_dir, engine, mut conn) = setup(Settings::default()).await;
        execute(
            &engine,
            &mut conn,
            "CREATE TABLE s_test__items(id INTEGER)",
            false,
        )
        .await
        .unwrap();
        engine
            .analyze(&mut conn, &policy(), "SELECT * FROM s_test__items")
            .await
            .unwrap();
        let denied = Policy {
            namespace: "s_other__".into(),
            ..policy()
        };
        assert!(
            engine
                .analyze(&mut conn, &denied, "SELECT * FROM s_test__items")
                .await
                .is_err()
        );
        assert!(
            engine
                .analyze(&mut conn, &policy(), "SELECT * FROM s_test__absent")
                .await
                .is_err()
        );
        // Trusted fixed SQL remains usable after native prepare errors.
        query("CREATE TABLE __octo_sql_audit(id INTEGER)")
            .execute(&mut conn)
            .await
            .unwrap();
        assert!(
            engine
                .analyze(&mut conn, &policy(), "SELECT * FROM s_test__items")
                .await
                .is_ok()
        );
        assert!(matches!(
            engine
                .execute(
                    &mut conn,
                    &policy(),
                    "SELECT 1",
                    &[],
                    ExecutionPermit::ordinary(),
                    Cancellation::default()
                )
                .await,
            Err(EngineError::TransactionRequired)
        ));
    }

    #[tokio::test]
    async fn engine_result_bounds_and_wrong_database() {
        let (_dir, engine, mut conn) = setup(Settings {
            max_rows: 1,
            max_cell_bytes: 8,
            ..Settings::default()
        })
        .await;
        let result = execute(&engine, &mut conn, "SELECT 1 UNION ALL SELECT 2", false)
            .await
            .unwrap();
        assert!(result.truncated);
        assert_eq!(result.rows.len(), 1);
        assert!(matches!(
            execute(&engine, &mut conn, "SELECT '123456789'", false).await,
            Err(EngineError::Limit(_))
        ));
        let (_other_dir, _other_engine, mut other) = setup(Settings::default()).await;
        assert!(matches!(
            engine.analyze(&mut other, &policy(), "SELECT 1").await,
            Err(EngineError::WrongDatabase)
        ));
    }
    #[tokio::test]
    async fn engine_drop_table_precounters_enforce_effect_limit() {
        let (_dir, engine, mut conn) = setup(Settings {
            max_affected_rows: 2,
            ..Settings::default()
        })
        .await;
        execute(
            &engine,
            &mut conn,
            "CREATE TABLE s_test__items(id INTEGER)",
            false,
        )
        .await
        .unwrap();
        for id in 0..3 {
            execute(
                &engine,
                &mut conn,
                &format!("INSERT INTO s_test__items VALUES ({id})"),
                false,
            )
            .await
            .unwrap();
        }
        let result = execute(&engine, &mut conn, "DROP TABLE s_test__items", true).await;
        assert!(matches!(result, Err(EngineError::Limit(_))), "{result:?}");
    }

    #[tokio::test]
    async fn engine_cross_thread_cancellation_and_file_replacement() {
        let (dir, engine, mut conn) = setup(Settings::default()).await;
        let cancellation = Cancellation::default();
        let signal = cancellation.clone();
        let worker = spawn(move || {
            sleep(Duration::from_millis(10));
            signal.cancel();
        });
        let mut transaction = conn.begin().await.unwrap();
        let started = Instant::now();
        let query = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1000000000) SELECT sum(x) FROM c";
        let result = engine
            .execute(
                &mut transaction,
                &policy(),
                query,
                &[],
                ExecutionPermit::ordinary(),
                cancellation,
            )
            .await;
        assert!(matches!(result, Err(EngineError::Cancelled)));
        assert!(started.elapsed() < Duration::from_secs(1));
        let _ = transaction.rollback().await;
        worker.join().unwrap();
        let replacement = dir.path().join("replacement.db");
        let _replacement_engine = Engine::open(&replacement, Settings::default())
            .await
            .unwrap();
        rename(replacement, engine.path()).unwrap();
        assert!(matches!(
            engine.connect().await,
            Err(EngineError::WrongDatabase)
        ));
    }
    #[tokio::test]
    async fn engine_native_alter_and_drop_table_effects() {
        let (_dir, engine, mut conn) = setup(Settings::default()).await;
        execute(
            &engine,
            &mut conn,
            "CREATE TABLE s_test__data(id INTEGER, value TEXT)",
            false,
        )
        .await
        .unwrap();
        execute(
            &engine,
            &mut conn,
            "INSERT INTO s_test__data VALUES(1,'x')",
            false,
        )
        .await
        .unwrap();
        for sql in [
            "ALTER TABLE s_test__data ADD COLUMN extra REAL",
            "ALTER TABLE s_test__data RENAME COLUMN value TO label",
            "ALTER TABLE s_test__data RENAME TO s_test__renamed",
        ] {
            execute(&engine, &mut conn, sql, false).await.unwrap();
        }
        assert!(
            engine
                .analyze(
                    &mut conn,
                    &policy(),
                    "ALTER TABLE s_test__renamed RENAME TO __octo_sql_bad"
                )
                .await
                .is_err()
        );
        let dropped = execute(
            &engine,
            &mut conn,
            "ALTER TABLE s_test__renamed DROP COLUMN label",
            true,
        )
        .await
        .unwrap();
        assert_eq!(dropped.affected_rows, 1);
        let dropped = execute(&engine, &mut conn, "DROP TABLE s_test__renamed", true)
            .await
            .unwrap();
        assert_eq!(dropped.affected_rows, 1);
    }
}
