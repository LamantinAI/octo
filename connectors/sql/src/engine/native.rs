//! Native preparation/execution and trusted fixed SQL. SQLx's locked handle
//! excludes its worker; statements finalize before the authorization guard drops.
use std::{
    cell::{Cell, RefCell},
    ffi::{CStr, CString, c_char, c_int},
    path::Path,
    ptr::{null, null_mut},
    slice::from_raw_parts,
    time::Instant,
};

use libsqlite3_sys::{
    SQLITE_DONE, SQLITE_FLOAT, SQLITE_INTEGER, SQLITE_OK, SQLITE_ROW, SQLITE_TEXT,
    SQLITE_TRANSIENT, sqlite3, sqlite3_bind_blob, sqlite3_bind_double, sqlite3_bind_int64,
    sqlite3_bind_null, sqlite3_bind_parameter_count, sqlite3_bind_text, sqlite3_column_blob,
    sqlite3_column_bytes, sqlite3_column_count, sqlite3_column_double, sqlite3_column_int64,
    sqlite3_column_name, sqlite3_column_text, sqlite3_column_type, sqlite3_db_filename,
    sqlite3_errmsg, sqlite3_exec, sqlite3_finalize, sqlite3_get_autocommit, sqlite3_prepare_v3,
    sqlite3_step, sqlite3_stmt,
};
use sqlparser::{
    dialect::SQLiteDialect,
    keywords::Keyword,
    tokenizer::{Token, Tokenizer},
};

use super::{
    Analysis, Cancellation, EngineError, Policy, QueryResult, Settings, SqlParam,
    authorization::{Context, Guard},
};

pub(super) fn text(pointer: *const c_char) -> String {
    if pointer.is_null() {
        return String::new();
    }
    // SAFETY: SQLite callback arguments and SQLite string return values are
    // NUL-terminated and valid during the callback/locked handle lifetime.
    unsafe { CStr::from_ptr(pointer) }
        .to_string_lossy()
        .into_owned()
}
struct Statement(*mut sqlite3_stmt);
impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            sqlite3_finalize(self.0);
        }
    }
}
pub(super) fn error(db: *mut sqlite3) -> EngineError {
    EngineError::Sqlite(text(unsafe { sqlite3_errmsg(db) }))
}
fn prepare(db: *mut sqlite3, sql: &str) -> Result<Statement, EngineError> {
    let sql = CString::new(sql).map_err(|_| EngineError::Unsupported("NUL in SQL".into()))?;
    let mut statement = null_mut();
    let mut tail = null();
    let status = unsafe { sqlite3_prepare_v3(db, sql.as_ptr(), -1, 0, &mut statement, &mut tail) };
    if status != SQLITE_OK {
        return Err(error(db));
    }
    if statement.is_null() {
        return Err(EngineError::Unsupported("empty SQL".into()));
    }
    // AST enforces a single statement; SQLite additionally checks the tail.
    let statement = Statement(statement);
    if !text(tail).trim().is_empty() {
        // Comments after the final statement are accepted by AST but the native
        // tail can include them; prepare that tail to prove it has no statement.
        let mut extra = null_mut();
        let status = unsafe { sqlite3_prepare_v3(db, tail, -1, 0, &mut extra, null_mut()) };
        if !extra.is_null() {
            unsafe {
                sqlite3_finalize(extra);
            }
        }
        if status != SQLITE_OK || !extra.is_null() {
            return Err(EngineError::Unsupported("native statement tail".into()));
        }
    }
    Ok(statement)
}
fn exec_fixed(db: *mut sqlite3, sql: &'static [u8]) -> Result<(), EngineError> {
    if unsafe { sqlite3_exec(db, sql.as_ptr().cast(), None, null_mut(), null_mut()) } != SQLITE_OK {
        return Err(error(db));
    }
    Ok(())
}
fn check_file(db: *mut sqlite3, expected: &Path) -> Result<(), EngineError> {
    let filename = text(unsafe { sqlite3_db_filename(db, c"main".as_ptr()) });
    if Path::new(&filename).canonicalize()? != expected {
        return Err(EngineError::WrongDatabase);
    }
    for sql in ["PRAGMA foreign_keys", "PRAGMA recursive_triggers"] {
        let statement = prepare(db, sql)?;
        if unsafe { sqlite3_step(statement.0) } != SQLITE_ROW
            || unsafe { sqlite3_column_int64(statement.0, 0) } != 1
        {
            return Err(EngineError::Denied(
                "required SQLite safeguards disabled".into(),
            ));
        }
    }
    Ok(())
}
fn context<'a>(
    policy: &'a Policy,
    analysis: Analysis,
    settings: &Settings,
    approved: bool,
    cancellation: Cancellation,
) -> Context<'a> {
    Context {
        policy,
        counted_tables: analysis.counted_tables.clone(),
        analysis: RefCell::new(analysis),
        denied: Cell::new(false),
        changed: Cell::new(0),
        unexpected_delete: Cell::new(false),
        max_changed: settings.max_affected_rows,
        max_length: settings.max_bytes.min(i32::MAX as usize) as c_int,
        native: settings.native.clone(),
        approved,
        deadline: Instant::now() + settings.timeout,
        cancellation,
    }
}

/// Existing schema conflict policies can cause REPLACE without any REPLACE in
/// the invocation. Token classification is deliberately conservative; effect
/// authorization still comes from AST + native callbacks, never keyword safety.
fn schema_effects(
    db: *mut sqlite3,
    policy: &Policy,
    analysis: &mut Analysis,
) -> Result<(), EngineError> {
    let statement = prepare(
        db,
        "SELECT name, sql FROM sqlite_schema WHERE sql IS NOT NULL",
    )?;
    loop {
        let status = unsafe { sqlite3_step(statement.0) };
        if status == SQLITE_DONE {
            break;
        }
        if status != SQLITE_ROW {
            return Err(error(db));
        }
        let name = text(unsafe { sqlite3_column_text(statement.0, 0) }.cast());
        if !policy.owns(&name) {
            continue;
        }
        let sql = text(unsafe { sqlite3_column_text(statement.0, 1) }.cast());
        let tokens = Tokenizer::new(&SQLiteDialect {}, &sql)
            .tokenize()
            .map_err(|_| EngineError::Unsupported("stored schema tokens".into()))?;
        for token in tokens {
            if let Token::Word(word) = token {
                if word.keyword == Keyword::VIRTUAL {
                    return Err(EngineError::Unsupported("virtual table effects".into()));
                }
                if !analysis.scoped_updates.is_empty() && word.keyword == Keyword::UPDATE {
                    analysis.requires_approval = true;
                    analysis
                        .reasons
                        .insert("stored schema may propagate UPDATE effects".into());
                }
                if analysis.writes && word.keyword == Keyword::REPLACE {
                    analysis.requires_approval = true;
                    analysis
                        .reasons
                        .insert("stored schema may replace/delete existing rows".into());
                }
            }
        }
    }
    Ok(())
}

pub(super) fn analyze(
    db: *mut sqlite3,
    path: &Path,
    policy: &Policy,
    sql: &str,
    mut analysis: Analysis,
    settings: &Settings,
) -> Result<Analysis, EngineError> {
    check_file(db, path)?;
    schema_effects(db, policy, &mut analysis)?;
    let context = context(policy, analysis, settings, false, Cancellation::default());
    let _guard = Guard::install(db, &context)?;
    let result = prepare(db, sql);
    if context.denied.get() {
        return Err(EngineError::Denied("native authorizer".into()));
    }
    drop(result?);
    Ok(context.analysis.borrow().clone())
}

pub(super) fn execute(
    db: *mut sqlite3,
    path: &Path,
    policy: &Policy,
    sql: &str,
    params: &[SqlParam],
    mut analysis: Analysis,
    settings: &Settings,
    approved: bool,
    cancellation: Cancellation,
) -> Result<QueryResult, EngineError> {
    check_file(db, path)?;
    if unsafe { sqlite3_get_autocommit(db) } != 0 {
        return Err(EngineError::TransactionRequired);
    }
    schema_effects(db, policy, &mut analysis)?;
    if cancellation.is_cancelled() {
        return Err(EngineError::Cancelled);
    }
    exec_fixed(db, b"SAVEPOINT __octo_sql_engine_operation\0")?;
    let result = (|| {
        let context = context(policy, analysis, settings, approved, cancellation);
        let _guard = Guard::install(db, &context)?;
        let result = run(db, sql, params, settings, &context);
        if context.denied.get() {
            Err(EngineError::Denied("native authorizer".into()))
        } else if context.analysis.borrow().requires_approval && !approved {
            Err(EngineError::ApprovalRequired)
        } else if context.unexpected_delete.get() {
            Err(EngineError::ApprovalRequired)
        } else if context.changed.get() > settings.max_affected_rows {
            Err(EngineError::Limit("actual row effects".into()))
        } else if context.cancellation.is_cancelled() || Instant::now() >= context.deadline {
            Err(EngineError::Cancelled)
        } else {
            result
        }
    })();
    if result.is_err() {
        // SQLITE_INTERRUPT may roll back the whole transaction. Do not mistake
        // missing savepoint after automatic rollback for successful execution.
        if unsafe { sqlite3_get_autocommit(db) } == 0 {
            exec_fixed(db, b"ROLLBACK TO __octo_sql_engine_operation\0")?;
            exec_fixed(db, b"RELEASE __octo_sql_engine_operation\0")?;
        }
    } else {
        exec_fixed(db, b"RELEASE __octo_sql_engine_operation\0")?;
    }
    result
}

fn run(
    db: *mut sqlite3,
    sql: &str,
    params: &[SqlParam],
    settings: &Settings,
    context: &Context<'_>,
) -> Result<QueryResult, EngineError> {
    for table in &context.counted_tables {
        // AST has already validated the unqualified physical relation. Quote it
        // again to prevent values from becoming SQL in trusted pre-counting.
        let name = table;
        let query = format!("SELECT count(*) FROM \"{}\"", name.replace('"', "\"\""));
        let counted = prepare(db, &query)?;
        if unsafe { sqlite3_step(counted.0) } != SQLITE_ROW {
            return Err(error(db));
        }
        let rows = unsafe { sqlite3_column_int64(counted.0, 0) } as u64;
        context
            .changed
            .set(context.changed.get().saturating_add(rows));
        if context.changed.get() > settings.max_affected_rows {
            return Err(EngineError::Limit("implicit row effects".into()));
        }
    }
    let statement = prepare(db, sql)?;
    if context.analysis.borrow().requires_approval && !context.approved {
        return Err(EngineError::ApprovalRequired);
    }
    if unsafe { sqlite3_bind_parameter_count(statement.0) } as usize != params.len() {
        return Err(EngineError::Unsupported("parameter count mismatch".into()));
    }
    let parameter_bytes = params
        .iter()
        .try_fold(0usize, |total, param| {
            total.checked_add(match param {
                SqlParam::Text(v) => v.len(),
                SqlParam::Blob(v) => v.len(),
                _ => 8,
            })
        })
        .ok_or_else(|| EngineError::Limit("parameter bytes".into()))?;
    if parameter_bytes > settings.max_bytes {
        return Err(EngineError::Limit("parameter bytes".into()));
    }
    for (index, param) in params.iter().enumerate() {
        let index = (index + 1) as c_int;
        let status = unsafe {
            match param {
                SqlParam::Null => sqlite3_bind_null(statement.0, index),
                SqlParam::Integer(value) => sqlite3_bind_int64(statement.0, index, *value),
                SqlParam::Real(value) if value.is_finite() => {
                    sqlite3_bind_double(statement.0, index, *value)
                }
                SqlParam::Real(_) => {
                    return Err(EngineError::Unsupported("non-finite parameter".into()));
                }
                SqlParam::Text(value) if value.len() <= settings.max_cell_bytes => {
                    sqlite3_bind_text(
                        statement.0,
                        index,
                        value.as_ptr().cast(),
                        value.len() as c_int,
                        SQLITE_TRANSIENT(),
                    )
                }
                SqlParam::Blob(value) if value.len() <= settings.max_cell_bytes => {
                    sqlite3_bind_blob(
                        statement.0,
                        index,
                        value.as_ptr().cast(),
                        value.len() as c_int,
                        SQLITE_TRANSIENT(),
                    )
                }
                _ => return Err(EngineError::Limit("parameter cell".into())),
            }
        };
        if status != SQLITE_OK {
            return Err(error(db));
        }
    }
    let count = unsafe { sqlite3_column_count(statement.0) };
    let mut result = QueryResult {
        columns: (0..count)
            .map(|i| text(unsafe { sqlite3_column_name(statement.0, i) }))
            .collect(),
        rows: Vec::new(),
        affected_rows: 0,
        truncated: false,
    };
    let mut bytes = result.columns.iter().map(String::len).sum::<usize>();
    if bytes > settings.max_bytes {
        return Err(EngineError::Limit("result column bytes".into()));
    }
    loop {
        let status = unsafe { sqlite3_step(statement.0) };
        if status == SQLITE_DONE {
            break;
        }
        if status != SQLITE_ROW {
            return Err(error(db));
        }
        if result.rows.len() >= settings.max_rows {
            result.truncated = true;
            continue;
        }
        let mut row = Vec::new();
        for column in 0..count {
            let size = unsafe { sqlite3_column_bytes(statement.0, column) } as usize;
            if size > settings.max_cell_bytes {
                return Err(EngineError::Limit("result cell".into()));
            }
            bytes = bytes.saturating_add(size.max(8));
            if bytes > settings.max_bytes {
                return Err(EngineError::Limit("inline result bytes".into()));
            }
            let cell = unsafe {
                match sqlite3_column_type(statement.0, column) {
                    SQLITE_INTEGER => SqlParam::Integer(sqlite3_column_int64(statement.0, column)),
                    SQLITE_FLOAT => SqlParam::Real(sqlite3_column_double(statement.0, column)),
                    SQLITE_TEXT => {
                        let pointer = sqlite3_column_text(statement.0, column);
                        let slice = if size == 0 {
                            &[]
                        } else {
                            from_raw_parts(pointer, size)
                        };
                        SqlParam::Text(String::from_utf8_lossy(slice).into_owned())
                    }
                    4 => {
                        let pointer = sqlite3_column_blob(statement.0, column);
                        let slice = if size == 0 {
                            &[]
                        } else {
                            from_raw_parts(pointer.cast::<u8>(), size)
                        };
                        SqlParam::Blob(slice.to_vec())
                    }
                    _ => SqlParam::Null,
                }
            };
            row.push(cell);
        }
        result.rows.push(row);
    }
    result.affected_rows = context.changed.get();
    Ok(result)
}
