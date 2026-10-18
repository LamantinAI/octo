//! Native callbacks and their lifetime guard. Only SQLx locked handles may use this adapter.
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    ffi::{c_char, c_int, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr::null_mut,
    time::Instant,
};

use libsqlite3_sys::{
    SQLITE_ALTER_TABLE, SQLITE_CREATE_INDEX, SQLITE_CREATE_TABLE, SQLITE_CREATE_TRIGGER,
    SQLITE_CREATE_VIEW, SQLITE_DELETE, SQLITE_DENY, SQLITE_DROP_INDEX, SQLITE_DROP_TABLE,
    SQLITE_DROP_TRIGGER, SQLITE_DROP_VIEW, SQLITE_FUNCTION, SQLITE_INSERT, SQLITE_LIMIT_ATTACHED,
    SQLITE_LIMIT_COLUMN, SQLITE_LIMIT_EXPR_DEPTH, SQLITE_LIMIT_LENGTH,
    SQLITE_LIMIT_LIKE_PATTERN_LENGTH, SQLITE_LIMIT_SQL_LENGTH, SQLITE_LIMIT_TRIGGER_DEPTH,
    SQLITE_LIMIT_VARIABLE_NUMBER, SQLITE_OK, SQLITE_READ, SQLITE_RECURSIVE, SQLITE_SELECT,
    SQLITE_UPDATE, sqlite3, sqlite3_interrupt, sqlite3_limit, sqlite3_preupdate_hook,
    sqlite3_progress_handler, sqlite3_set_authorizer,
};

use super::{
    Analysis, Cancellation, EngineError, NativeLimits, Policy,
    ast::allowed_function,
    native::{error, text},
};

pub(super) struct Context<'a> {
    pub(super) policy: &'a Policy,
    pub(super) analysis: RefCell<Analysis>,
    pub(super) denied: Cell<bool>,
    pub(super) changed: Cell<u64>,
    pub(super) unexpected_delete: Cell<bool>,
    pub(super) max_changed: u64,
    pub(super) max_length: c_int,
    pub(super) native: NativeLimits,
    pub(super) counted_tables: BTreeSet<String>,
    pub(super) approved: bool,
    pub(super) deadline: Instant,
    pub(super) cancellation: Cancellation,
}

unsafe fn authorize_inner(
    data: *mut c_void,
    action: c_int,
    first: *const c_char,
    second: *const c_char,
    database: *const c_char,
    source: *const c_char,
) -> c_int {
    // Callback uses no user code, recursion, or panic-prone indexing.
    let context = unsafe { &*(data.cast::<Context<'_>>()) };
    let first = text(first);
    let second = text(second);
    let database = text(database);
    let internal = matches!(
        first.as_str(),
        "sqlite_master" | "sqlite_schema" | "sqlite_temp_master" | "sqlite_temp_schema"
    );
    let owned = context.policy.owns(&first);
    let allowed = if action != SQLITE_ALTER_TABLE
        && !database.is_empty()
        && database != "main"
        && !(database == "temp"
            && internal
            && source.is_null()
            && context.analysis.borrow().schema_changes)
    {
        false
    } else {
        match action {
            SQLITE_SELECT | SQLITE_RECURSIVE => context.policy.read,
            SQLITE_FUNCTION => {
                context
                    .analysis
                    .borrow_mut()
                    .functions
                    .insert(second.to_ascii_lowercase());
                allowed_function(&second.to_ascii_lowercase())
                    || (source.is_null()
                        && context.analysis.borrow().schema_changes
                        && matches!(
                            second.as_str(),
                            "printf"
                                | "sqlite_rename_table"
                                | "sqlite_rename_column"
                                | "sqlite_rename_test"
                                | "sqlite_drop_column"
                                | "sqlite_rename_quotefix"
                        ))
            }
            SQLITE_READ => {
                if !internal {
                    context.analysis.borrow_mut().objects.insert(first.clone());
                    context.analysis.borrow_mut().reads = true;
                }
                (owned && context.policy.read)
                    || (internal && source.is_null() && context.analysis.borrow().schema_changes)
            }
            SQLITE_INSERT | SQLITE_UPDATE | SQLITE_DELETE => {
                if internal {
                    context.analysis.borrow().schema_changes && context.policy.schema
                } else {
                    context.analysis.borrow_mut().objects.insert(first.clone());
                    context.analysis.borrow_mut().writes = true;
                    if action == SQLITE_DELETE {
                        context.analysis.borrow_mut().requires_approval = true;
                        context
                            .analysis
                            .borrow_mut()
                            .reasons
                            .insert("native DELETE effect, including triggers/cascades".into());
                    }
                    if action == SQLITE_UPDATE
                        && !(source.is_null()
                            && context
                                .analysis
                                .borrow()
                                .scoped_updates
                                .iter()
                                .any(|table| table.eq_ignore_ascii_case(&first)))
                    {
                        context.analysis.borrow_mut().requires_approval = true;
                        context
                            .analysis
                            .borrow_mut()
                            .reasons
                            .insert("native UPDATE effect".into());
                    }
                    owned && context.policy.write
                }
            }
            SQLITE_CREATE_TABLE
            | SQLITE_CREATE_VIEW
            | SQLITE_CREATE_TRIGGER
            | SQLITE_DROP_TABLE
            | SQLITE_DROP_VIEW
            | SQLITE_DROP_TRIGGER => owned && context.policy.schema,
            SQLITE_CREATE_INDEX | SQLITE_DROP_INDEX => {
                (owned || first.starts_with("sqlite_autoindex_"))
                    && context.policy.owns(&second)
                    && context.policy.schema
            }
            SQLITE_ALTER_TABLE => {
                first == "main" && context.policy.owns(&second) && context.policy.schema
            }
            _ => false,
        }
    };
    if allowed {
        SQLITE_OK
    } else {
        context.denied.set(true);
        SQLITE_DENY
    }
}
unsafe extern "C" fn authorize(
    data: *mut c_void,
    action: c_int,
    first: *const c_char,
    second: *const c_char,
    database: *const c_char,
    source: *const c_char,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        authorize_inner(data, action, first, second, database, source)
    }))
    .unwrap_or(SQLITE_DENY)
}
unsafe extern "C" fn progress(data: *mut c_void) -> c_int {
    let context = unsafe { &*(data.cast::<Context<'_>>()) };
    i32::from(
        context.cancellation.is_cancelled()
            || Instant::now() >= context.deadline
            || context.changed.get() > context.max_changed
            || context.unexpected_delete.get(),
    )
}
unsafe extern "C" fn preupdate(
    data: *mut c_void,
    db: *mut sqlite3,
    action: c_int,
    _database: *const c_char,
    table: *const c_char,
    _old: i64,
    _new: i64,
) {
    let context = unsafe { &*(data.cast::<Context<'_>>()) };
    let table = text(table);
    if !context.policy.owns(&table) {
        context.denied.set(true);
        unsafe {
            sqlite3_interrupt(db);
        }
        return;
    }
    if !context
        .counted_tables
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&table))
    {
        context.changed.set(context.changed.get().saturating_add(1));
    }
    if action == SQLITE_DELETE && !context.approved {
        context.unexpected_delete.set(true);
    }
    if context.changed.get() > context.max_changed || context.unexpected_delete.get() {
        unsafe {
            sqlite3_interrupt(db);
        }
    }
}

pub(super) struct Guard {
    db: *mut sqlite3,
    limits: [(c_int, c_int); 8],
}
impl Guard {
    pub(super) fn install(db: *mut sqlite3, context: &Context<'_>) -> Result<Self, EngineError> {
        let data = (context as *const Context<'_>).cast_mut().cast();
        // SAFETY: the Context is stationary on our stack until Guard drops;
        // no async suspension or SQLx worker access occurs within this scope.
        unsafe {
            if sqlite3_set_authorizer(db, Some(authorize), data) != SQLITE_OK {
                return Err(error(db));
            }
            sqlite3_progress_handler(db, 100, Some(progress), data);
            sqlite3_preupdate_hook(db, Some(preupdate), data);
        }
        let limits = [
            (SQLITE_LIMIT_LENGTH, context.max_length),
            (
                SQLITE_LIMIT_SQL_LENGTH,
                context.native.max_sql_bytes as c_int,
            ),
            (SQLITE_LIMIT_COLUMN, context.native.max_columns),
            (SQLITE_LIMIT_EXPR_DEPTH, context.native.max_expression_depth),
            (SQLITE_LIMIT_TRIGGER_DEPTH, context.native.max_trigger_depth),
            (SQLITE_LIMIT_ATTACHED, 0),
            (SQLITE_LIMIT_VARIABLE_NUMBER, context.native.max_parameters),
            (
                SQLITE_LIMIT_LIKE_PATTERN_LENGTH,
                context.native.max_like_pattern_bytes,
            ),
        ]
        .map(|(id, desired)| {
            let old = unsafe { sqlite3_limit(db, id, -1) };
            unsafe {
                sqlite3_limit(db, id, old.min(desired));
            }
            (id, old)
        });
        Ok(Self { db, limits })
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        unsafe {
            for (id, value) in self.limits {
                sqlite3_limit(self.db, id, value);
            }
            sqlite3_set_authorizer(self.db, None, null_mut());
            sqlite3_progress_handler(self.db, 0, None, null_mut());
            sqlite3_preupdate_hook(self.db, None, null_mut());
        }
    }
}
