use std::{collections::BTreeSet, ops::ControlFlow};

use sqlparser::{
    ast::{
        AlterTableOperation, BinaryOperator, ColumnOption, Expr, ObjectName, ObjectType, Query,
        RenameTableNameKind, SqliteOnConflict, Statement, TableConstraint, TableFactor, Visit,
        Visitor,
    },
    dialect::SQLiteDialect,
    parser::Parser,
};

use super::{Analysis, EngineError, NativeLimits, Policy};

pub(super) fn analyze(
    sql: &str,
    policy: &Policy,
    native: &NativeLimits,
) -> Result<Analysis, EngineError> {
    policy.validate()?;
    if sql.len() > native.max_sql_bytes || sql.contains('\0') {
        return Err(EngineError::Limit("SQL text".into()));
    }
    let statements = Parser::new(&SQLiteDialect {})
        .with_recursion_limit(native.max_expression_depth as usize)
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_statements())
        .map_err(|e| EngineError::Unsupported(e.to_string()))?;
    if statements.len() != 1 {
        return Err(EngineError::Unsupported(
            "exactly one top-level statement required".into(),
        ));
    }
    let mut checker = Checker {
        policy,
        analysis: Analysis::default(),
        ctes: Vec::new(),
    };
    if let ControlFlow::Break(error) = statements[0].visit(&mut checker) {
        return Err(error);
    }
    Ok(checker.analysis)
}

struct Checker<'a> {
    policy: &'a Policy,
    analysis: Analysis,
    ctes: Vec<BTreeSet<String>>,
}
impl Checker<'_> {
    fn name(&mut self, name: &ObjectName) -> ControlFlow<EngineError> {
        // No qualified names: main/temp and attached schemas cannot be selected.
        if name.0.len() != 1 {
            return ControlFlow::Break(EngineError::Denied("qualified relation".into()));
        }
        let rendered = name.to_string();
        let value = rendered
            .trim_matches(['"', '`', '[', ']'])
            .to_ascii_lowercase();
        if self.ctes.iter().rev().any(|scope| scope.contains(&value)) {
            return ControlFlow::Continue(());
        }
        if !self.policy.owns(&value) {
            return ControlFlow::Break(EngineError::Denied(format!("relation {value}")));
        }
        self.analysis.objects.insert(value);
        ControlFlow::Continue(())
    }
    fn approval(&mut self, reason: &str) {
        self.analysis.requires_approval = true;
        self.analysis.reasons.insert(reason.into());
    }
}
impl Visitor for Checker<'_> {
    type Break = EngineError;
    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        let mut scope = BTreeSet::new();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                scope.insert(cte.alias.name.value.to_ascii_lowercase());
            }
        }
        self.ctes.push(scope);
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.ctes.pop();
        ControlFlow::Continue(())
    }
    fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
        self.name(name)
    }
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(f) = expr {
            let function = f.name.to_string().to_ascii_lowercase();
            if !allowed_function(&function) {
                return ControlFlow::Break(EngineError::Denied(format!("function {function}")));
            }
            self.analysis.functions.insert(function);
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Query(_) => self.analysis.reads = true,
            Statement::Insert(insert) => {
                self.analysis.writes = true;
                if insert.replace_into || insert.or == Some(SqliteOnConflict::Replace) {
                    self.approval("REPLACE can delete conflicting rows");
                }
                if insert.on.is_some() {
                    self.approval("UPSERT can overwrite existing rows");
                }
            }
            Statement::Update(update) => {
                self.analysis.writes = true;
                if update.or != Some(SqliteOnConflict::Replace)
                    && update.selection.as_ref().is_some_and(scoped_predicate)
                {
                    if let TableFactor::Table { name, .. } = &update.table.relation {
                        self.name(name)?;
                        if let Some(id) = name.0.first().and_then(|p| p.as_ident()) {
                            self.analysis.scoped_updates.insert(id.value.clone());
                        } else {
                            self.approval("UPDATE target cannot be bounded");
                        }
                    } else {
                        self.approval("UPDATE target cannot be bounded");
                    }
                } else {
                    self.approval(
                        "broad UPDATE or conflict replacement can overwrite existing values",
                    );
                }
            }
            Statement::Delete(_) => {
                self.analysis.writes = true;
                self.approval("DELETE removes rows");
            }
            Statement::CreateTable(table) => {
                self.analysis.schema_changes = true;
                if table.temporary {
                    return ControlFlow::Break(EngineError::Denied("temporary objects".into()));
                }
                for constraint in &table.constraints {
                    if let TableConstraint::ForeignKey(fk) = constraint {
                        self.name(&fk.foreign_table)?;
                    }
                }
                for column in &table.columns {
                    for option in &column.options {
                        if let ColumnOption::ForeignKey(fk) = &option.option {
                            self.name(&fk.foreign_table)?;
                        }
                    }
                }
            }
            Statement::CreateIndex(_) | Statement::CreateView(_) => {
                self.analysis.schema_changes = true
            }
            Statement::CreateTrigger(trigger) => {
                self.analysis.schema_changes = true;
                if trigger.temporary {
                    return ControlFlow::Break(EngineError::Denied("temporary triggers".into()));
                }
                self.name(&trigger.name)?;
                self.name(&trigger.table_name)?;
            }
            Statement::AlterTable(table) => {
                self.analysis.schema_changes = true;
                for operation in &table.operations {
                    match operation {
                        AlterTableOperation::AddColumn { column_def, .. } => {
                            for option in &column_def.options {
                                if let ColumnOption::ForeignKey(fk) = &option.option {
                                    self.name(&fk.foreign_table)?;
                                }
                            }
                        }
                        AlterTableOperation::RenameColumn { .. } => {}
                        AlterTableOperation::RenameTable { table_name } => {
                            let (RenameTableNameKind::To(name) | RenameTableNameKind::As(name)) =
                                table_name;
                            self.name(name)?;
                        }
                        AlterTableOperation::DropColumn { .. } => {
                            self.approval("DROP COLUMN destroys values; no backup is provided");
                            self.analysis.counted_tables.insert(
                                table
                                    .name
                                    .to_string()
                                    .trim_matches(['"', '`', '[', ']'])
                                    .to_owned(),
                            );
                        }
                        _ => {
                            return ControlFlow::Break(EngineError::Unsupported(
                                "ALTER TABLE variant".into(),
                            ));
                        }
                    }
                }
            }
            Statement::Drop {
                names, object_type, ..
            } => {
                if *object_type == ObjectType::Table {
                    for name in names {
                        self.analysis.counted_tables.insert(
                            name.0
                                .first()
                                .and_then(|p| p.as_ident())
                                .map(|id| id.value.clone())
                                .unwrap_or_default(),
                        );
                    }
                }
                self.analysis.schema_changes = true;
                self.approval("DROP destroys schema or data; no backup is provided");
                for name in names {
                    self.name(name)?;
                }
            }
            _ => return ControlFlow::Break(EngineError::Unsupported("statement kind".into())),
        }
        if self.analysis.reads && !self.policy.read
            || self.analysis.writes && !self.policy.write
            || self.analysis.schema_changes && !self.policy.schema
        {
            return ControlFlow::Break(EngineError::Denied("store capability".into()));
        }
        ControlFlow::Continue(())
    }
}

fn scoped_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) => scoped_predicate(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => scoped_predicate(left) || scoped_predicate(right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => scoped_predicate(left) && scoped_predicate(right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let column = |e: &Expr| matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
            (column(left) && matches!(right.as_ref(), Expr::Value(_)))
                || (column(right) && matches!(left.as_ref(), Expr::Value(_)))
        }
        _ => false,
    }
}

pub(super) fn allowed_function(name: &str) -> bool {
    matches!(
        name,
        "abs"
            | "avg"
            | "count"
            | "sum"
            | "total"
            | "min"
            | "max"
            | "round"
            | "coalesce"
            | "ifnull"
            | "nullif"
            | "iif"
            | "length"
            | "octet_length"
            | "lower"
            | "upper"
            | "trim"
            | "ltrim"
            | "rtrim"
            | "substr"
            | "substring"
            | "replace"
            | "instr"
            | "typeof"
            | "unicode"
            | "char"
            | "hex"
            | "unhex"
            | "quote"
            | "concat"
            | "concat_ws"
            | "like"
            | "glob"
            | "date"
            | "time"
            | "datetime"
            | "julianday"
            | "unixepoch"
            | "strftime"
            | "timediff"
            | "current_date"
            | "current_time"
            | "current_timestamp"
            | "json"
            | "json_array"
            | "json_object"
            | "json_extract"
            | "json_type"
            | "json_valid"
            | "json_quote"
            | "json_array_length"
            | "json_group_array"
            | "json_group_object"
            | "row_number"
            | "rank"
            | "dense_rank"
            | "lag"
            | "lead"
            | "first_value"
            | "last_value"
    )
}
