//! Read-only SQL admission control.
//!
//! Every network-facing SQL surface (HTTP `/api/v1/sql`, gRPC `ExecuteSql`,
//! Flight SQL `DoGet`) must only ever run *pure read* queries. This module is
//! the single authoritative implementation of that rule.
//!
//! ## Why a shared module
//!
//! The three protocol handlers used to each carry a copy of the same
//! `matches!` check applied to `SessionContext::sql(..)`'s result. That
//! pattern is unsound for two independent reasons:
//!
//! 1. **The check ran too late.** [`SessionContext::sql`] is
//!    `sql_with_options(sql, SQLOptions::new())`, and `SQLOptions::new()`
//!    permits everything. It plans *and then executes* the plan via
//!    `execute_logical_plan`, which applies DDL and `Statement` side effects
//!    eagerly. By the time a handler inspected `df.logical_plan()` the side
//!    effect had already been applied. Because `chronixd` shares one
//!    `Arc<SessionContext>` across all requests, tenants and namespaces, a
//!    caller holding nothing but read access could run
//!    `SET datafusion.catalog.information_schema = true` and re-enable the
//!    catalog introspection that [`create_session_context`] deliberately
//!    disables — process-wide and permanently.
//!
//! 2. **The check only looked at the root node.** DDL/DML nested inside a
//!    subquery was invisible to a `matches!` on the top-level plan.
//!
//! [`plan_read_only`] fixes both: it verifies the plan *before* anything is
//! executed, and verification walks the whole tree including subqueries.
//!
//! [`create_session_context`]: super::create_session_context

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SQLOptions, SessionContext};

/// `SQLOptions` denying every plan class that can mutate catalog, data or
/// session state.
///
/// This is what makes the admission check *preventive* rather than
/// after-the-fact: `SQLOptions::verify_plan` runs against the logical plan
/// before `execute_logical_plan` is ever called.
fn deny_mutations() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// Reject plan nodes that are side-effect-free but still not something a
/// read-only endpoint should expose.
///
/// `SQLOptions` covers DDL/DML/`Statement`. `EXPLAIN`, `ANALYZE` and
/// `DESCRIBE` are harmless to run but disclose internal plan shape, catalog
/// layout and statistics, so Chronix blocks them too. These carry no side
/// effects, so checking them at plan time (rather than parse time) is safe —
/// but we still do it before execution for uniformity.
fn reject_introspection(plan: &LogicalPlan) -> DfResult<()> {
    let mut offender = None;
    plan.apply_with_subqueries(|node| {
        let name = match node {
            LogicalPlan::Explain(_) => Some("EXPLAIN"),
            LogicalPlan::Analyze(_) => Some("ANALYZE"),
            LogicalPlan::DescribeTable(_) => Some("DESCRIBE"),
            _ => None,
        };
        if let Some(name) = name {
            offender = Some(name);
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;

    match offender {
        Some(name) => Err(DataFusionError::Plan(format!(
            "{name} is not permitted on a read-only SQL endpoint"
        ))),
        None => Ok(()),
    }
}

/// Verify that an already-built [`LogicalPlan`] is a pure read.
///
/// Walks the plan *including subqueries*, rejecting DDL, DML, `COPY`,
/// `Statement` (e.g. `SET`, `PREPARE`, `BEGIN`), `EXPLAIN`, `ANALYZE` and
/// `DESCRIBE`.
///
/// # Errors
///
/// Returns [`DataFusionError::Plan`] naming the offending construct.
pub fn verify_read_only(plan: &LogicalPlan) -> DfResult<()> {
    deny_mutations().verify_plan(plan)?;
    reject_introspection(plan)
}

/// Parse `sql` into a verified read-only [`LogicalPlan`] **without executing
/// it**.
///
/// Use this when you want to cache or inspect the plan before running it.
/// The returned plan is guaranteed to have passed [`verify_read_only`].
///
/// # Errors
///
/// Returns an error if the SQL fails to parse/plan, or if the resulting plan
/// is not a pure read.
pub async fn plan_read_only(ctx: &SessionContext, sql: &str) -> DfResult<LogicalPlan> {
    // `create_logical_plan` performs no execution and applies no side
    // effects — the verification below therefore happens strictly before
    // anything can be applied to the shared session.
    let plan = ctx.state().create_logical_plan(sql).await?;
    verify_read_only(&plan)?;
    Ok(plan)
}

/// Plan, verify and execute `sql` as a read-only query.
///
/// Equivalent to [`plan_read_only`] followed by
/// [`SessionContext::execute_logical_plan`].
///
/// # Errors
///
/// Returns an error if the SQL fails to parse/plan, is not a pure read, or
/// fails during execution setup.
pub async fn sql_read_only(ctx: &SessionContext, sql: &str) -> DfResult<DataFrame> {
    let plan = plan_read_only(ctx, sql).await?;
    ctx.execute_logical_plan(plan).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Chronix;
    use crate::sql::create_session_context;
    use chronix_core::ChronixConfig;
    use std::sync::Arc;

    fn test_ctx(dir: &tempfile::TempDir) -> SessionContext {
        let config = ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .expect("config");
        let db = Arc::new(Chronix::open(config).expect("open"));
        create_session_context(db)
    }

    /// The regression that motivated this module: `SET` used to be applied to
    /// the shared session before the handler could reject it, which allowed
    /// re-enabling `information_schema` process-wide.
    #[tokio::test]
    async fn set_variable_is_rejected_without_taking_effect() {
        let dir = tempfile::tempdir().expect("tmp");
        let ctx = test_ctx(&dir);

        assert!(
            ctx.sql("SELECT * FROM information_schema.tables")
                .await
                .is_err(),
            "information_schema must be disabled to begin with"
        );

        let err = plan_read_only(&ctx, "SET datafusion.catalog.information_schema = true")
            .await
            .expect_err("SET must be rejected");
        assert!(err.to_string().contains("Statement not supported"), "{err}");

        // The decisive assertion: the session was NOT mutated.
        assert!(
            !ctx.copied_config().options().catalog.information_schema,
            "SET leaked through and mutated the shared session"
        );
        assert!(
            ctx.sql("SELECT * FROM information_schema.tables")
                .await
                .is_err(),
            "information_schema was re-enabled by a rejected statement"
        );
    }

    #[tokio::test]
    async fn external_table_is_rejected_before_touching_the_filesystem() {
        let dir = tempfile::tempdir().expect("tmp");
        let ctx = test_ctx(&dir);
        let secret = dir.path().join("secret.csv");
        std::fs::write(&secret, "user,token\nadmin,s3cr3t\n").expect("write");

        let err = plan_read_only(
            &ctx,
            &format!(
                "CREATE EXTERNAL TABLE leak STORED AS CSV LOCATION '{}'",
                secret.display()
            ),
        )
        .await
        .expect_err("CREATE EXTERNAL TABLE must be rejected");
        // A DDL rejection, not an object-store error: proves we never opened
        // the file, so there is no existence/content oracle.
        assert!(err.to_string().contains("DDL not supported"), "{err}");
    }

    #[tokio::test]
    async fn blocks_ddl_dml_and_introspection() {
        let dir = tempfile::tempdir().expect("tmp");
        let ctx = test_ctx(&dir);
        for sql in [
            "CREATE VIEW v AS SELECT 1",
            "CREATE TABLE t (x INT)",
            "CREATE SCHEMA s",
            "PREPARE p AS SELECT 1",
            "EXPLAIN SELECT 1",
            "EXPLAIN ANALYZE SELECT 1",
        ] {
            assert!(
                plan_read_only(&ctx, sql).await.is_err(),
                "expected rejection for: {sql}"
            );
        }
    }

    /// The old root-only `matches!` check could not see this.
    #[tokio::test]
    async fn blocks_mutations_nested_in_subqueries() {
        let dir = tempfile::tempdir().expect("tmp");
        let ctx = test_ctx(&dir);
        // Planning may fail outright; what must never happen is a plan that
        // passes verification while containing a nested mutation.
        if let Ok(plan) = ctx
            .state()
            .create_logical_plan("SELECT (EXPLAIN SELECT 1)")
            .await
        {
            assert!(verify_read_only(&plan).is_err());
        }
    }

    #[tokio::test]
    async fn allows_plain_selects() {
        let dir = tempfile::tempdir().expect("tmp");
        let ctx = test_ctx(&dir);
        let plan = plan_read_only(&ctx, "SELECT 1 AS x").await.expect("plan");
        assert!(ctx.execute_logical_plan(plan).await.is_ok());
        assert!(sql_read_only(&ctx, "SELECT 1 AS x").await.is_ok());
    }
}
