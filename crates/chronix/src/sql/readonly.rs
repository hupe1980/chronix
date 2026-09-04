//! Read-only SQL admission control.
//!
//! Every network-facing SQL surface — HTTP `/api/v1/chronix/sql`, gRPC
//! `ExecuteSql`, Flight SQL `DoGet` — runs only pure reads, and this module
//! is the single implementation of that rule.
//!
//! Two properties make it sound:
//!
//! 1. **Verification precedes execution.** [`SessionContext::sql`] plans
//!    *and then executes*, applying DDL and `Statement` side effects
//!    eagerly, so inspecting the returned plan is too late — and `chronixd`
//!    shares one `SessionContext` across every request, tenant and
//!    namespace, so a `SET` that slipped through would change execution
//!    settings for all of them. Admission runs against
//!    `create_logical_plan()`, which applies nothing.
//! 2. **The whole tree is checked**, subqueries included, so a mutation
//!    nested inside one cannot pass a check that only saw the root.

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

/// The plan `EXPLAIN`, `EXPLAIN ANALYZE` or `DESCRIBE` wraps, if any.
///
/// These three used to be refused outright as "information disclosure",
/// which cost the user the only way to find out whether a time filter
/// pushed down — the single most important operational question about a
/// query against a time-series database — and bought nothing. The scan
/// node's display is `measurement`, the time range, a filter count and a
/// limit: the caller's own query, echoed back. There are no file paths, no
/// catalog layout and no cross-tenant statistics in it, and under
/// multi-tenancy the table provider the plan names is already scoped to the
/// caller's namespace.
///
/// So they are permitted, and what they wrap is verified exactly as a
/// bare query would be. `EXPLAIN ANALYZE` executes, which is why the inner
/// plan has to pass the same check rather than a weaker one: without it,
/// `EXPLAIN ANALYZE INSERT …` would be a write with a fig leaf.
fn wrapped_plan(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Explain(explain) => Some(&explain.plan),
        LogicalPlan::Analyze(analyze) => Some(&analyze.input),
        _ => None,
    }
}

/// Reject introspection nested anywhere but at the root.
///
/// A top-level `EXPLAIN` is a question about a query. One buried inside a
/// subquery is not something SQL can express meaningfully, and permitting it
/// would mean `verify_read_only` had to reason about a plan shape nothing
/// produces — so it stays refused.
fn reject_nested_introspection(plan: &LogicalPlan) -> DfResult<()> {
    let mut offender = None;
    plan.apply_with_subqueries(|node| {
        // The root itself is the permitted case; only look below it.
        if std::ptr::eq(node, plan) {
            return Ok(TreeNodeRecursion::Continue);
        }
        let name = match node {
            LogicalPlan::Explain(_) => Some("EXPLAIN"),
            LogicalPlan::Analyze(_) => Some("ANALYZE"),
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
            "a nested {name} is not permitted on a read-only SQL endpoint"
        ))),
        None => Ok(()),
    }
}

/// Verify that an already-built [`LogicalPlan`] is a pure read.
///
/// Walks the plan *including subqueries*, rejecting DDL, DML, `COPY` and
/// `Statement` (e.g. `SET`, `PREPARE`, `BEGIN`).
///
/// A top-level `EXPLAIN`, `EXPLAIN ANALYZE` or `DESCRIBE` is permitted, and
/// what it wraps is verified by the same rules — so `EXPLAIN SELECT` is
/// allowed and `EXPLAIN ANALYZE INSERT` is not.
///
/// # Errors
///
/// Returns [`DataFusionError::Plan`] naming the offending construct.
pub fn verify_read_only(plan: &LogicalPlan) -> DfResult<()> {
    // `DescribeTable` names a table and produces its column list — strictly
    // less than the `SELECT *` the caller could already run.
    if matches!(plan, LogicalPlan::DescribeTable(_)) {
        return Ok(());
    }

    // Verify what an EXPLAIN wraps, not the wrapper: `verify_plan` looks at
    // the node it is given, and an `Explain` around an `Insert` is not a
    // DML node.
    if let Some(inner) = wrapped_plan(plan) {
        deny_mutations().verify_plan(inner)?;
        return reject_nested_introspection(inner);
    }

    deny_mutations().verify_plan(plan)?;
    reject_nested_introspection(plan)
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

    /// The regression that motivated this module: `SET` used to be applied
    /// to the shared session before the handler could reject it. Because
    /// `chronixd` shares one context across every request, tenant and
    /// namespace, that made a read-only caller able to change execution
    /// settings for everyone, permanently.
    ///
    /// The observable is a setting a `SET` would change, checked before and
    /// after. It used to be `information_schema`, which is now on by
    /// default — the catalog is namespace-scoped, so the views enumerate the
    /// caller's own data — so the proof moved to a setting still at its
    /// default.
    #[tokio::test]
    async fn set_variable_is_rejected_without_taking_effect() {
        let dir = tempfile::tempdir().expect("tmp");
        let ctx = test_ctx(&dir);

        let before = ctx.copied_config().options().execution.target_partitions;
        let wanted = before + 1;

        let err = plan_read_only(
            &ctx,
            &format!("SET datafusion.execution.target_partitions = {wanted}"),
        )
        .await
        .expect_err("SET must be rejected");
        assert!(err.to_string().contains("Statement not supported"), "{err}");

        // The decisive assertion: the shared session was NOT mutated.
        assert_eq!(
            ctx.copied_config().options().execution.target_partitions,
            before,
            "SET leaked through and mutated the shared session"
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
    async fn blocks_ddl_dml_and_statements() {
        let dir = tempfile::tempdir().expect("tmp");
        let ctx = test_ctx(&dir);
        for sql in [
            "CREATE VIEW v AS SELECT 1",
            "CREATE TABLE t (x INT)",
            "CREATE SCHEMA s",
            "PREPARE p AS SELECT 1",
        ] {
            assert!(
                plan_read_only(&ctx, sql).await.is_err(),
                "expected rejection for: {sql}"
            );
        }
    }

    /// `EXPLAIN` was refused as information disclosure, which cost the user
    /// the only way to see whether a time filter pushed down and disclosed
    /// nothing: the scan's display is the caller's own query echoed back.
    #[tokio::test]
    async fn permits_explaining_a_read() {
        let dir = tempfile::tempdir().expect("tmp");
        let ctx = test_ctx(&dir);
        for sql in ["EXPLAIN SELECT 1", "EXPLAIN ANALYZE SELECT 1"] {
            assert!(
                plan_read_only(&ctx, sql).await.is_ok(),
                "expected acceptance for: {sql}"
            );
        }
    }

    /// And it is not a way past the admission check. `EXPLAIN ANALYZE`
    /// executes, so what it wraps has to pass the same rules — verifying the
    /// wrapper instead would let `EXPLAIN ANALYZE INSERT` through, because
    /// an `Explain` node is not a DML node.
    #[tokio::test]
    async fn explain_does_not_wrap_a_mutation_past_the_check() {
        let dir = tempfile::tempdir().expect("tmp");
        let ctx = test_ctx(&dir);
        for sql in [
            "EXPLAIN CREATE VIEW v AS SELECT 1",
            "EXPLAIN SET datafusion.catalog.information_schema = true",
            "EXPLAIN ANALYZE SET datafusion.catalog.information_schema = true",
        ] {
            assert!(
                plan_read_only(&ctx, sql).await.is_err(),
                "wrapping in EXPLAIN must not permit: {sql}"
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
