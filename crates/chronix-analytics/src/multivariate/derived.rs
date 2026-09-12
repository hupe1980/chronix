//! Derived series engine with DAG-based dependency resolution.

use std::collections::HashMap;

use crate::multivariate::context::MultiSeriesContext;
use crate::multivariate::error::MultivariateError;

/// An expression that produces a derived series from a context.
pub trait DerivedSeriesExpr: Send + Sync {
    /// Evaluate the expression against the given context.
    fn evaluate(&self, ctx: &MultiSeriesContext) -> Result<Vec<f64>, MultivariateError>;
}

/// Definition of a derived series.
pub struct DerivedSeriesDefinition {
    /// Name of the derived series.
    pub name: String,
    /// Expression that computes the derived values.
    pub expression: Box<dyn DerivedSeriesExpr>,
    /// Names of the source series this definition depends on.
    pub dependencies: Vec<String>,
}

// ─── Built-in expressions ───────────────────────────────────────────

/// Arithmetic on two named series.
pub struct ArithmeticExpr {
    /// Name of the left operand series.
    pub left: String,
    /// Name of the right operand series.
    pub right: String,
    /// Binary arithmetic operator.
    pub op: ArithOp,
}

/// Binary arithmetic operators for derived series.
#[derive(Debug, Clone, Copy)]
pub enum ArithOp {
    /// Element-wise addition.
    Add,
    /// Element-wise subtraction.
    Sub,
    /// Element-wise multiplication.
    Mul,
    /// Element-wise division (safe: divides by zero yield 0.0).
    Div,
}

impl DerivedSeriesExpr for ArithmeticExpr {
    fn evaluate(&self, ctx: &MultiSeriesContext) -> Result<Vec<f64>, MultivariateError> {
        let l = ctx
            .matrix
            .series_by_name(&self.left)
            .ok_or_else(|| MultivariateError::SeriesNotFound(self.left.clone()))?;
        let r = ctx
            .matrix
            .series_by_name(&self.right)
            .ok_or_else(|| MultivariateError::SeriesNotFound(self.right.clone()))?;
        if l.len() != r.len() {
            return Err(MultivariateError::DimensionMismatch {
                expected: l.len(),
                got: r.len(),
            });
        }
        let result = match self.op {
            ArithOp::Add => l.iter().zip(r.iter()).map(|(a, b)| a + b).collect(),
            ArithOp::Sub => l.iter().zip(r.iter()).map(|(a, b)| a - b).collect(),
            ArithOp::Mul => l.iter().zip(r.iter()).map(|(a, b)| a * b).collect(),
            ArithOp::Div => l
                .iter()
                .zip(r.iter())
                .map(|(a, b)| if b.abs() > 1e-15 { a / b } else { 0.0 })
                .collect(),
        };
        Ok(result)
    }
}

/// Log returns: ln(p_t / p_{t-1}).
pub struct LogReturnExpr {
    /// Name of the price / value series.
    pub series_name: String,
}

impl DerivedSeriesExpr for LogReturnExpr {
    fn evaluate(&self, ctx: &MultiSeriesContext) -> Result<Vec<f64>, MultivariateError> {
        let s = ctx
            .matrix
            .series_by_name(&self.series_name)
            .ok_or_else(|| MultivariateError::SeriesNotFound(self.series_name.clone()))?;
        let mut result = Vec::with_capacity(s.len());
        result.push(0.0); // first point has no return
        for w in s.windows(2) {
            if w[0].abs() > 1e-15 {
                let ratio = w[1] / w[0];
                if ratio > 0.0 {
                    result.push(ratio.ln());
                } else {
                    result.push(f64::NAN);
                }
            } else {
                result.push(0.0);
            }
        }
        Ok(result)
    }
}

/// Rolling statistic expression.
pub struct RollingStatExpr {
    /// Name of the source series.
    pub series_name: String,
    /// Window size (number of observations).
    pub window: usize,
    /// Which rolling statistic to compute.
    pub stat: RollingStat,
}

/// Supported rolling statistics.
#[derive(Debug, Clone, Copy)]
pub enum RollingStat {
    /// Rolling arithmetic mean.
    Mean,
    /// Rolling standard deviation.
    Std,
    /// Rolling minimum.
    Min,
    /// Rolling maximum.
    Max,
}

impl DerivedSeriesExpr for RollingStatExpr {
    fn evaluate(&self, ctx: &MultiSeriesContext) -> Result<Vec<f64>, MultivariateError> {
        let s = ctx
            .matrix
            .series_by_name(&self.series_name)
            .ok_or_else(|| MultivariateError::SeriesNotFound(self.series_name.clone()))?;
        let n = s.len();
        let mut result = Vec::with_capacity(n);
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        // Monotone deques for O(n) amortised rolling min/max
        let mut min_deque: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
        let mut max_deque: std::collections::VecDeque<usize> = std::collections::VecDeque::new();

        for i in 0..n {
            sum += s[i];
            sum_sq += s[i] * s[i];
            if i >= self.window {
                sum -= s[i - self.window];
                sum_sq -= s[i - self.window] * s[i - self.window];
            }

            // Periodic FP drift recomputation every 1024 steps.
            if i > 0 && i % 1024 == 0 {
                let start = i.saturating_sub(self.window.saturating_sub(1));
                sum = s[start..=i].iter().sum();
                sum_sq = s[start..=i].iter().map(|&v| v * v).sum();
            }

            let count = (i + 1).min(self.window) as f64;
            match self.stat {
                RollingStat::Mean => {
                    result.push(sum / count);
                }
                RollingStat::Std => {
                    if count < 2.0 {
                        result.push(0.0);
                    } else {
                        // Bessel's correction: sample std = sqrt(Var/(n-1))
                        let var = ((sum_sq - sum * sum / count) / (count - 1.0)).max(0.0);
                        result.push(var.sqrt());
                    }
                }
                RollingStat::Min => {
                    // Remove elements that fell out of the window
                    while min_deque.front().is_some_and(|&f| f + self.window <= i) {
                        min_deque.pop_front();
                    }
                    // Maintain increasing monotone deque
                    while min_deque.back().is_some_and(|&b| s[b] >= s[i]) {
                        min_deque.pop_back();
                    }
                    min_deque.push_back(i);
                    result.push(s[min_deque[0]]);
                }
                RollingStat::Max => {
                    // Remove elements that fell out of the window
                    while max_deque.front().is_some_and(|&f| f + self.window <= i) {
                        max_deque.pop_front();
                    }
                    // Maintain decreasing monotone deque
                    while max_deque.back().is_some_and(|&b| s[b] <= s[i]) {
                        max_deque.pop_back();
                    }
                    max_deque.push_back(i);
                    result.push(s[max_deque[0]]);
                }
            }
        }
        Ok(result)
    }
}

// ─── Lazy Derived Series ────────────────────────────────────────────

/// Lazy wrapper around a derived series definition that caches its result.
///
/// The expression is only evaluated when `get()` is called for the first time
/// (or after `invalidate()`), and the result is cached for subsequent reads.
pub struct LazyDerivedSeries {
    definition: DerivedSeriesDefinition,
    cache: parking_lot::Mutex<Option<Vec<f64>>>,
}

impl LazyDerivedSeries {
    /// Creates a new lazy-evaluated derived series.
    pub fn new(definition: DerivedSeriesDefinition) -> Self {
        Self {
            definition,
            cache: parking_lot::Mutex::new(None),
        }
    }

    /// Returns the cached result, evaluating the expression if needed.
    pub fn get(&self, ctx: &MultiSeriesContext) -> Result<Vec<f64>, MultivariateError> {
        let mut cache = self.cache.lock();
        if let Some(ref cached) = *cache {
            return Ok(cached.clone());
        }
        let result = self.definition.expression.evaluate(ctx)?;
        *cache = Some(result.clone());
        Ok(result)
    }

    /// Invalidates the cached result, forcing re-evaluation on next `get()`.
    pub fn invalidate(&self) {
        let mut cache = self.cache.lock();
        *cache = None;
    }

    /// Returns true if the result is currently cached.
    pub fn is_cached(&self) -> bool {
        self.cache.lock().is_some()
    }

    /// Returns the underlying definition.
    pub fn definition(&self) -> &DerivedSeriesDefinition {
        &self.definition
    }
}

// ─── DAG Engine ─────────────────────────────────────────────────────

/// Engine that evaluates derived series in topological order.
///
/// # Evaluation Strategy
///
/// Definitions are topologically sorted into **wave-front levels**:
/// nodes within the same level share no mutual dependencies and are
/// evaluated in parallel via `rayon`.  After each level completes,
/// results are merged into the [`MultiSeriesContext`] so that the
/// next level's expressions can reference them.
pub struct DerivedSeriesEngine {
    definitions: Vec<DerivedSeriesDefinition>,
}

impl DerivedSeriesEngine {
    /// Creates a new engine from the given definitions.
    pub fn new(definitions: Vec<DerivedSeriesDefinition>) -> Self {
        Self { definitions }
    }

    /// Topologically sort and evaluate all definitions.
    ///
    /// Nodes within the same wave-front level are evaluated in parallel
    /// via rayon.  Results are merged into the context after each level
    /// so that the next level's expressions can reference them.
    pub fn evaluate(
        &self,
        ctx: &mut MultiSeriesContext,
    ) -> Result<HashMap<String, Vec<f64>>, MultivariateError> {
        use rayon::prelude::*;

        let _start = std::time::Instant::now();
        let levels = self.topological_sort_levels()?;
        let mut results = HashMap::new();

        for level in &levels {
            if level.len() == 1 {
                // Single node — no parallelism overhead.
                let def = &self.definitions[level[0]];
                let vals = def.expression.evaluate(ctx)?;
                Self::insert_into_context(ctx, &def.name, vals.clone());
                results.insert(def.name.clone(), vals);
            } else {
                // Multiple independent nodes — evaluate in parallel.
                let wave_results: Vec<Result<(String, Vec<f64>), MultivariateError>> = level
                    .par_iter()
                    .map(|&idx| {
                        let def = &self.definitions[idx];
                        let vals = def.expression.evaluate(ctx)?;
                        Ok((def.name.clone(), vals))
                    })
                    .collect();

                // Merge wave results into context (sequential).
                for result in wave_results {
                    let (name, vals) = result?;
                    Self::insert_into_context(ctx, &name, vals.clone());
                    results.insert(name, vals);
                }
            }
        }

        metrics::histogram!("chronix_derived_series_eval_duration_seconds")
            .record(_start.elapsed().as_secs_f64());
        Ok(results)
    }

    /// Insert or update a series in the context matrix.
    fn insert_into_context(ctx: &mut MultiSeriesContext, name: &str, vals: Vec<f64>) {
        if let Some(pos) = ctx.matrix.series_ids.iter().position(|n| n == name) {
            ctx.matrix.data[pos] = vals;
        } else {
            ctx.matrix.series_ids.push(name.to_string());
            ctx.matrix.data.push(vals);
        }
    }

    /// Topological sort that returns levels (wave-fronts).
    ///
    /// All nodes within a level have zero remaining in-degree and can
    /// be evaluated concurrently.
    fn topological_sort_levels(&self) -> Result<Vec<Vec<usize>>, MultivariateError> {
        let n = self.definitions.len();
        let name_to_idx: HashMap<&str, usize> = self
            .definitions
            .iter()
            .enumerate()
            .map(|(i, d)| (d.name.as_str(), i))
            .collect();

        // Build adjacency list
        let mut in_degree = vec![0usize; n];
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, def) in self.definitions.iter().enumerate() {
            for dep in &def.dependencies {
                if let Some(&j) = name_to_idx.get(dep.as_str()) {
                    adj[j].push(i);
                    in_degree[i] += 1;
                }
            }
        }

        // Kahn's algorithm — level-by-level.
        let mut current_level: Vec<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
        let mut levels = Vec::new();
        let mut processed = 0usize;

        while !current_level.is_empty() {
            let mut next_level = Vec::new();
            for &node in &current_level {
                processed += 1;
                for &next in &adj[node] {
                    in_degree[next] -= 1;
                    if in_degree[next] == 0 {
                        next_level.push(next);
                    }
                }
            }
            levels.push(current_level);
            current_level = next_level;
        }

        if processed != n {
            return Err(MultivariateError::CycleDetected);
        }
        Ok(levels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> MultiSeriesContext {
        MultiSeriesContext::build(
            vec![
                (
                    "a".to_string(),
                    vec![0, 1_000_000_000, 2_000_000_000],
                    vec![10.0, 20.0, 30.0],
                ),
                (
                    "b".to_string(),
                    vec![0, 1_000_000_000, 2_000_000_000],
                    vec![5.0, 10.0, 15.0],
                ),
            ],
            None,
        )
        .unwrap()
    }

    #[test]
    fn arithmetic_add() {
        let ctx = test_ctx();
        let expr = ArithmeticExpr {
            left: "a".to_string(),
            right: "b".to_string(),
            op: ArithOp::Add,
        };
        let result = expr.evaluate(&ctx).unwrap();
        assert!((result[0] - 15.0).abs() < 1e-6);
        assert!((result[1] - 30.0).abs() < 1e-6);
        assert!((result[2] - 45.0).abs() < 1e-6);
    }

    #[test]
    fn log_return() {
        let ctx = test_ctx();
        let expr = LogReturnExpr {
            series_name: "a".to_string(),
        };
        let result = expr.evaluate(&ctx).unwrap();
        assert!((result[0] - 0.0).abs() < 1e-6);
        assert!((result[1] - (20.0f64 / 10.0).ln()).abs() < 1e-6);
    }

    #[test]
    fn dag_evaluation() {
        let mut ctx = test_ctx();
        let defs = vec![DerivedSeriesDefinition {
            name: "spread".to_string(),
            expression: Box::new(ArithmeticExpr {
                left: "a".to_string(),
                right: "b".to_string(),
                op: ArithOp::Sub,
            }),
            dependencies: vec![],
        }];
        let engine = DerivedSeriesEngine::new(defs);
        let results = engine.evaluate(&mut ctx).unwrap();
        let spread = &results["spread"];
        assert!((spread[0] - 5.0).abs() < 1e-6);
        assert!((spread[1] - 10.0).abs() < 1e-6);
    }

    #[test]
    fn cycle_detection() {
        let defs = vec![
            DerivedSeriesDefinition {
                name: "x".to_string(),
                expression: Box::new(ArithmeticExpr {
                    left: "a".to_string(),
                    right: "y".to_string(),
                    op: ArithOp::Add,
                }),
                dependencies: vec!["y".to_string()],
            },
            DerivedSeriesDefinition {
                name: "y".to_string(),
                expression: Box::new(ArithmeticExpr {
                    left: "a".to_string(),
                    right: "x".to_string(),
                    op: ArithOp::Add,
                }),
                dependencies: vec!["x".to_string()],
            },
        ];
        let engine = DerivedSeriesEngine::new(defs);
        let mut ctx = test_ctx();
        assert!(engine.evaluate(&mut ctx).is_err());
    }

    #[test]
    fn lazy_derived_series_caches_result() {
        let ctx = test_ctx();

        let def = DerivedSeriesDefinition {
            name: "double_a".to_string(),
            expression: Box::new(ArithmeticExpr {
                left: "a".to_string(),
                right: "a".to_string(),
                op: ArithOp::Add,
            }),
            dependencies: vec!["a".to_string()],
        };

        let lazy = LazyDerivedSeries::new(def);

        // Not cached initially
        assert!(!lazy.is_cached());

        // First call evaluates
        let result = lazy.get(&ctx).unwrap();
        assert_eq!(result, vec![20.0, 40.0, 60.0]);
        assert!(lazy.is_cached());

        // Second call returns cached
        let result2 = lazy.get(&ctx).unwrap();
        assert_eq!(result2, vec![20.0, 40.0, 60.0]);

        // Invalidate clears cache
        lazy.invalidate();
        assert!(!lazy.is_cached());
    }
}
