//! Numerical optimizer for smoothing parameter estimation.
//!
//! Implements the Nelder-Mead simplex algorithm (downhill simplex method)
//! for unconstrained optimization of objective functions over low-dimensional
//! parameter spaces (1–5 dimensions), which is the standard approach for
//! exponential smoothing parameter estimation.
//!
//! # Box constraints
//!
//! Parameters are internally transformed via a logit/sigmoid mapping to
//! enforce box constraints (e.g. α ∈ (0, 1)) while the optimizer operates
//! in unconstrained space.
//!
//! # References
//!
//! - Nelder, J.A. & Mead, R. (1965). "A simplex method for function
//!   minimization." *The Computer Journal*, 7(4), 308–313.
//! - Lagarias et al. (1998). "Convergence properties of the Nelder–Mead
//!   simplex method in low dimensions." *SIAM J. Optim.*, 9(1), 112–147.

/// Result of an optimization run.
#[derive(Debug, Clone)]
pub struct OptResult {
    /// Optimal parameter values.
    pub params: Vec<f64>,
    /// Objective function value at the optimum.
    pub value: f64,
    /// Number of function evaluations used.
    pub n_evals: usize,
    /// Whether the optimizer converged within tolerance.
    pub converged: bool,
}

/// A box constraint on a parameter.
#[derive(Debug, Clone, Copy)]
pub struct Bound {
    /// Lower bound (exclusive).
    pub lo: f64,
    /// Upper bound (exclusive).
    pub hi: f64,
}

impl Bound {
    /// Create a new bound.
    #[must_use]
    pub fn new(lo: f64, hi: f64) -> Self {
        debug_assert!(lo < hi, "lo must be less than hi");
        Self { lo, hi }
    }

    /// Transform a constrained value to unconstrained space via logit.
    fn to_unconstrained(self, x: f64) -> f64 {
        let clamped = x.clamp(self.lo + 1e-10, self.hi - 1e-10);
        let t = (clamped - self.lo) / (self.hi - self.lo);
        (t / (1.0 - t)).ln()
    }

    /// Transform an unconstrained value back to constrained space via sigmoid.
    fn to_constrained(self, y: f64) -> f64 {
        let t = 1.0 / (1.0 + (-y).exp());
        self.lo + t * (self.hi - self.lo)
    }
}

/// Minimize `f` over a bounded parameter space using Nelder-Mead.
///
/// # Arguments
///
/// * `f` — objective function to minimize, called with a slice of parameters
/// * `x0` — initial guess (must be within bounds)
/// * `bounds` — box constraints for each parameter
/// * `max_iter` — maximum number of iterations
/// * `tol` — convergence tolerance on the simplex diameter
///
/// # Returns
///
/// An [`OptResult`] with the optimal parameters in the constrained space.
pub fn minimize_nelder_mead<F>(
    f: F,
    x0: &[f64],
    bounds: &[Bound],
    max_iter: usize,
    tol: f64,
) -> OptResult
where
    F: Fn(&[f64]) -> f64,
{
    let n = x0.len();
    assert_eq!(n, bounds.len(), "x0 and bounds must have the same length");
    assert!(n > 0, "must have at least one parameter");

    // Standard Nelder-Mead coefficients
    let alpha = 1.0; // reflection
    let gamma = 2.0; // expansion
    let rho = 0.5; // contraction
    let sigma = 0.5; // shrink

    let mut n_evals = 0usize;

    // Evaluate the objective in constrained space
    let eval = |y: &[f64], evals: &mut usize| -> f64 {
        let x: Vec<f64> = y
            .iter()
            .zip(bounds.iter())
            .map(|(&yi, &b)| b.to_constrained(yi))
            .collect();
        *evals += 1;
        let val = f(&x);
        if val.is_nan() || val.is_infinite() {
            f64::MAX
        } else {
            val
        }
    };

    // Transform initial guess to unconstrained space
    let y0: Vec<f64> = x0
        .iter()
        .zip(bounds.iter())
        .map(|(&xi, &b)| b.to_unconstrained(xi))
        .collect();

    // Build initial simplex (n+1 vertices)
    let mut simplex: Vec<Vec<f64>> = Vec::with_capacity(n + 1);
    simplex.push(y0.clone());
    for i in 0..n {
        let mut vertex = y0.clone();
        // Use a perturbation that's meaningful in logit space
        let delta = if vertex[i].abs() < 1e-8 {
            0.5
        } else {
            vertex[i].abs() * 0.25
        };
        vertex[i] += delta;
        simplex.push(vertex);
    }

    // Evaluate all vertices
    let mut values: Vec<f64> = simplex.iter().map(|v| eval(v, &mut n_evals)).collect();

    for _ in 0..max_iter {
        // Sort by objective value
        let mut order: Vec<usize> = (0..=n).collect();
        order.sort_by(|&a, &b| {
            values[a]
                .partial_cmp(&values[b])
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Reorder simplex and values by rank
        let sorted_simplex: Vec<Vec<f64>> = order.iter().map(|&i| simplex[i].clone()).collect();
        let sorted_values: Vec<f64> = order.iter().map(|&i| values[i]).collect();
        simplex = sorted_simplex;
        values = sorted_values;

        // Check convergence: diameter of simplex
        let mut diameter = 0.0f64;
        for i in 1..=n {
            let dist: f64 = simplex[i]
                .iter()
                .zip(simplex[0].iter())
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f64>()
                .sqrt();
            diameter = diameter.max(dist);
        }
        // Also check value spread
        let val_spread = values[n] - values[0];
        if diameter < tol && val_spread.abs() < tol {
            break;
        }

        // Centroid of all vertices except the worst
        let centroid: Vec<f64> = (0..n)
            .map(|dim| simplex[..n].iter().map(|v| v[dim]).sum::<f64>() / n as f64)
            .collect();

        // Reflection
        let reflected: Vec<f64> = centroid
            .iter()
            .zip(simplex[n].iter())
            .map(|(&c, &w)| c + alpha * (c - w))
            .collect();
        let f_reflected = eval(&reflected, &mut n_evals);

        if f_reflected < values[0] {
            // Expansion
            let expanded: Vec<f64> = centroid
                .iter()
                .zip(simplex[n].iter())
                .map(|(&c, &w)| c + gamma * (c - w))
                .collect();
            let f_expanded = eval(&expanded, &mut n_evals);
            if f_expanded < f_reflected {
                simplex[n] = expanded;
                values[n] = f_expanded;
            } else {
                simplex[n] = reflected;
                values[n] = f_reflected;
            }
        } else if f_reflected < values[n - 1] {
            // Accept reflection
            simplex[n] = reflected;
            values[n] = f_reflected;
        } else {
            // Contraction
            let contracted: Vec<f64> = if f_reflected < values[n] {
                // Outside contraction
                centroid
                    .iter()
                    .zip(simplex[n].iter())
                    .map(|(&c, &w)| c + rho * (c - w))
                    .collect()
            } else {
                // Inside contraction
                centroid
                    .iter()
                    .zip(simplex[n].iter())
                    .map(|(&c, &w)| c - rho * (c - w))
                    .collect()
            };
            let f_contracted = eval(&contracted, &mut n_evals);

            if f_contracted < values[n].min(f_reflected) {
                simplex[n] = contracted;
                values[n] = f_contracted;
            } else {
                // Shrink: move all vertices toward the best
                for i in 1..=n {
                    for dim in 0..n {
                        simplex[i][dim] =
                            simplex[0][dim] + sigma * (simplex[i][dim] - simplex[0][dim]);
                    }
                    values[i] = eval(&simplex[i], &mut n_evals);
                }
            }
        }
    }

    // Return the best vertex in constrained space
    let best_idx = values
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
        .unwrap_or(0);

    let best_params: Vec<f64> = simplex[best_idx]
        .iter()
        .zip(bounds.iter())
        .map(|(&yi, &b)| b.to_constrained(yi))
        .collect();

    let diameter: f64 = (1..=n)
        .map(|i| {
            simplex[i]
                .iter()
                .zip(simplex[0].iter())
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f64>()
                .sqrt()
        })
        .fold(0.0f64, f64::max);

    OptResult {
        params: best_params,
        value: values[best_idx],
        n_evals,
        converged: diameter < tol,
    }
}

/// Minimize a scalar function `f(x)` over `[lo, hi]` using Brent's method.
///
/// This is specialized for 1-D optimization (e.g. SES alpha).
/// It combines golden-section search with parabolic interpolation for
/// super-linear convergence.
///
/// # Arguments
///
/// * `f` — objective function to minimize
/// * `lo`, `hi` — bracket on the optimal parameter
/// * `tol` — convergence tolerance
/// * `max_iter` — maximum number of iterations
pub fn minimize_brent<F>(f: F, lo: f64, hi: f64, tol: f64, max_iter: usize) -> OptResult
where
    F: Fn(f64) -> f64,
{
    // Golden ratio
    let golden = 0.5 * (3.0 - 5.0_f64.sqrt());

    let mut a = lo;
    let mut b = hi;
    let mut x = a + golden * (b - a);
    let mut w = x;
    let mut v = x;
    let mut fx = f(x);
    let mut fw = fx;
    let mut fv = fx;
    let mut n_evals = 1;
    let mut d: f64 = 0.0;
    let mut e: f64 = 0.0;

    for _ in 0..max_iter {
        let m = 0.5 * (a + b);
        let tol1 = tol * x.abs() + 1e-10;
        let tol2 = 2.0 * tol1;

        if (x - m).abs() <= tol2 - 0.5 * (b - a) {
            break;
        }

        let mut use_golden = true;

        // Try parabolic interpolation
        if (e).abs() > tol1 {
            let r = (x - w) * (fx - fv);
            let mut q = (x - v) * (fx - fw);
            let mut p = (x - v) * q - (x - w) * r;
            q = 2.0 * (q - r);
            if q > 0.0 {
                p = -p;
            } else {
                q = -q;
            }

            if p.abs() < (0.5 * q * e).abs() && p > q * (a - x) && p < q * (b - x) {
                // Accept parabolic step
                e = d;
                d = p / q;
                let u = x + d;
                if (u - a) < tol2 || (b - u) < tol2 {
                    d = if x < m { tol1 } else { -tol1 };
                }
                use_golden = false;
            }
        }

        if use_golden {
            e = if x < m { b - x } else { a - x };
            d = golden * e;
        }

        let u = if d.abs() >= tol1 {
            x + d
        } else if d > 0.0 {
            x + tol1
        } else {
            x - tol1
        };

        let fu = f(u);
        n_evals += 1;

        if fu <= fx {
            if u < x {
                b = x;
            } else {
                a = x;
            }
            v = w;
            fv = fw;
            w = x;
            fw = fx;
            x = u;
            fx = fu;
        } else {
            if u < x {
                a = u;
            } else {
                b = u;
            }
            if fu <= fw || (w - x).abs() < 1e-15 {
                v = w;
                fv = fw;
                w = u;
                fw = fu;
            } else if fu <= fv || (v - x).abs() < 1e-15 || (v - w).abs() < 1e-15 {
                v = u;
                fv = fu;
            }
        }
    }

    OptResult {
        params: vec![x],
        value: fx,
        n_evals,
        converged: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brent_finds_parabola_minimum() {
        // f(x) = (x - 0.3)^2 → minimum at 0.3
        let result = minimize_brent(|x| (x - 0.3).powi(2), 0.0, 1.0, 1e-8, 100);
        assert!(result.converged);
        assert!((result.params[0] - 0.3).abs() < 1e-6);
        assert!(result.value < 1e-12);
    }

    #[test]
    fn brent_asymmetric_function() {
        // f(x) = (x - 0.7)^2 + 0.1 * x
        let result = minimize_brent(|x| (x - 0.7).powi(2) + 0.1 * x, 0.0, 1.0, 1e-8, 100);
        assert!(result.converged);
        // Minimum at x = 0.7 - 0.05 = 0.65
        assert!((result.params[0] - 0.65).abs() < 1e-5);
    }

    #[test]
    fn nelder_mead_rosenbrock_2d() {
        // Rosenbrock: f(x,y) = (1-x)^2 + 100*(y-x^2)^2
        // Minimum at (1, 1) — we search in a bounded region
        let result = minimize_nelder_mead(
            |p| (1.0 - p[0]).powi(2) + 100.0 * (p[1] - p[0].powi(2)).powi(2),
            &[0.5, 0.5],
            &[Bound::new(-2.0, 3.0), Bound::new(-2.0, 3.0)],
            1000,
            1e-10,
        );
        assert!((result.params[0] - 1.0).abs() < 1e-3);
        assert!((result.params[1] - 1.0).abs() < 1e-3);
    }

    #[test]
    fn nelder_mead_1d() {
        let result = minimize_nelder_mead(
            |p| (p[0] - 0.4).powi(2),
            &[0.1],
            &[Bound::new(0.01, 0.99)],
            200,
            1e-10,
        );
        assert!((result.params[0] - 0.4).abs() < 1e-4);
    }

    #[test]
    fn nelder_mead_3d() {
        // f(x,y,z) = (x-0.3)^2 + (y-0.1)^2 + (z-0.2)^2
        let result = minimize_nelder_mead(
            |p| (p[0] - 0.3).powi(2) + (p[1] - 0.1).powi(2) + (p[2] - 0.2).powi(2),
            &[0.5, 0.5, 0.5],
            &[
                Bound::new(0.01, 0.99),
                Bound::new(0.01, 0.99),
                Bound::new(0.01, 0.99),
            ],
            500,
            1e-10,
        );
        assert!((result.params[0] - 0.3).abs() < 1e-3);
        assert!((result.params[1] - 0.1).abs() < 1e-3);
        assert!((result.params[2] - 0.2).abs() < 1e-3);
    }

    #[test]
    fn bound_roundtrip() {
        let b = Bound::new(0.0, 1.0);
        for &x in &[0.01, 0.1, 0.3, 0.5, 0.7, 0.9, 0.99] {
            let y = b.to_unconstrained(x);
            let x2 = b.to_constrained(y);
            assert!(
                (x - x2).abs() < 1e-10,
                "roundtrip failed for x={x}: got {x2}",
            );
        }
    }
}
