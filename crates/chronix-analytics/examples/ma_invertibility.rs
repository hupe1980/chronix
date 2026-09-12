//! Does a fitted MA polynomial ever land outside the invertible region?
//!
//! `estimate_ma` bounds each coefficient to ±0.99, which does **not** imply
//! invertibility for `q > 1`: θ = (0.99, −0.99) is inside the box and
//! `1 + 0.99B − 0.99B²` has a root at |z| ≈ 0.62. The question this answers
//! is whether the optimiser ever *reaches* such a point — the CSS objective
//! has its own reason not to, since the error recursion diverges there.
//!
//! Run with `cargo run -p chronix-analytics --example ma_invertibility`.
use chronix_analytics::forecast::{ArimaModel, ForecastModel, ModelParams};

/// Smallest |root| of `1 + θ₁z + … + θ_qz^q`, by Durand–Kerner.
///
/// Invertible iff every root lies strictly outside the unit circle, so the
/// answer is compared against 1.
fn min_root_modulus(theta: &[f64]) -> f64 {
    let q = theta.len();
    if q == 0 {
        return f64::INFINITY;
    }
    // Coefficients low-to-high: [1, θ₁, …, θ_q]; normalise by the leading one.
    let mut c: Vec<(f64, f64)> = std::iter::once(1.0)
        .chain(theta.iter().copied())
        .map(|v| (v, 0.0))
        .collect();
    let lead = c[q].0;
    if lead.abs() < 1e-12 {
        return min_root_modulus(&theta[..q - 1]);
    }
    for v in &mut c {
        v.0 /= lead;
    }

    let mul = |a: (f64, f64), b: (f64, f64)| (a.0 * b.0 - a.1 * b.1, a.0 * b.1 + a.1 * b.0);
    let sub = |a: (f64, f64), b: (f64, f64)| (a.0 - b.0, a.1 - b.1);
    let div = |a: (f64, f64), b: (f64, f64)| {
        let d = b.0 * b.0 + b.1 * b.1;
        ((a.0 * b.0 + a.1 * b.1) / d, (a.1 * b.0 - a.0 * b.1) / d)
    };
    let eval = |z: (f64, f64)| {
        let mut acc = (0.0, 0.0);
        for k in (0..=q).rev() {
            acc = mul(acc, z);
            acc = (acc.0 + c[k].0, acc.1 + c[k].1);
        }
        acc
    };

    // Distinct starting points on a circle, the standard seeding.
    let mut roots: Vec<(f64, f64)> = (0..q)
        .map(|k| {
            let a = 0.4 + 0.9 * k as f64;
            (a.cos() * 0.9, a.sin() * 0.9)
        })
        .collect();
    for _ in 0..500 {
        let mut moved = 0.0f64;
        for i in 0..q {
            let mut denom = (1.0, 0.0);
            for j in 0..q {
                if i != j {
                    denom = mul(denom, sub(roots[i], roots[j]));
                }
            }
            if denom.0.abs() + denom.1.abs() < 1e-300 {
                continue;
            }
            let delta = div(eval(roots[i]), denom);
            roots[i] = sub(roots[i], delta);
            moved = moved.max(delta.0.abs() + delta.1.abs());
        }
        if moved < 1e-14 {
            break;
        }
    }
    roots
        .iter()
        .map(|r| (r.0 * r.0 + r.1 * r.1).sqrt())
        .fold(f64::INFINITY, f64::min)
}

fn main() {
    // The claim in the header, checked directly.
    let m = min_root_modulus(&[0.99, -0.99]);
    println!(
        "theta = (0.99, -0.99): min |root| = {m:.4}  (invertible: {})",
        m > 1.0
    );

    let mut state = 7u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        ((state >> 33) as f64 / f64::from(u32::MAX >> 1)) - 1.0
    };

    let (mut fits, mut noninvertible, mut worst) = (0usize, 0usize, f64::INFINITY);
    for q in 2..=3usize {
        for trial in 0..300 {
            // A mix of shapes: white noise, a strong MA, a random walk, and a
            // trend — the optimiser should be pushed at the boundary.
            let n = 160usize;
            let mut v = Vec::with_capacity(n);
            let mut prev = [0.0f64; 3];
            let mut walk = 0.0f64;
            for i in 0..n {
                let e = next();
                let kind = trial % 4;
                let x = match kind {
                    0 => e,
                    1 => e + 0.95 * prev[0] + 0.9 * prev[1],
                    2 => {
                        walk += e;
                        walk
                    }
                    _ => i as f64 * 0.05 + e,
                };
                prev[1] = prev[0];
                prev[0] = e;
                v.push(x);
            }
            let ts: Vec<i64> = (0..n as i64).collect();
            let mut model = ArimaModel::new(1, 0, q);
            if model.fit(&ts, &v).is_err() {
                continue;
            }
            if let ModelParams::Arima { ma_coeffs, .. } = model.params() {
                if ma_coeffs.len() < 2 {
                    continue;
                }
                fits += 1;
                let r = min_root_modulus(ma_coeffs);
                worst = worst.min(r);
                if r <= 1.0 {
                    noninvertible += 1;
                    if noninvertible <= 5 {
                        println!("  non-invertible q={q}: theta={ma_coeffs:?} min|root|={r:.4}");
                    }
                }
            }
        }
    }
    println!(
        "\n{fits} fits at q>=2; {noninvertible} non-invertible; smallest |root| seen = {worst:.4}"
    );

    // The middle of the distribution says little. These are the shapes that
    // would push an optimiser to the boundary: a series with no information,
    // one with a single dominating outlier, an alternating sequence whose
    // theoretical MA representation sits *on* the unit circle, and an
    // over-differenced series, which is the textbook way to manufacture a
    // non-invertible MA(1).
    println!("\nadversarial shapes:");
    let cases: Vec<(&str, Vec<f64>)> = vec![
        ("constant", vec![5.0; 120]),
        ("two values", (0..120).map(|i| f64::from(i % 2)).collect()),
        (
            "alternating +/-1",
            (0..120)
                .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
                .collect(),
        ),
        (
            "one huge outlier",
            (0..120).map(|i| if i == 60 { 1e6 } else { 0.1 }).collect(),
        ),
        ("pure trend", (0..120).map(f64::from).collect()),
        ("over-differenced white noise", {
            let mut st = 99u64;
            let mut nx = move || {
                st = st.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                ((st >> 33) as f64 / f64::from(u32::MAX >> 1)) - 1.0
            };
            let raw: Vec<f64> = (0..121).map(|_| nx()).collect();
            raw.windows(2).map(|w| w[1] - w[0]).collect()
        }),
    ];
    for (name, v) in cases {
        let ts: Vec<i64> = (0..v.len() as i64).collect();
        for q in 2..=3usize {
            let mut model = ArimaModel::new(1, 0, q);
            match model.fit(&ts, &v) {
                Err(e) => println!("  {name:<28} q={q}  fit refused: {e}"),
                Ok(()) => {
                    if let ModelParams::Arima {
                        ma_coeffs,
                        residual_std,
                        ..
                    } = model.params()
                    {
                        let r = min_root_modulus(ma_coeffs);
                        let verdict = if r > 1.0 {
                            "invertible"
                        } else {
                            "NON-INVERTIBLE"
                        };
                        println!(
                            "  {name:<28} q={q}  min|root|={r:>8.4}  sigma={residual_std:>10.3e}  {verdict}"
                        );
                    }
                }
            }
        }
    }
}
