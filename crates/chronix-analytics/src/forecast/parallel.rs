//! Parallel forecasting utilities built on rayon.

use rayon::prelude::*;

use crate::forecast::error::ForecastError;
use crate::forecast::result::ForecastResult;
use crate::forecast::traits::ForecastModel;

/// Fit multiple series in parallel.
///
/// Each element is (timestamps, values) for one series.
/// Returns one `Result` per series in the same order.
///
/// # Errors
///
/// Returns a single-element vec with `ForecastError` if slice lengths differ.
pub fn parallel_fit<M: ForecastModel + Send>(
    models: &mut [M],
    data: &[(&[i64], &[f64])],
) -> Vec<Result<(), ForecastError>> {
    let _start = std::time::Instant::now();
    if models.len() != data.len() {
        let err_msg = format!(
            "models.len() ({}) != data.len() ({})",
            models.len(),
            data.len()
        );
        return (0..models.len())
            .map(|_| Err(ForecastError::InvalidInput(err_msg.clone())))
            .collect();
    }
    let results: Vec<_> = models
        .par_iter_mut()
        .zip(data.par_iter())
        .map(|(model, (ts, vals))| model.fit(ts, vals))
        .collect();
    metrics::histogram!("chronix_compute_parallel_fit_duration_seconds")
        .record(_start.elapsed().as_secs_f64());
    results
}

/// Predict from multiple fitted models in parallel.
///
/// Returns one `Result<ForecastResult>` per model in the same order.
pub fn parallel_predict<M: ForecastModel + Sync>(
    models: &[M],
    horizon: usize,
) -> Vec<Result<ForecastResult, ForecastError>> {
    let _start = std::time::Instant::now();
    let results: Vec<_> = models
        .par_iter()
        .map(|model| model.predict(horizon))
        .collect();
    metrics::histogram!("chronix_compute_parallel_predict_duration_seconds")
        .record(_start.elapsed().as_secs_f64());
    results
}

/// Fit a single model type across many series in parallel, then predict.
///
/// This is a convenience function that creates one model per series,
/// fits each, and predicts the requested horizon.
///
/// The `factory` closure creates a fresh model instance for each series.
pub fn parallel_fit_predict<M, F>(
    data: &[(&[i64], &[f64])],
    horizon: usize,
    factory: F,
) -> Vec<Result<ForecastResult, ForecastError>>
where
    M: ForecastModel + Send,
    F: Fn() -> M + Sync,
{
    data.par_iter()
        .map(|(ts, vals)| {
            let mut model = factory();
            model.fit(ts, vals)?;
            model.predict(horizon)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forecast::ses::SesModel;

    fn make_series(len: usize, offset: f64) -> (Vec<i64>, Vec<f64>) {
        let ts: Vec<i64> = (0..len as i64).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..len).map(|i| i as f64 + offset).collect();
        (ts, vals)
    }

    #[test]
    fn parallel_fit_all_succeed() {
        let s1 = make_series(50, 0.0);
        let s2 = make_series(50, 100.0);
        let s3 = make_series(50, 200.0);
        let data: Vec<(&[i64], &[f64])> = vec![(&s1.0, &s1.1), (&s2.0, &s2.1), (&s3.0, &s3.1)];
        let mut models = vec![
            SesModel::new(None),
            SesModel::new(None),
            SesModel::new(None),
        ];
        let results = parallel_fit(&mut models, &data);
        assert!(results.iter().all(std::result::Result::is_ok));
    }

    #[test]
    fn parallel_predict_produces_forecasts() {
        let s1 = make_series(50, 0.0);
        let s2 = make_series(50, 100.0);
        let data: Vec<(&[i64], &[f64])> = vec![(&s1.0, &s1.1), (&s2.0, &s2.1)];
        let mut models = vec![SesModel::new(None), SesModel::new(None)];
        parallel_fit(&mut models, &data);
        let preds = parallel_predict(&models, 5);
        assert_eq!(preds.len(), 2);
        for p in &preds {
            let r = p.as_ref().unwrap();
            assert_eq!(r.values.len(), 5);
        }
    }

    #[test]
    fn parallel_fit_predict_convenience() {
        let s1 = make_series(50, 0.0);
        let s2 = make_series(50, 100.0);
        let s3 = make_series(50, 200.0);
        let data: Vec<(&[i64], &[f64])> = vec![(&s1.0, &s1.1), (&s2.0, &s2.1), (&s3.0, &s3.1)];
        let results = parallel_fit_predict(&data, 10, || SesModel::new(None));
        assert_eq!(results.len(), 3);
        for r in &results {
            let fc = r.as_ref().unwrap();
            assert_eq!(fc.values.len(), 10);
        }
    }

    #[test]
    fn partial_failure_propagates() {
        let good = make_series(50, 0.0);
        let bad_ts: Vec<i64> = vec![0];
        let bad_vals: Vec<f64> = vec![1.0];
        let data: Vec<(&[i64], &[f64])> = vec![(&good.0, &good.1), (&bad_ts, &bad_vals)];
        let mut models = vec![SesModel::new(None), SesModel::new(None)];
        let results = parallel_fit(&mut models, &data);
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
    }
}
