use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

/// A test metrics recorder that just remembers the current values of gauges and counters, and all
/// inserted values for histograms.
#[derive(Debug, Default, Clone)]
pub struct TestRecorder {
    pub metrics: Arc<Mutex<Metrics>>,
}

#[derive(Debug, Default, Clone)]
pub struct Metrics {
    metrics: HashMap<Key, Arc<Metric>>,
}

impl Metrics {
    /// Retrieve a metric with the given name, and optionally matching the given label key and value
    pub fn get(&self, name: &str, label_key: Option<&str>, label_value: Option<&str>) -> Option<(&Key, &Metric)> {
        assert!(
            label_key.is_none() || label_value.is_some(),
            "can only filter values with keys"
        );
        self.metrics
            .iter()
            .find(move |(key, _)| {
                let label_matches = key.labels().any(|label| {
                    let key_matches = label_key.map(|k| label.key() == k).unwrap_or(true);
                    let value_matches = label_value.map(|v| label.value() == v).unwrap_or(true);
                    key_matches && value_matches
                }) || label_key.is_none();
                key.name() == name && label_matches
            })
            .map(|(key, metric)| (key, metric.as_ref()))
    }
}

impl Recorder for TestRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let mut metrics = self.metrics.lock().unwrap();
        let metric = metrics
            .metrics
            .entry(key.clone())
            .or_insert(Arc::new(Metric::Counter(Default::default())));
        Counter::from_arc(metric.clone())
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        let mut metrics = self.metrics.lock().unwrap();
        let metric = metrics
            .metrics
            .entry(key.clone())
            .or_insert(Arc::new(Metric::Gauge(Default::default())));
        Gauge::from_arc(metric.clone())
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        let mut metrics = self.metrics.lock().unwrap();
        let metric = metrics
            .metrics
            .entry(key.clone())
            .or_insert(Arc::new(Metric::Histogram(Default::default())));
        Histogram::from_arc(metric.clone())
    }
}

#[derive(Debug)]
pub enum Metric {
    Histogram(Mutex<Vec<f64>>),
    Counter(Mutex<u64>),
    Gauge(Mutex<f64>),
}

impl CounterFn for Metric {
    fn increment(&self, value: u64) {
        let Metric::Counter(counter) = self else {
            panic!("expected counter");
        };
        *counter.lock().unwrap() += value;
    }

    fn absolute(&self, value: u64) {
        let Metric::Counter(counter) = self else {
            panic!("expected counter");
        };
        *counter.lock().unwrap() = value;
    }
}

impl GaugeFn for Metric {
    fn increment(&self, value: f64) {
        let Metric::Gauge(gauge) = self else {
            panic!("expected gauge");
        };
        *gauge.lock().unwrap() += value;
    }

    fn decrement(&self, value: f64) {
        let Metric::Gauge(gauge) = self else {
            panic!("expected gauge");
        };
        *gauge.lock().unwrap() -= value;
    }

    fn set(&self, value: f64) {
        let Metric::Gauge(gauge) = self else {
            panic!("expected gauge");
        };
        *gauge.lock().unwrap() = value;
    }
}

impl HistogramFn for Metric {
    fn record(&self, value: f64) {
        let Metric::Histogram(histogram) = self else {
            panic!("expected histogram");
        };
        histogram.lock().unwrap().push(value);
    }
}

pub fn get_counter_value(recorder: &TestRecorder, metric_name: &str) -> u64 {
    let metrics = recorder.metrics.lock().unwrap().clone();

    let Some((_, metric)) = metrics.get(metric_name, None, None) else {
        return 0;
    };

    let Metric::Counter(metric) = metric else {
        panic!("expected a counter metric: {}", metric_name);
    };
    let value = *metric.lock().unwrap();
    value
}
