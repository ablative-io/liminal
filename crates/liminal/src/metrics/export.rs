use std::collections::BTreeSet;
use std::fmt::{Display, Write as _};

use super::{HistogramSnapshot, MetricValue, MetricsSnapshot};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum PrometheusMetricType {
    Counter,
    Gauge,
    Histogram,
}

impl PrometheusMetricType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

/// Render a point-in-time metrics snapshot as Prometheus 0.0.4 text.
#[must_use]
pub fn render(snapshot: &MetricsSnapshot) -> String {
    let mut output = String::new();
    let mut emitted_families = BTreeSet::new();

    for metric in snapshot.metrics() {
        let name = sanitize_metric_name(&metric.name);
        let metric_type = prometheus_metric_type(&metric.value);

        if emitted_families.insert((name.clone(), metric_type)) {
            render_family_header(&mut output, &name, metric_type);
        }

        match &metric.value {
            MetricValue::Counter(value) => {
                render_number_sample(&mut output, &name, &metric.labels, *value);
            }
            MetricValue::Gauge(value) => {
                render_number_sample(&mut output, &name, &metric.labels, *value);
            }
            MetricValue::Histogram(histogram) => {
                render_histogram(&mut output, &name, &metric.labels, histogram);
            }
        }
    }

    if output.is_empty() {
        output.push('\n');
    }

    output
}

fn render_family_header(output: &mut String, name: &str, metric_type: PrometheusMetricType) {
    let _ = writeln!(output, "# HELP {name} {name}");
    let _ = writeln!(output, "# TYPE {name} {}", metric_type.as_str());
}

const fn prometheus_metric_type(value: &MetricValue) -> PrometheusMetricType {
    match value {
        MetricValue::Counter(_) => PrometheusMetricType::Counter,
        MetricValue::Gauge(_) => PrometheusMetricType::Gauge,
        MetricValue::Histogram(_) => PrometheusMetricType::Histogram,
    }
}

fn render_number_sample<Value>(
    output: &mut String,
    name: &str,
    labels: &[(String, String)],
    value: Value,
) where
    Value: Display,
{
    let labels = render_labels(labels, None);
    let _ = writeln!(output, "{name}{labels} {value}");
}

fn render_histogram(
    output: &mut String,
    name: &str,
    labels: &[(String, String)],
    histogram: &HistogramSnapshot,
) {
    let total_count = histogram
        .buckets
        .iter()
        .fold(0_u64, |total, bucket| total.saturating_add(bucket.count));
    let mut cumulative_count = 0_u64;

    for bucket in &histogram.buckets {
        let Some(upper_bound) = bucket.upper_bound else {
            continue;
        };
        cumulative_count = cumulative_count.saturating_add(bucket.count);
        let boundary = format_bucket_bound(upper_bound);
        render_histogram_bucket(output, name, labels, &boundary, cumulative_count);
    }

    render_histogram_bucket(output, name, labels, "+Inf", total_count);

    let labels = render_labels(labels, None);
    let sum = format_sample_float(histogram.sum);
    let _ = writeln!(output, "{name}_sum{labels} {sum}");
    let _ = writeln!(output, "{name}_count{labels} {total_count}");
}

fn render_histogram_bucket(
    output: &mut String,
    name: &str,
    labels: &[(String, String)],
    upper_bound: &str,
    count: u64,
) {
    let labels = render_labels(labels, Some(("le", upper_bound)));
    let _ = writeln!(output, "{name}_bucket{labels} {count}");
}

#[must_use]
fn render_labels(labels: &[(String, String)], extra_label: Option<(&str, &str)>) -> String {
    if labels.is_empty() && extra_label.is_none() {
        return String::new();
    }

    let mut rendered = String::from("{");
    let mut first = true;

    for (name, value) in labels {
        append_label(&mut rendered, &mut first, name, value);
    }

    if let Some((name, value)) = extra_label {
        append_label(&mut rendered, &mut first, name, value);
    }

    rendered.push('}');
    rendered
}

fn append_label(output: &mut String, first: &mut bool, name: &str, value: &str) {
    if *first {
        *first = false;
    } else {
        output.push(',');
    }

    let name = sanitize_label_name(name);
    let value = escape_label_value(value);
    let _ = write!(output, "{name}=\"{value}\"");
}

#[must_use]
fn sanitize_metric_name(name: &str) -> String {
    let mut sanitized = name
        .chars()
        .map(|character| {
            if is_valid_metric_name_char(character) {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    if sanitized.is_empty() {
        sanitized.push('_');
    } else if sanitized.starts_with(|character: char| character.is_ascii_digit()) {
        // A leading digit is valid in later positions but not as the first
        // character of a Prometheus name ([a-zA-Z_:][a-zA-Z0-9_:]*).
        sanitized.insert(0, '_');
    }

    sanitized
}

const fn is_valid_metric_name_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | ':')
}

#[must_use]
fn sanitize_label_name(name: &str) -> String {
    let mut sanitized = name
        .chars()
        .map(|character| {
            if is_valid_label_name_char(character) {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    if sanitized.is_empty() {
        sanitized.push('_');
    } else if sanitized.starts_with(|character: char| character.is_ascii_digit()) {
        // A leading digit is invalid as the first character of a Prometheus
        // label name ([a-zA-Z_][a-zA-Z0-9_]*).
        sanitized.insert(0, '_');
    }

    if sanitized.starts_with("__") {
        let mut prefixed = String::from("label");
        prefixed.push_str(&sanitized);
        sanitized = prefixed;
    }

    sanitized
}

const fn is_valid_label_name_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

#[must_use]
fn escape_label_value(value: &str) -> String {
    let mut escaped = String::new();

    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }

    escaped
}

#[must_use]
fn format_bucket_bound(bound: f64) -> String {
    let mut rendered = format_sample_float(bound);

    if !is_non_integral_float_text(&rendered) {
        rendered.push_str(".0");
    }

    rendered
}

fn is_non_integral_float_text(value: &str) -> bool {
    value.contains('.')
        || value.contains('e')
        || value.contains('E')
        || value == "+Inf"
        || value == "-Inf"
        || value == "NaN"
}

#[must_use]
fn format_sample_float(value: f64) -> String {
    if value.is_nan() {
        String::from("NaN")
    } else if value.is_infinite() && value.is_sign_positive() {
        String::from("+Inf")
    } else if value.is_infinite() {
        String::from("-Inf")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{HistogramBucketSnapshot, MetricKind, MetricSnapshot, MetricsSnapshot};

    #[test]
    fn renders_counter_with_type_help_labels_and_sanitized_name() {
        let snapshot = MetricsSnapshot {
            metrics: vec![MetricSnapshot {
                name: String::from("channel-message-rate"),
                labels: vec![(String::from("channel"), String::from("orders"))],
                kind: MetricKind::Counter,
                value: MetricValue::Counter(42),
            }],
        };

        let output = render(&snapshot);

        assert!(output.contains("# HELP channel_message_rate channel_message_rate\n"));
        assert!(output.contains("# TYPE channel_message_rate counter\n"));
        assert!(output.contains("channel_message_rate{channel=\"orders\"} 42\n"));
        assert!(output.ends_with('\n'));
    }

    #[test]
    fn renders_gauge_values_without_empty_label_blocks() {
        let snapshot = MetricsSnapshot {
            metrics: vec![
                MetricSnapshot {
                    name: String::from("active_conversations"),
                    labels: Vec::new(),
                    kind: MetricKind::Gauge,
                    value: MetricValue::Gauge(7),
                },
                MetricSnapshot {
                    name: String::from("conversation_delta"),
                    labels: Vec::new(),
                    kind: MetricKind::Gauge,
                    value: MetricValue::Gauge(-3),
                },
            ],
        };

        let output = render(&snapshot);

        assert!(output.contains("# TYPE active_conversations gauge\n"));
        assert!(output.contains("active_conversations 7\n"));
        assert!(output.contains("conversation_delta -3\n"));
    }

    #[test]
    fn renders_histogram_buckets_sum_and_count() {
        let snapshot = MetricsSnapshot {
            metrics: vec![MetricSnapshot {
                name: String::from("metric_name"),
                labels: Vec::new(),
                kind: MetricKind::Histogram,
                value: MetricValue::Histogram(HistogramSnapshot {
                    buckets: vec![
                        HistogramBucketSnapshot {
                            upper_bound: Some(0.01),
                            count: 1,
                        },
                        HistogramBucketSnapshot {
                            upper_bound: Some(0.1),
                            count: 1,
                        },
                        HistogramBucketSnapshot {
                            upper_bound: Some(1.0),
                            count: 0,
                        },
                        HistogramBucketSnapshot {
                            upper_bound: None,
                            count: 1,
                        },
                    ],
                    sum: 5.055,
                }),
            }],
        };

        let output = render(&snapshot);

        assert!(output.contains("# TYPE metric_name histogram\n"));
        assert!(output.contains("metric_name_bucket{le=\"0.01\"} 1\n"));
        assert!(output.contains("metric_name_bucket{le=\"0.1\"} 2\n"));
        assert!(output.contains("metric_name_bucket{le=\"1.0\"} 2\n"));
        assert!(output.contains("metric_name_bucket{le=\"+Inf\"} 3\n"));
        assert!(output.contains("metric_name_sum 5.055\n"));
        assert!(output.contains("metric_name_count 3\n"));
    }

    #[test]
    fn escapes_label_values_and_sanitizes_label_names() {
        let snapshot = MetricsSnapshot {
            metrics: vec![MetricSnapshot {
                name: String::from("label_escape_total"),
                labels: vec![
                    (
                        String::from("bad-label"),
                        String::from("quote\" slash\\ newline\n"),
                    ),
                    (String::from("__reserved"), String::from("value")),
                ],
                kind: MetricKind::Counter,
                value: MetricValue::Counter(1),
            }],
        };

        let output = render(&snapshot);

        assert!(output.contains("bad_label=\"quote\\\" slash\\\\ newline\\n\""));
        assert!(output.contains("label__reserved=\"value\""));
    }

    #[test]
    fn sanitizers_prefix_leading_digit_to_keep_first_char_valid() {
        // Prometheus names/label names must not start with a digit; a digit is
        // only valid in later positions.
        assert_eq!(sanitize_metric_name("5xx_responses"), "_5xx_responses");
        assert_eq!(sanitize_label_name("2nd_zone"), "_2nd_zone");
        // Valid leading characters are left untouched.
        assert_eq!(sanitize_metric_name("http:requests"), "http:requests");
        assert_eq!(sanitize_label_name("zone"), "zone");
        // Empty input still yields a single underscore.
        assert_eq!(sanitize_metric_name(""), "_");
        assert_eq!(sanitize_label_name(""), "_");
    }

    #[test]
    fn render_prefixes_digit_leading_metric_and_label_names() {
        let snapshot = MetricsSnapshot {
            metrics: vec![MetricSnapshot {
                name: String::from("5xx_responses"),
                labels: vec![(String::from("2nd_zone"), String::from("alpha"))],
                kind: MetricKind::Counter,
                value: MetricValue::Counter(7),
            }],
        };

        let output = render(&snapshot);

        // Both the metric name and the label name must be prefixed with '_'
        // (these prefixed forms cannot appear unless the first-char fix runs).
        assert!(output.contains("# TYPE _5xx_responses counter\n"));
        assert!(output.contains("_5xx_responses{_2nd_zone=\"alpha\"} 7\n"));
    }

    #[test]
    fn renders_same_snapshot_identically() {
        let snapshot = MetricsSnapshot {
            metrics: vec![MetricSnapshot {
                name: String::from("stable_metric"),
                labels: vec![(String::from("channel"), String::from("orders"))],
                kind: MetricKind::Counter,
                value: MetricValue::Counter(9),
            }],
        };

        assert_eq!(render(&snapshot), render(&snapshot));
    }
}
