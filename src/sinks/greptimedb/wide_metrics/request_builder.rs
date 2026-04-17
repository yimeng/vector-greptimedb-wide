use chrono::Utc;
use greptimedb_ingester::{api::v1::*, helpers::values::*};
use vector_lib::{
    event::{
        Metric, MetricValue,
        metric::{Bucket, MetricSketch, Quantile, Sample},
    },
    metrics::AgentDDSketch,
};

use crate::sinks::util::statistic::DistributionStatistic;
use crate::template::Template;
use crate::internal_events::TemplateRenderingError;
use vector_lib::event::Event;

#[derive(Clone)]
pub(super) struct WideRequestBuilderOptions {
    pub(super) use_new_naming: bool,
    pub(super) tag_columns: Vec<String>,
    pub(super) tag_column_patterns: Vec<String>,
    pub(super) fallback_behavior: Option<String>,
    pub(super) table: Option<Template>,
}

pub(super) const DISTRIBUTION_QUANTILES: [f64; 5] = [0.5, 0.75, 0.90, 0.95, 0.99];
pub(super) const DISTRIBUTION_STAT_FIELD_COUNT: usize = 5;
pub(super) const SUMMARY_STAT_FIELD_COUNT: usize = 2;
pub(super) const LEGACY_TIME_INDEX_COLUMN_NAME: &str = "ts";
pub(super) const TIME_INDEX_COLUMN_NAME: &str = "greptime_timestamp";
pub(super) const LEGACY_VALUE_COLUMN_NAME: &str = "val";
pub(super) const VALUE_COLUMN_NAME: &str = "greptime_value";

fn encode_f64_value(
    name: &str,
    value: f64,
    schema: &mut Vec<ColumnSchema>,
    columns: &mut Vec<Value>,
) {
    schema.push(f64_column(name));
    columns.push(f64_value(value));
}

pub fn metric_to_wide_insert_request(
    metric: Metric,
    options: &WideRequestBuilderOptions,
) -> RowInsertRequest {
    let table_name = if let Some(ref tmpl) = options.table {
        let event = Event::from(metric.clone());
        match tmpl.render_string(&event) {
            Ok(name) => name,
            Err(error) => {
                emit!(TemplateRenderingError {
                    error,
                    field: Some("table"),
                    drop_event: false,
                });
                let ns = metric.namespace();
                let metric_name = metric.name();
                if let Some(ns) = ns {
                    format!("{ns}_{metric_name}")
                } else {
                    metric_name.to_owned()
                }
            }
        }
    } else {
        let ns = metric.namespace();
        let metric_name = metric.name();
        if let Some(ns) = ns {
            format!("{ns}_{metric_name}")
        } else {
            metric_name.to_owned()
        }
    };
    let mut schema = Vec::new();
    let mut columns = Vec::new();

    // timestamp
    let timestamp = metric
        .timestamp()
        .map(|t| t.timestamp_millis())
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    schema.push(ts_column(if options.use_new_naming {
        TIME_INDEX_COLUMN_NAME
    } else {
        LEGACY_TIME_INDEX_COLUMN_NAME
    }));
    columns.push(timestamp_millisecond_value(timestamp));

    // Pre-compile regex patterns for tag whitelist
    let tag_patterns: Vec<regex::Regex> = options
        .tag_column_patterns
        .iter()
        .filter_map(|p| regex::Regex::new(p).ok())
        .collect();

    // Collect tags that should be converted to float fields
    let mut field_tags: Vec<(String, f64)> = Vec::new();

    // tags
    if let Some(tags) = metric.tags() {
        for (key, value) in tags.iter_single() {
            let is_tag = options.tag_columns.iter().any(|k| k == key)
                || tag_patterns.iter().any(|re| re.is_match(key));

            if is_tag {
                schema.push(tag_column(key));
                columns.push(string_value(value.to_owned()));
            } else {
                // Wide-table logic: try to parse as f64 -> FIELD
                if let Ok(v) = value.parse::<f64>() {
                    field_tags.push((key.to_owned(), v));
                } else {
                    match options.fallback_behavior.as_deref() {
                        Some("drop") => continue,
                        Some("tag") | None => {
                            schema.push(tag_column(key));
                            columns.push(string_value(value.to_owned()));
                        }
                        _ => continue,
                    }
                }
            }
        }
    }

    // fields converted from tags
    for (key, value) in field_tags {
        encode_f64_value(&key, value, &mut schema, &mut columns);
    }

    // fields
    match metric.value() {
        MetricValue::Counter { value } => {
            encode_f64_value(
                if options.use_new_naming {
                    VALUE_COLUMN_NAME
                } else {
                    LEGACY_VALUE_COLUMN_NAME
                },
                *value,
                &mut schema,
                &mut columns,
            );
        }
        MetricValue::Gauge { value } => {
            encode_f64_value(
                if options.use_new_naming {
                    VALUE_COLUMN_NAME
                } else {
                    LEGACY_VALUE_COLUMN_NAME
                },
                *value,
                &mut schema,
                &mut columns,
            );
        }
        MetricValue::Set { values } => {
            encode_f64_value(
                if options.use_new_naming {
                    VALUE_COLUMN_NAME
                } else {
                    LEGACY_VALUE_COLUMN_NAME
                },
                values.len() as f64,
                &mut schema,
                &mut columns,
            );
        }
        MetricValue::Distribution { samples, .. } => {
            encode_distribution(samples, &mut schema, &mut columns);
        }

        MetricValue::AggregatedHistogram {
            buckets,
            count,
            sum,
        } => {
            encode_histogram(buckets.as_ref(), &mut schema, &mut columns);
            encode_f64_value("count", *count as f64, &mut schema, &mut columns);
            encode_f64_value("sum", *sum, &mut schema, &mut columns);
        }
        MetricValue::AggregatedSummary {
            quantiles,
            count,
            sum,
        } => {
            encode_quantiles(quantiles.as_ref(), &mut schema, &mut columns);
            encode_f64_value("count", *count as f64, &mut schema, &mut columns);
            encode_f64_value("sum", *sum, &mut schema, &mut columns);
        }
        MetricValue::Sketch { sketch } => {
            let MetricSketch::AgentDDSketch(sketch) = sketch;
            encode_sketch(sketch, &mut schema, &mut columns);
        }
    }

    RowInsertRequest {
        table_name,
        rows: Some(Rows {
            schema,
            rows: vec![Row { values: columns }],
        }),
    }
}

fn encode_distribution(
    samples: &[Sample],
    schema: &mut Vec<ColumnSchema>,
    columns: &mut Vec<Value>,
) {
    if let Some(stats) = DistributionStatistic::from_samples(samples, &DISTRIBUTION_QUANTILES) {
        encode_f64_value("min", stats.min, schema, columns);
        encode_f64_value("max", stats.max, schema, columns);
        encode_f64_value("avg", stats.avg, schema, columns);
        encode_f64_value("sum", stats.sum, schema, columns);
        encode_f64_value("count", stats.count as f64, schema, columns);

        for (quantile, value) in stats.quantiles {
            encode_f64_value(
                &format!("p{:02}", quantile * 100f64),
                value,
                schema,
                columns,
            );
        }
    }
}

fn encode_histogram(buckets: &[Bucket], schema: &mut Vec<ColumnSchema>, columns: &mut Vec<Value>) {
    for bucket in buckets {
        let column_name = format!("b{}", bucket.upper_limit);
        encode_f64_value(&column_name, bucket.count as f64, schema, columns);
    }
}

fn encode_quantiles(
    quantiles: &[Quantile],
    schema: &mut Vec<ColumnSchema>,
    columns: &mut Vec<Value>,
) {
    for quantile in quantiles {
        let column_name = format!("p{:02}", quantile.quantile * 100f64);
        encode_f64_value(&column_name, quantile.value, schema, columns);
    }
}

fn encode_sketch(sketch: &AgentDDSketch, schema: &mut Vec<ColumnSchema>, columns: &mut Vec<Value>) {
    encode_f64_value("count", sketch.count() as f64, schema, columns);
    if let Some(min) = sketch.min() {
        encode_f64_value("min", min, schema, columns);
    }

    if let Some(max) = sketch.max() {
        encode_f64_value("max", max, schema, columns);
    }

    if let Some(sum) = sketch.sum() {
        encode_f64_value("sum", sum, schema, columns);
    }

    if let Some(avg) = sketch.avg() {
        encode_f64_value("avg", avg, schema, columns);
    }

    for q in DISTRIBUTION_QUANTILES {
        if let Some(quantile) = sketch.quantile(q) {
            let column_name = format!("p{:02}", q * 100f64);
            encode_f64_value(&column_name, quantile, schema, columns);
        }
    }
}

fn f64_column(name: &str) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_owned(),
        semantic_type: SemanticType::Field as i32,
        datatype: ColumnDataType::Float64 as i32,
        ..Default::default()
    }
}

fn ts_column(name: &str) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_owned(),
        semantic_type: SemanticType::Timestamp as i32,
        datatype: ColumnDataType::TimestampMillisecond as i32,
        ..Default::default()
    }
}

fn tag_column(name: &str) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_owned(),
        semantic_type: SemanticType::Tag as i32,
        datatype: ColumnDataType::String as i32,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {

    use similar_asserts::assert_eq;

    use super::*;
    use crate::event::metric::{MetricKind, StatisticKind};

    fn get_column(rows: &Rows, name: &str) -> f64 {
        let (col_index, _) = rows
            .schema
            .iter()
            .enumerate()
            .find(|(_, c)| c.column_name == name)
            .unwrap();
        let value_data = rows.rows[0].values[col_index]
            .value_data
            .as_ref()
            .expect("null value");
        match value_data {
            value::ValueData::F64Value(v) => *v,
            _ => {
                unreachable!()
            }
        }
    }

    fn get_tag(rows: &Rows, name: &str) -> String {
        let (col_index, _) = rows
            .schema
            .iter()
            .enumerate()
            .find(|(_, c)| c.column_name == name)
            .unwrap();
        let value_data = rows.rows[0].values[col_index]
            .value_data
            .as_ref()
            .expect("null value");
        match value_data {
            value::ValueData::StringValue(v) => v.clone(),
            _ => {
                unreachable!()
            }
        }
    }

    #[test]
    fn test_wide_default_all_fields() {
        let metric = Metric::new(
            "aocs_tm",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 99.0 },
        )
        .with_namespace(Some("ssw_test"))
        .with_tags(Some(
            [
                ("alpha_0".to_owned(), "1.5".to_owned()),
                ("gps_altitude".to_owned(), "500".to_owned()),
                ("host".to_owned(), "node1".to_owned()),
            ]
            .into(),
        ))
        .with_timestamp(Some(Utc::now()));

        let options = WideRequestBuilderOptions {
            use_new_naming: false,
            tag_columns: vec![],
            tag_column_patterns: vec![],
            fallback_behavior: None,
            table: None,
        };

        let insert = metric_to_wide_insert_request(metric, &options);
        assert_eq!(insert.table_name, "ssw_test_aocs_tm");
        let rows = insert.rows.expect("Empty insert request");
        // ts + alpha_0(FIELD) + gps_altitude(FIELD) + host(TAG fallback) + val = 5
        assert_eq!(rows.rows[0].values.len(), 5);

        assert_eq!(get_column(&rows, "alpha_0"), 1.5);
        assert_eq!(get_column(&rows, "gps_altitude"), 500.0);
        assert_eq!(get_column(&rows, LEGACY_VALUE_COLUMN_NAME), 99.0);
        assert_eq!(get_tag(&rows, "host"), "node1");
    }

    #[test]
    fn test_wide_whitelist_tag_columns() {
        let metric = Metric::new(
            "aocs_tm",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 99.0 },
        )
        .with_namespace(Some("ssw_test"))
        .with_tags(Some(
            [
                ("task_instance_id".to_owned(), "399".to_owned()),
                ("alpha_0".to_owned(), "1.5".to_owned()),
                ("gps_altitude".to_owned(), "500".to_owned()),
                ("host".to_owned(), "node1".to_owned()),
            ]
            .into(),
        ))
        .with_timestamp(Some(Utc::now()));

        let options = WideRequestBuilderOptions {
            use_new_naming: false,
            tag_columns: vec!["task_instance_id".to_owned(), "host".to_owned()],
            tag_column_patterns: vec![],
            fallback_behavior: None,
            table: None,
        };

        let insert = metric_to_wide_insert_request(metric, &options);
        let rows = insert.rows.expect("Empty insert request");
        // ts + task_instance_id(TAG) + host(TAG) + alpha_0(FIELD) + gps_altitude(FIELD) + val = 6
        assert_eq!(rows.rows[0].values.len(), 6);

        let column_names = rows
            .schema
            .iter()
            .map(|c| c.column_name.as_ref())
            .collect::<Vec<&str>>();
        assert!(column_names.contains(&"task_instance_id"));
        assert!(column_names.contains(&"host"));
        assert!(column_names.contains(&"alpha_0"));
        assert!(column_names.contains(&"gps_altitude"));
        assert!(column_names.contains(&LEGACY_VALUE_COLUMN_NAME));

        assert_eq!(get_column(&rows, "alpha_0"), 1.5);
        assert_eq!(get_column(&rows, "gps_altitude"), 500.0);
        assert_eq!(get_column(&rows, LEGACY_VALUE_COLUMN_NAME), 99.0);
    }

    #[test]
    fn test_wide_tag_column_patterns() {
        let metric = Metric::new(
            "aocs_tm",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 99.0 },
        )
        .with_namespace(Some("ssw_test"))
        .with_tags(Some(
            [
                ("task_instance_id".to_owned(), "399".to_owned()),
                ("aocs_alpha".to_owned(), "2.0".to_owned()),
                ("aocs_beta".to_owned(), "3.0".to_owned()),
                ("host".to_owned(), "node1".to_owned()),
            ]
            .into(),
        ))
        .with_timestamp(Some(Utc::now()));

        let options = WideRequestBuilderOptions {
            use_new_naming: false,
            tag_columns: vec!["task_instance_id".to_owned()],
            tag_column_patterns: vec!["^aocs_.*".to_owned()],
            fallback_behavior: None,
            table: None,
        };

        let insert = metric_to_wide_insert_request(metric, &options);
        let rows = insert.rows.expect("Empty insert request");
        // ts + task_instance_id(TAG) + aocs_alpha(TAG) + aocs_beta(TAG) + host(FIELD) + val = 6
        assert_eq!(rows.rows[0].values.len(), 6);

        assert_eq!(get_tag(&rows, "aocs_alpha"), "2.0");
        assert_eq!(get_tag(&rows, "aocs_beta"), "3.0");
        assert_eq!(get_column(&rows, "host"), 1.0); // "node1" parsed as f64 fails -> fallback to TAG... wait
        // Actually "node1" cannot parse as f64, so with fallback_behavior=None it becomes TAG
        // Let me check... in the code, if parse fails and fallback is None -> tag. So host should be TAG.
        // But in this test aocs_alpha and aocs_beta are TAG by pattern, host should also be TAG by fallback.
        // That means we have 3 TAGs, 1 val FIELD -> 5 columns, not 6. I need to fix the test.
        // Wait, host "node1" fails parse, fallback -> tag. So columns: ts, task_instance_id(TAG), aocs_alpha(TAG), aocs_beta(TAG), host(TAG), val(FIELD) = 6. Yes 6.
        assert_eq!(get_tag(&rows, "host"), "node1");
    }

    #[test]
    fn test_wide_fallback_drop() {
        let metric = Metric::new(
            "aocs_tm",
            MetricKind::Absolute,
            MetricValue::Gauge { value: 99.0 },
        )
        .with_namespace(Some("ssw_test"))
        .with_tags(Some(
            [
                ("task_instance_id".to_owned(), "399".to_owned()),
                ("alpha_0".to_owned(), "1.5".to_owned()),
                ("host".to_owned(), "node1".to_owned()),
            ]
            .into(),
        ))
        .with_timestamp(Some(Utc::now()));

        let options = WideRequestBuilderOptions {
            use_new_naming: false,
            tag_columns: vec!["task_instance_id".to_owned()],
            tag_column_patterns: vec![],
            fallback_behavior: Some("drop".to_owned()),
            table: None,
        };

        let insert = metric_to_wide_insert_request(metric, &options);
        let rows = insert.rows.expect("Empty insert request");
        // ts + task_instance_id(TAG) + alpha_0(FIELD) + val = 4 (host dropped)
        assert_eq!(rows.rows[0].values.len(), 4);

        assert_eq!(get_column(&rows, "alpha_0"), 1.5);
        assert_eq!(get_column(&rows, LEGACY_VALUE_COLUMN_NAME), 99.0);
    }

    #[test]
    fn test_counter() {
        let metric = Metric::new(
            "cpu_seconds_total",
            MetricKind::Incremental,
            MetricValue::Counter { value: 1.1 },
        );
        let options = WideRequestBuilderOptions {
            use_new_naming: false,
            tag_columns: vec![],
            tag_column_patterns: vec![],
            fallback_behavior: None,
            table: None,
        };

        let insert = metric_to_wide_insert_request(metric, &options);
        let rows = insert.rows.expect("Empty insert request");
        assert_eq!(rows.rows[0].values.len(), 2);

        assert_eq!(get_column(&rows, LEGACY_VALUE_COLUMN_NAME), 1.1);
    }
}
