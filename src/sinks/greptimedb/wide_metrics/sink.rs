use async_trait::async_trait;
use futures::StreamExt;
use futures_util::stream::BoxStream;
use std::collections::HashMap;
use vector_lib::event::{Metric, MetricValue};

use crate::sinks::{
    greptimedb::wide_metrics::{
        batch::GreptimeDBBatchSizer,
        request::{GreptimeDBGrpcRequest, GreptimeDBGrpcRetryLogic},
        request_builder::WideRequestBuilderOptions,
        service::GreptimeDBGrpcService,
    },
    prelude::*,
    util::buffer::metrics::{MetricNormalize, MetricSet},
};

#[derive(Clone, Debug, Default)]
pub struct GreptimeDBMetricNormalize;

impl MetricNormalize for GreptimeDBMetricNormalize {
    fn normalize(&mut self, state: &mut MetricSet, metric: Metric) -> Option<Metric> {
        match (metric.kind(), &metric.value()) {
            (_, MetricValue::Counter { .. }) => state.make_absolute(metric),
            (_, MetricValue::Gauge { .. }) => state.make_absolute(metric),
            // All others are left as-is
            _ => Some(metric),
        }
    }
}

/// GreptimeDBWideGrpcSink sends metrics to GreptimeDB using a wide-table schema.
pub struct GreptimeDBWideGrpcSink {
    pub(super) service: Svc<GreptimeDBGrpcService, GreptimeDBGrpcRetryLogic>,
    pub(super) batch_settings: BatcherSettings,
    pub(super) request_builder_options: WideRequestBuilderOptions,
    pub(super) dbname: Template,
    pub(super) table: Option<Template>,
}

impl GreptimeDBWideGrpcSink {
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let options = self.request_builder_options.clone();
        let dbname_template = self.dbname.clone();
        let table_template = self.table.clone();
        input
            .map(|event| event.into_metric())
            .normalized_with_default::<GreptimeDBMetricNormalize>()
            .batched(
                self.batch_settings
                    .as_item_size_config(GreptimeDBBatchSizer),
            )
            .flat_map(move |metrics: Vec<Metric>| {
                let mut groups: HashMap<String, Vec<Metric>> = HashMap::new();
                for metric in metrics {
                    let event = Event::from(metric.clone());
                    let dbname = match dbname_template.render_string(&event) {
                        Ok(name) => name,
                        Err(error) => {
                            emit!(TemplateRenderingError {
                                error,
                                field: Some("dbname"),
                                drop_event: true,
                            });
                            continue;
                        }
                    };
                    groups.entry(dbname).or_default().push(metric);
                }
                futures::stream::iter(groups.into_iter().map({
                    let options = options.clone();
                    let table_template = table_template.clone();
                    move |(dbname, group)| {
                    GreptimeDBGrpcRequest::from_metrics(group, &options, &dbname, table_template.as_ref())
                    }
                }))
            })
            .into_driver(self.service)
            .protocol("grpc")
            .run()
            .await
    }
}

#[async_trait]
impl StreamSink<Event> for GreptimeDBWideGrpcSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
