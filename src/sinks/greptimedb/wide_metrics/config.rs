use vector_lib::{configurable::configurable_component, sensitive_string::SensitiveString};

use crate::sinks::{
    greptimedb::{
        GreptimeDBDefaultBatchSettings, default_dbname_template,
        wide_metrics::{
            request::GreptimeDBGrpcRetryLogic,
            request_builder::WideRequestBuilderOptions,
            service::{GreptimeDBGrpcService, healthcheck},
            sink,
        },
    },
    prelude::*,
};

/// Configuration items for GreptimeDB wide-metrics sink
#[configurable_component(sink("greptimedb_wide_metrics", "Ingest metrics data into GreptimeDB using a wide-table schema."))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct GreptimeDBWideMetricsConfig {
    /// The [GreptimeDB database][database] name to connect.
    ///
    /// Default to `public`, the default database of GreptimeDB.
    /// Supports template syntax to extract dbname from event fields.
    #[configurable(metadata(docs::examples = "public"))]
    #[derivative(Default(value = "default_dbname_template()"))]
    #[serde(default = "default_dbname_template")]
    pub dbname: Template,
    /// The host and port of GreptimeDB gRPC service.
    #[configurable(metadata(docs::examples = "example.com:4001"))]
    pub endpoint: String,
    /// The username for your GreptimeDB instance.
    #[configurable(metadata(docs::examples = "username"))]
    #[serde(default)]
    pub username: Option<String>,
    /// The password for your GreptimeDB instance.
    #[configurable(metadata(docs::examples = "password"))]
    #[serde(default)]
    pub password: Option<SensitiveString>,
    /// Set gRPC compression encoding for the request.
    #[configurable(metadata(docs::examples = "gzip"))]
    #[serde(default)]
    pub grpc_compression: Option<String>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub(crate) batch: BatchConfig<GreptimeDBDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    /// Use Greptime's prefixed naming for time index and value columns.
    #[configurable]
    pub new_naming: Option<bool>,

    /// Tag keys to keep as string TAGs instead of converting to fields.
    #[configurable(metadata(docs::examples = "task_instance_id"))]
    #[serde(default)]
    pub tag_columns: Vec<String>,

    /// Regex patterns for tag keys to keep as string TAGs.
    #[configurable(metadata(docs::examples = "^resource\\.service\\..*"))]
    #[serde(default)]
    pub tag_column_patterns: Vec<String>,

    /// Behavior when a non-tag attribute cannot be parsed as a float.
    ///
    /// - `"tag"` (default): store it as a STRING TAG.
    /// - `"drop"`: skip the attribute entirely.
    #[configurable(metadata(docs::examples = "tag"))]
    #[serde(default)]
    pub fallback_behavior: Option<String>,

    /// Optional table name template. If provided, overrides the default `{ns}_{metric_name}` format.
    /// Supports template syntax to extract table name from event fields.
    #[configurable(metadata(docs::examples = "{{ tags.task_instance_id }}_{{ metric.name }}"))]
    #[serde(default)]
    pub table: Option<Template>,
}

impl_generate_config_from_default!(GreptimeDBWideMetricsConfig);

#[typetag::serde(name = "greptimedb_wide_metrics")]
#[async_trait::async_trait]
impl SinkConfig for GreptimeDBWideMetricsConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let request_settings = self.request.into_settings();
        let service = ServiceBuilder::new()
            .settings(request_settings, GreptimeDBGrpcRetryLogic)
            .service(GreptimeDBGrpcService::try_new(self)?);
        let sink = sink::GreptimeDBWideGrpcSink {
            service,
            batch_settings: self.batch.into_batcher_settings()?,
            dbname: self.dbname.clone(),
            request_builder_options: WideRequestBuilderOptions {
                use_new_naming: self.new_naming.unwrap_or(false),
                tag_columns: self.tag_columns.clone(),
                tag_column_patterns: self.tag_column_patterns.clone(),
                fallback_behavior: self.fallback_behavior.clone(),
                table: self.table.clone(),
            },
        };

        let healthcheck = healthcheck(self)?;
        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<GreptimeDBWideMetricsConfig>();
    }

    #[test]
    fn test_config_with_username() {
        let config = indoc! {r#"
            endpoint = "foo-bar.ap-southeast-1.aws.greptime.cloud:4001"
            dbname = "foo-bar"
        "#};

        toml::from_str::<GreptimeDBWideMetricsConfig>(config).unwrap();
    }
}
