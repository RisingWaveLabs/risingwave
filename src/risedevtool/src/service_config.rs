// Copyright 2022 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct ComputeNodeConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,
    pub exporter_port: u16,
    pub async_stack_trace: String,
    pub enable_tiered_cache: bool,

    pub provide_minio: Option<Vec<MinioConfig>>,
    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_compute_node: Option<Vec<ComputeNodeConfig>>,
    pub provide_opendal: Option<Vec<OpendalConfig>>,
    pub provide_aws_s3: Option<Vec<AwsS3Config>>,
    pub provide_moat: Option<Vec<MoatConfig>>,
    pub provide_tempo: Option<Vec<TempoConfig>>,
    pub user_managed: bool,
    pub resource_group: String,

    pub total_memory_bytes: usize,
    pub parallelism: usize,
    pub role: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub enum MetaBackend {
    Memory,
    Sqlite,
    Postgres,
    Mysql,
    Env,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MetaNodeConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,
    pub dashboard_port: u16,
    pub exporter_port: u16,

    pub user_managed: bool,

    pub meta_backend: MetaBackend,
    pub provide_sqlite_backend: Option<Vec<SqliteConfig>>,
    pub provide_postgres_backend: Option<Vec<PostgresConfig>>,
    pub provide_mysql_backend: Option<Vec<MySqlConfig>>,
    pub provide_prometheus: Option<Vec<PrometheusConfig>>,

    pub provide_compute_node: Option<Vec<ComputeNodeConfig>>,
    pub provide_compactor: Option<Vec<CompactorConfig>>,

    pub provide_tempo: Option<Vec<TempoConfig>>,

    pub provide_aws_s3: Option<Vec<AwsS3Config>>,
    pub provide_minio: Option<Vec<MinioConfig>>,
    pub provide_opendal: Option<Vec<OpendalConfig>>,
    pub provide_moat: Option<Vec<MoatConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct FrontendConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,
    pub exporter_port: u16,
    pub health_check_port: u16,

    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_tempo: Option<Vec<TempoConfig>>,
    pub provide_prometheus: Option<Vec<PrometheusConfig>>,

    pub user_managed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct CompactorConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,
    pub exporter_port: u16,

    pub provide_minio: Option<Vec<MinioConfig>>,

    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_tempo: Option<Vec<TempoConfig>>,

    pub user_managed: bool,
    pub compaction_worker_threads_number: Option<usize>,

    pub compactor_mode: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MinioConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,

    pub console_address: String,
    #[serde(with = "string")]
    pub console_port: u16,

    pub root_user: String,
    pub root_password: String,
    pub hummock_bucket: String,

    pub provide_prometheus: Option<Vec<PrometheusConfig>>,

    // For rate limiting minio in a test environment.
    pub api_requests_max: usize,
    pub api_requests_deadline: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct SqliteConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub file: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct PrometheusConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,

    pub remote_write: bool,
    pub remote_write_region: String,
    pub remote_write_url: String,

    pub scrape_interval: String,

    pub provide_compute_node: Option<Vec<ComputeNodeConfig>>,
    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_minio: Option<Vec<MinioConfig>>,
    pub provide_compactor: Option<Vec<CompactorConfig>>,
    pub provide_frontend: Option<Vec<FrontendConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct GrafanaConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,
    pub address: String,
    pub listen_address: String,
    pub port: u16,

    pub provide_prometheus: Option<Vec<PrometheusConfig>>,
    pub provide_tempo: Option<Vec<TempoConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct TempoConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub listen_address: String,
    pub address: String,
    pub port: u16,
    pub otlp_port: u16,
    pub max_bytes_per_trace: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct AwsS3Config {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,
    pub bucket: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MotoConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,

    pub image: String,
    pub user_managed: bool,

    pub provide_minio: Option<Vec<MinioConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct OpendalConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,

    pub id: String,
    pub engine: String,
    pub namenode: String,
    pub bucket: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    /// Advertise address
    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    /// Port for other services in docker. They need to connect to `host.docker.internal`, while the host
    /// need to connect to `localhost`.
    pub docker_port: u16,

    #[serde(with = "string")]
    pub controller_port: u16,

    pub image: String,
    pub persist_data: bool,
    pub node_id: u32,

    pub user_managed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,

    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,

    pub provide_kafka: Option<Vec<KafkaConfig>>,

    pub image: String,
    /// Redpanda supports schema registry natively. You can configure a `user_managed` schema registry
    /// to use with redpanda.
    pub user_managed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct PubsubConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,
    #[serde(default)]
    pub user_managed: bool,
    #[serde(with = "string")]
    pub port: u16,
    pub address: String,

    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct PulsarConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    pub broker_port: u16,
    pub http_port: u16,

    pub user_managed: bool,
    pub image: String,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub port: u16,
    pub address: String,

    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub enum Application {
    Metastore,
    Connector,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MySqlConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub port: u16,
    pub address: String,

    pub user: String,
    pub password: String,
    pub database: String,

    pub application: Application,
    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub port: u16,
    pub address: String,

    pub user: String,
    pub password: String,
    pub database: String,

    pub application: Application,
    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,

    // Inject latency into any network calls to the postgres service.
    pub latency_ms: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct SqlServerConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub port: u16,
    pub address: String,

    pub user: String,
    pub password: String,
    pub database: String,

    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MongoDbConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    pub port: u16,

    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    pub port: u16,
    pub monitor_port: u16,

    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    pub port: u16,

    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct ElasticSearchConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    pub port: u16,

    pub user: String,
    pub password: String,

    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct OpenSearchConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    pub port: u16,

    pub user: String,
    pub password: String,

    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct LakekeeperConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub port: u16,
    pub address: String,

    pub user_managed: bool,
    pub persist_data: bool,

    pub catalog_backend: String,
    pub encryption_key: String,
    pub provide_postgres_backend: Option<Vec<PostgresConfig>>,
    pub provide_minio: Option<Vec<MinioConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MoatConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    pub port: u16,

    pub provide_minio: Option<Vec<MinioConfig>>,
}

/// All service configuration
#[derive(Clone, Debug, PartialEq)]
pub enum ServiceConfig {
    ComputeNode(ComputeNodeConfig),
    MetaNode(MetaNodeConfig),
    Frontend(FrontendConfig),
    Compactor(CompactorConfig),
    Minio(MinioConfig),
    Sqlite(SqliteConfig),
    Prometheus(PrometheusConfig),
    Grafana(GrafanaConfig),
    Tempo(TempoConfig),
    Opendal(OpendalConfig),
    AwsS3(AwsS3Config),
    Moto(MotoConfig),
    Kafka(KafkaConfig),
    SchemaRegistry(SchemaRegistryConfig),
    Pubsub(PubsubConfig),
    Pulsar(PulsarConfig),
    Redis(RedisConfig),
    MySql(MySqlConfig),
    Postgres(PostgresConfig),
    SqlServer(SqlServerConfig),
    MongoDb(MongoDbConfig),
    ElasticSearch(ElasticSearchConfig),
    OpenSearch(OpenSearchConfig),
    Nats(NatsConfig),
    Mqtt(MqttConfig),
    Lakekeeper(LakekeeperConfig),
    Moat(MoatConfig),
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum TaskGroup {
    RisingWave,
    Observability,
    Kafka,
    Pubsub,
    Pulsar,
    MySql,
    Postgres,
    SqlServer,
    MongoDb,
    ElasticSearch,
    OpenSearch,
    Nats,
    Mqtt,
    Redis,
    Lakekeeper,
    Moat,
}

impl ServiceConfig {
    pub fn id(&self) -> &str {
        match self {
            Self::ComputeNode(c) => &c.id,
            Self::MetaNode(c) => &c.id,
            Self::Frontend(c) => &c.id,
            Self::Compactor(c) => &c.id,
            Self::Minio(c) => &c.id,
            Self::Sqlite(c) => &c.id,
            Self::Prometheus(c) => &c.id,
            Self::Grafana(c) => &c.id,
            Self::Tempo(c) => &c.id,
            Self::AwsS3(c) => &c.id,
            Self::Moto(c) => &c.id,
            Self::Kafka(c) => &c.id,
            Self::Pubsub(c) => &c.id,
            Self::Pulsar(c) => &c.id,
            Self::Redis(c) => &c.id,
            Self::Opendal(c) => &c.id,
            Self::MySql(c) => &c.id,
            Self::Postgres(c) => &c.id,
            Self::SqlServer(c) => &c.id,
            Self::MongoDb(c) => &c.id,
            Self::ElasticSearch(c) => &c.id,
            Self::OpenSearch(c) => &c.id,
            Self::Nats(c) => &c.id,
            Self::Mqtt(c) => &c.id,
            Self::SchemaRegistry(c) => &c.id,
            Self::Lakekeeper(c) => &c.id,
            Self::Moat(c) => &c.id,
        }
    }

    /// Used to check whether the port is occupied before running the service.
    pub fn port(&self) -> Option<u16> {
        match self {
            Self::ComputeNode(c) => Some(c.port),
            Self::MetaNode(c) => Some(c.port),
            Self::Frontend(c) => Some(c.port),
            Self::Compactor(c) => Some(c.port),
            Self::Minio(c) => Some(c.port),
            Self::Sqlite(_) => None,
            Self::Prometheus(c) => Some(c.port),
            Self::Grafana(c) => Some(c.port),
            Self::Tempo(c) => Some(c.port),
            Self::AwsS3(_) => None,
            Self::Moto(c) => Some(c.port),
            Self::Kafka(c) => Some(c.port),
            Self::Pubsub(c) => Some(c.port),
            Self::Pulsar(c) => Some(c.http_port),
            Self::Redis(c) => Some(c.port),
            Self::Opendal(_) => None,
            Self::MySql(c) => Some(c.port),
            Self::Postgres(c) => Some(c.port),
            Self::SqlServer(c) => Some(c.port),
            Self::MongoDb(c) => Some(c.port),
            Self::ElasticSearch(c) => Some(c.port),
            Self::OpenSearch(c) => Some(c.port),
            Self::Nats(c) => Some(c.port),
            Self::Mqtt(c) => Some(c.port),
            Self::SchemaRegistry(c) => Some(c.port),
            Self::Lakekeeper(c) => Some(c.port),
            Self::Moat(c) => Some(c.port),
        }
    }

    pub fn user_managed(&self) -> bool {
        match self {
            Self::ComputeNode(c) => c.user_managed,
            Self::MetaNode(c) => c.user_managed,
            Self::Frontend(c) => c.user_managed,
            Self::Compactor(c) => c.user_managed,
            Self::Minio(_c) => false,
            Self::Sqlite(_c) => false,
            Self::Prometheus(_c) => false,
            Self::Grafana(_c) => false,
            Self::Tempo(_c) => false,
            Self::AwsS3(_c) => false,
            Self::Moto(c) => c.user_managed,
            Self::Kafka(c) => c.user_managed,
            Self::Pubsub(c) => c.user_managed,
            Self::Pulsar(c) => c.user_managed,
            Self::Redis(c) => c.user_managed,
            Self::Opendal(_c) => false,
            Self::MySql(c) => c.user_managed,
            Self::Postgres(c) => c.user_managed,
            Self::SqlServer(c) => c.user_managed,
            Self::MongoDb(c) => c.user_managed,
            Self::ElasticSearch(c) => c.user_managed,
            Self::OpenSearch(c) => c.user_managed,
            Self::Nats(c) => c.user_managed,
            Self::Mqtt(c) => c.user_managed,
            Self::SchemaRegistry(c) => c.user_managed,
            Self::Lakekeeper(c) => c.user_managed,
            Self::Moat(_c) => false,
        }
    }

    pub fn task_group(&self) -> TaskGroup {
        use TaskGroup::*;
        match self {
            ServiceConfig::ComputeNode(_)
            | ServiceConfig::MetaNode(_)
            | ServiceConfig::Frontend(_)
            | ServiceConfig::Compactor(_)
            | ServiceConfig::Minio(_)
            | ServiceConfig::Sqlite(_) => RisingWave,
            ServiceConfig::Prometheus(_) | ServiceConfig::Grafana(_) | ServiceConfig::Tempo(_) => {
                Observability
            }
            ServiceConfig::Opendal(_) | ServiceConfig::AwsS3(_) => RisingWave,
            ServiceConfig::Moto(_) => RisingWave,
            ServiceConfig::Kafka(_) | ServiceConfig::SchemaRegistry(_) => Kafka,
            ServiceConfig::Pubsub(_) => Pubsub,
            ServiceConfig::Pulsar(_) => Pulsar,
            ServiceConfig::Redis(_) => Redis,
            ServiceConfig::MySql(my_sql_config) => {
                if matches!(my_sql_config.application, Application::Metastore) {
                    RisingWave
                } else {
                    MySql
                }
            }
            ServiceConfig::Postgres(postgres_config) => {
                if matches!(postgres_config.application, Application::Metastore) {
                    RisingWave
                } else {
                    Postgres
                }
            }
            ServiceConfig::SqlServer(_) => SqlServer,
            ServiceConfig::MongoDb(_) => MongoDb,
            ServiceConfig::ElasticSearch(_) => ElasticSearch,
            ServiceConfig::OpenSearch(_) => OpenSearch,
            ServiceConfig::Nats(_) => Nats,
            ServiceConfig::Mqtt(_) => Mqtt,
            ServiceConfig::Lakekeeper(_) => Lakekeeper,
            ServiceConfig::Moat(_) => Moat,
        }
    }
}

mod string {
    use std::fmt::Display;
    use std::str::FromStr;

    use serde::{Deserialize, Deserializer, Serializer, de};

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Display,
        S: Serializer,
    {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: FromStr,
        T::Err: Display,
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}
