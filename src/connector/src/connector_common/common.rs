// Copyright 2024 RisingWave Labs
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

use std::collections::BTreeMap;
use std::hash::Hash;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::{self};
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_kinesis::config::{AsyncSleep, SharedAsyncSleep, Sleep};
use moka::future::Cache as MokaCache;
use moka::ops::compute::Op;
use phf::{Set, phf_set};
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::{Authentication, OperationRetryOptions, Pulsar, TokioExecutor};
use rdkafka::ClientConfig;
use risingwave_common::bail;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::Deserialize;
use serde_with::json::JsonString;
use serde_with::{DisplayFromStr, serde_as};
use tempfile::NamedTempFile;
use time::OffsetDateTime;
use url::Url;
use with_options::WithOptions;

use crate::aws_utils::load_file_descriptor_from_s3;
use crate::deserialize_duration_from_string;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::sink::SinkError;
use crate::source::nats::source::NatsOffset;

pub const PRIVATE_LINK_BROKER_REWRITE_MAP_KEY: &str = "broker.rewrite.endpoints";
pub const PRIVATE_LINK_TARGETS_KEY: &str = "privatelink.targets";

const AWS_MSK_IAM_AUTH: &str = "AWS_MSK_IAM";

/// The environment variable to disable using default credential from environment.
/// It's recommended to set this variable to `true` in cloud hosting environment.
pub const DISABLE_DEFAULT_CREDENTIAL: &str = "DISABLE_DEFAULT_CREDENTIAL";

#[derive(Debug, Clone, Deserialize)]
pub struct AwsPrivateLinkItem {
    pub az_id: Option<String>,
    pub port: u16,
}

use aws_config::default_provider::region::DefaultRegionChain;
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_types::SdkConfig;
use aws_types::region::Region;
use risingwave_common::util::env_var::env_var_is_true;

/// A flatten config map for aws auth.
#[derive(Deserialize, Debug, Clone, WithOptions, PartialEq)]
pub struct AwsAuthProps {
    #[serde(rename = "aws.region", alias = "region", alias = "s3.region")]
    pub region: Option<String>,

    #[serde(
        rename = "aws.endpoint_url",
        alias = "endpoint_url",
        alias = "endpoint",
        alias = "s3.endpoint"
    )]
    pub endpoint: Option<String>,
    #[serde(
        rename = "aws.credentials.access_key_id",
        alias = "access_key",
        alias = "s3.access.key"
    )]
    pub access_key: Option<String>,
    #[serde(
        rename = "aws.credentials.secret_access_key",
        alias = "secret_key",
        alias = "s3.secret.key"
    )]
    pub secret_key: Option<String>,
    #[serde(rename = "aws.credentials.session_token", alias = "session_token")]
    pub session_token: Option<String>,
    /// IAM role
    #[serde(rename = "aws.credentials.role.arn", alias = "arn")]
    pub arn: Option<String>,
    /// external ID in IAM role trust policy
    #[serde(rename = "aws.credentials.role.external_id", alias = "external_id")]
    pub external_id: Option<String>,
    #[serde(rename = "aws.profile", alias = "profile")]
    pub profile: Option<String>,
    #[serde(rename = "aws.msk.signer_timeout_sec")]
    pub msk_signer_timeout_sec: Option<u64>,
}

impl EnforceSecret for AwsAuthProps {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "access_key",
        "aws.credentials.access_key_id",
        "s3.access.key",
        "secret_key",
        "aws.credentials.secret_access_key",
        "s3.secret.key",
        "session_token",
        "aws.credentials.session_token",
    };
}

impl AwsAuthProps {
    async fn build_region(&self) -> ConnectorResult<Region> {
        if let Some(region_name) = &self.region {
            Ok(Region::new(region_name.clone()))
        } else {
            let mut region_chain = DefaultRegionChain::builder();
            if let Some(profile_name) = &self.profile {
                region_chain = region_chain.profile_name(profile_name);
            }

            Ok(region_chain
                .build()
                .region()
                .await
                .context("region must be provided")?)
        }
    }

    async fn build_credential_provider(&self) -> ConnectorResult<SharedCredentialsProvider> {
        if let (Some(access_key), Some(secret_key)) =
            (self.access_key.as_ref(), self.secret_key.as_ref())
        {
            Ok(SharedCredentialsProvider::new(
                aws_credential_types::Credentials::from_keys(
                    access_key,
                    secret_key,
                    self.session_token.clone(),
                ),
            ))
        } else if !env_var_is_true(DISABLE_DEFAULT_CREDENTIAL) {
            Ok(SharedCredentialsProvider::new(
                aws_config::default_provider::credentials::default_provider().await,
            ))
        } else {
            bail!("Both `access_key` and `secret_key` must be provided")
        }
    }

    async fn with_role_provider(
        &self,
        credential: SharedCredentialsProvider,
    ) -> ConnectorResult<SharedCredentialsProvider> {
        if let Some(role_name) = &self.arn {
            let region = self.build_region().await?;
            let mut role = AssumeRoleProvider::builder(role_name)
                .session_name("RisingWave")
                .region(region);
            if let Some(id) = &self.external_id {
                role = role.external_id(id);
            }
            let provider = role.build_from_provider(credential).await;
            Ok(SharedCredentialsProvider::new(provider))
        } else {
            Ok(credential)
        }
    }

    pub async fn build_config(&self) -> ConnectorResult<SdkConfig> {
        let region = self.build_region().await?;
        let credentials_provider = self
            .with_role_provider(self.build_credential_provider().await?)
            .await?;
        let mut config_loader = aws_config::from_env()
            .region(region)
            .credentials_provider(credentials_provider);

        if let Some(endpoint) = self.endpoint.as_ref() {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        Ok(config_loader.load().await)
    }
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions, PartialEq, Hash, Eq)]
pub struct KafkaConnectionProps {
    #[serde(rename = "properties.bootstrap.server", alias = "kafka.brokers")]
    pub brokers: String,

    /// Security protocol used for RisingWave to communicate with Kafka brokers. Could be
    /// PLAINTEXT, SSL, `SASL_PLAINTEXT` or `SASL_SSL`.
    #[serde(rename = "properties.security.protocol")]
    #[with_option(allow_alter_on_fly)]
    security_protocol: Option<String>,

    #[serde(rename = "properties.ssl.endpoint.identification.algorithm")]
    #[with_option(allow_alter_on_fly)]
    ssl_endpoint_identification_algorithm: Option<String>,

    // For the properties below, please refer to [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) for more information.
    /// Path to CA certificate file for verifying the broker's key.
    #[serde(rename = "properties.ssl.ca.location")]
    #[with_option(allow_alter_on_fly)]
    ssl_ca_location: Option<String>,

    /// CA certificate string (PEM format) for verifying the broker's key.
    #[serde(rename = "properties.ssl.ca.pem")]
    #[with_option(allow_alter_on_fly)]
    ssl_ca_pem: Option<String>,

    /// Path to client's certificate file (PEM).
    #[serde(rename = "properties.ssl.certificate.location")]
    #[with_option(allow_alter_on_fly)]
    ssl_certificate_location: Option<String>,

    /// Client's public key string (PEM format) used for authentication.
    #[serde(rename = "properties.ssl.certificate.pem")]
    #[with_option(allow_alter_on_fly)]
    ssl_certificate_pem: Option<String>,

    /// Path to client's private key file (PEM).
    #[serde(rename = "properties.ssl.key.location")]
    #[with_option(allow_alter_on_fly)]
    ssl_key_location: Option<String>,

    /// Client's private key string (PEM format) used for authentication.
    #[serde(rename = "properties.ssl.key.pem")]
    #[with_option(allow_alter_on_fly)]
    ssl_key_pem: Option<String>,

    /// Passphrase of client's private key.
    #[serde(rename = "properties.ssl.key.password")]
    #[with_option(allow_alter_on_fly)]
    ssl_key_password: Option<String>,

    /// SASL mechanism if SASL is enabled. Currently support PLAIN, SCRAM, GSSAPI, OAUTHBEARER, and `AWS_MSK_IAM`.
    #[serde(rename = "properties.sasl.mechanism")]
    #[with_option(allow_alter_on_fly)]
    sasl_mechanism: Option<String>,

    /// SASL username for SASL/PLAIN and SASL/SCRAM.
    #[serde(rename = "properties.sasl.username")]
    #[with_option(allow_alter_on_fly)]
    sasl_username: Option<String>,

    /// SASL password for SASL/PLAIN and SASL/SCRAM.
    #[serde(rename = "properties.sasl.password")]
    #[with_option(allow_alter_on_fly)]
    sasl_password: Option<String>,

    /// Kafka server's Kerberos principal name under SASL/GSSAPI, not including /hostname@REALM.
    #[serde(rename = "properties.sasl.kerberos.service.name")]
    #[with_option(allow_alter_on_fly)]
    sasl_kerberos_service_name: Option<String>,

    /// Path to client's Kerberos keytab file under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.keytab")]
    #[with_option(allow_alter_on_fly)]
    sasl_kerberos_keytab: Option<String>,

    /// Client's Kerberos principal name under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.principal")]
    #[with_option(allow_alter_on_fly)]
    sasl_kerberos_principal: Option<String>,

    /// Shell command to refresh or acquire the client's Kerberos ticket under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.kinit.cmd")]
    #[with_option(allow_alter_on_fly)]
    sasl_kerberos_kinit_cmd: Option<String>,

    /// Minimum time in milliseconds between key refresh attempts under SASL/GSSAPI.
    #[serde(rename = "properties.sasl.kerberos.min.time.before.relogin")]
    #[with_option(allow_alter_on_fly)]
    sasl_kerberos_min_time_before_relogin: Option<String>,

    /// Configurations for SASL/OAUTHBEARER.
    #[serde(rename = "properties.sasl.oauthbearer.config")]
    #[with_option(allow_alter_on_fly)]
    sasl_oathbearer_config: Option<String>,

    /// SASL/OAUTHBEARER token acquisition method. Set to "oidc" to use the OIDC client credentials flow.
    #[serde(rename = "properties.sasl.oauthbearer.method")]
    #[with_option(allow_alter_on_fly)]
    sasl_oauthbearer_method: Option<String>,

    /// Client ID for SASL/OAUTHBEARER OIDC provider.
    #[serde(rename = "properties.sasl.oauthbearer.client.id")]
    #[with_option(allow_alter_on_fly)]
    sasl_oauthbearer_client_id: Option<String>,

    /// Client secret for SASL/OAUTHBEARER OIDC provider.
    #[serde(rename = "properties.sasl.oauthbearer.client.secret")]
    #[with_option(allow_alter_on_fly)]
    sasl_oauthbearer_client_secret: Option<String>,

    /// OIDC token endpoint URL for SASL/OAUTHBEARER.
    #[serde(rename = "properties.sasl.oauthbearer.token.endpoint.url")]
    #[with_option(allow_alter_on_fly)]
    sasl_oauthbearer_token_endpoint_url: Option<String>,

    /// OIDC scope for SASL/OAUTHBEARER.
    #[serde(rename = "properties.sasl.oauthbearer.scope")]
    #[with_option(allow_alter_on_fly)]
    sasl_oauthbearer_scope: Option<String>,

    /// SASL/OAUTHBEARER extensions for broker-facing claims (e.g. Confluent Cloud logicalCluster, identityPoolId).
    #[serde(rename = "properties.sasl.oauthbearer.extensions")]
    #[with_option(allow_alter_on_fly)]
    sasl_oauthbearer_extensions: Option<String>,
}

impl EnforceSecret for KafkaConnectionProps {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "properties.ssl.key.pem",
        "properties.ssl.key.password",
        "properties.sasl.password",
        "properties.sasl.oauthbearer.client.secret",
    };
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct KafkaCommon {
    // connection related props are moved to `KafkaConnection`
    #[serde(rename = "topic", alias = "kafka.topic")]
    pub topic: String,

    #[serde(
        rename = "properties.sync.call.timeout",
        deserialize_with = "deserialize_duration_from_string",
        default = "default_kafka_sync_call_timeout"
    )]
    #[with_option(allow_alter_on_fly)]
    pub sync_call_timeout: Duration,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions, PartialEq, Hash, Eq)]
pub struct KafkaPrivateLinkCommon {
    /// This is generated from `private_link_targets` and `private_link_endpoint` in frontend, instead of given by users.
    #[serde(rename = "broker.rewrite.endpoints")]
    #[serde_as(as = "Option<JsonString>")]
    pub broker_rewrite_map: Option<BTreeMap<String, String>>,
}

const fn default_kafka_sync_call_timeout() -> Duration {
    Duration::from_secs(5)
}

const fn default_socket_keepalive_enable() -> bool {
    true
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct RdKafkaPropertiesCommon {
    /// Maximum Kafka protocol request message size. Due to differing framing overhead between
    /// protocol versions the producer is unable to reliably enforce a strict max message limit at
    /// produce time and may exceed the maximum size by one message in protocol `ProduceRequests`,
    /// the broker will enforce the topic's max.message.bytes limit
    #[serde(rename = "properties.message.max.bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub message_max_bytes: Option<usize>,

    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid
    /// memory exhaustion in case of protocol hickups. This value must be at least fetch.max.bytes
    /// + 512 to allow for protocol overhead; the value is adjusted automatically unless the
    /// configuration property is explicitly set.
    #[serde(rename = "properties.receive.message.max.bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub receive_message_max_bytes: Option<usize>,

    #[serde(rename = "properties.statistics.interval.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub statistics_interval_ms: Option<usize>,

    /// Client identifier
    #[serde(rename = "properties.client.id")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub client_id: Option<String>,

    #[serde(rename = "properties.enable.ssl.certificate.verification")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub enable_ssl_certificate_verification: Option<bool>,

    /// Initial backoff time in milliseconds before reconnecting to a broker after a connection
    /// closes.
    #[serde(rename = "properties.reconnect.backoff.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub reconnect_backoff_ms: Option<usize>,

    /// Maximum backoff time in milliseconds before reconnecting to a broker after a connection
    /// closes.
    #[serde(rename = "properties.reconnect.backoff.max.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub reconnect_backoff_max_ms: Option<usize>,

    /// Maximum time in milliseconds allowed for broker connection setup, including TCP setup and
    /// SSL/SASL handshakes.
    #[serde(rename = "properties.socket.connection.setup.timeout.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub socket_connection_setup_timeout_ms: Option<usize>,

    #[serde(
        rename = "properties.socket.keepalive.enable",
        default = "default_socket_keepalive_enable"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub socket_keepalive_enable: bool,

    /// Initial backoff time in milliseconds before retrying a failed protocol request.
    #[serde(rename = "properties.retry.backoff.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub retry_backoff_ms: Option<usize>,

    /// Maximum backoff time in milliseconds before retrying a failed protocol request.
    #[serde(rename = "properties.retry.backoff.max.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[with_option(allow_alter_on_fly)]
    pub retry_backoff_max_ms: Option<usize>,
}

impl RdKafkaPropertiesCommon {
    pub(crate) fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        if let Some(v) = self.statistics_interval_ms {
            c.set("statistics.interval.ms", v.to_string());
        }
        if let Some(v) = self.message_max_bytes {
            c.set("message.max.bytes", v.to_string());
        }
        if let Some(v) = self.receive_message_max_bytes {
            c.set("receive.message.max.bytes", v.to_string());
        }
        if let Some(v) = self.client_id.as_ref() {
            c.set("client.id", v);
        }
        if let Some(v) = self.enable_ssl_certificate_verification {
            c.set("enable.ssl.certificate.verification", v.to_string());
        }
        if let Some(v) = self.reconnect_backoff_ms {
            c.set("reconnect.backoff.ms", v.to_string());
        }
        if let Some(v) = self.reconnect_backoff_max_ms {
            c.set("reconnect.backoff.max.ms", v.to_string());
        }
        if let Some(v) = self.socket_connection_setup_timeout_ms {
            c.set("socket.connection.setup.timeout.ms", v.to_string());
        }
        c.set(
            "socket.keepalive.enable",
            self.socket_keepalive_enable.to_string(),
        );
        if let Some(v) = self.retry_backoff_ms {
            c.set("retry.backoff.ms", v.to_string());
        }
        if let Some(v) = self.retry_backoff_max_ms {
            c.set("retry.backoff.max.ms", v.to_string());
        }
    }
}

impl KafkaConnectionProps {
    #[cfg(test)]
    pub fn test_default() -> Self {
        Self {
            brokers: "localhost:9092".to_owned(),
            security_protocol: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_ca_pem: None,
            ssl_certificate_pem: None,
            ssl_key_pem: None,
            ssl_key_password: None,
            ssl_endpoint_identification_algorithm: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            sasl_kerberos_service_name: None,
            sasl_kerberos_keytab: None,
            sasl_kerberos_principal: None,
            sasl_kerberos_kinit_cmd: None,
            sasl_kerberos_min_time_before_relogin: None,
            sasl_oathbearer_config: None,
            sasl_oauthbearer_method: None,
            sasl_oauthbearer_client_id: None,
            sasl_oauthbearer_client_secret: None,
            sasl_oauthbearer_token_endpoint_url: None,
            sasl_oauthbearer_scope: None,
            sasl_oauthbearer_extensions: None,
        }
    }

    pub(crate) fn set_security_properties(&self, config: &mut ClientConfig) {
        // AWS_MSK_IAM
        if self.is_aws_msk_iam() {
            config.set("security.protocol", "SASL_SSL");
            config.set("sasl.mechanism", "OAUTHBEARER");
            return;
        }

        // Security protocol
        if let Some(security_protocol) = self.security_protocol.as_ref() {
            config.set("security.protocol", security_protocol);
        }

        // SSL
        if let Some(ssl_ca_location) = self.ssl_ca_location.as_ref() {
            config.set("ssl.ca.location", ssl_ca_location);
        }
        if let Some(ssl_ca_pem) = self.ssl_ca_pem.as_ref() {
            config.set("ssl.ca.pem", ssl_ca_pem);
        }
        if let Some(ssl_certificate_location) = self.ssl_certificate_location.as_ref() {
            config.set("ssl.certificate.location", ssl_certificate_location);
        }
        if let Some(ssl_certificate_pem) = self.ssl_certificate_pem.as_ref() {
            config.set("ssl.certificate.pem", ssl_certificate_pem);
        }
        if let Some(ssl_key_location) = self.ssl_key_location.as_ref() {
            config.set("ssl.key.location", ssl_key_location);
        }
        if let Some(ssl_key_pem) = self.ssl_key_pem.as_ref() {
            config.set("ssl.key.pem", ssl_key_pem);
        }
        if let Some(ssl_key_password) = self.ssl_key_password.as_ref() {
            config.set("ssl.key.password", ssl_key_password);
        }
        if let Some(ssl_endpoint_identification_algorithm) =
            self.ssl_endpoint_identification_algorithm.as_ref()
        {
            // accept only `none` and `http` here, let the sdk do the check
            config.set(
                "ssl.endpoint.identification.algorithm",
                ssl_endpoint_identification_algorithm,
            );
        }

        // SASL mechanism
        if let Some(sasl_mechanism) = self.sasl_mechanism.as_ref() {
            config.set("sasl.mechanism", sasl_mechanism);
        }

        // SASL/PLAIN & SASL/SCRAM
        if let Some(sasl_username) = self.sasl_username.as_ref() {
            config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = self.sasl_password.as_ref() {
            config.set("sasl.password", sasl_password);
        }

        // SASL/GSSAPI
        if let Some(sasl_kerberos_service_name) = self.sasl_kerberos_service_name.as_ref() {
            config.set("sasl.kerberos.service.name", sasl_kerberos_service_name);
        }
        if let Some(sasl_kerberos_keytab) = self.sasl_kerberos_keytab.as_ref() {
            config.set("sasl.kerberos.keytab", sasl_kerberos_keytab);
        }
        if let Some(sasl_kerberos_principal) = self.sasl_kerberos_principal.as_ref() {
            config.set("sasl.kerberos.principal", sasl_kerberos_principal);
        }
        if let Some(sasl_kerberos_kinit_cmd) = self.sasl_kerberos_kinit_cmd.as_ref() {
            config.set("sasl.kerberos.kinit.cmd", sasl_kerberos_kinit_cmd);
        }
        if let Some(sasl_kerberos_min_time_before_relogin) =
            self.sasl_kerberos_min_time_before_relogin.as_ref()
        {
            config.set(
                "sasl.kerberos.min.time.before.relogin",
                sasl_kerberos_min_time_before_relogin,
            );
        }

        // SASL/OAUTHBEARER
        if let Some(sasl_oathbearer_config) = self.sasl_oathbearer_config.as_ref() {
            config.set("sasl.oauthbearer.config", sasl_oathbearer_config);
        }
        if let Some(sasl_oauthbearer_method) = self.sasl_oauthbearer_method.as_ref() {
            config.set("sasl.oauthbearer.method", sasl_oauthbearer_method);
        }
        if let Some(sasl_oauthbearer_client_id) = self.sasl_oauthbearer_client_id.as_ref() {
            config.set("sasl.oauthbearer.client.id", sasl_oauthbearer_client_id);
        }
        if let Some(sasl_oauthbearer_client_secret) = self.sasl_oauthbearer_client_secret.as_ref() {
            config.set(
                "sasl.oauthbearer.client.secret",
                sasl_oauthbearer_client_secret,
            );
        }
        if let Some(sasl_oauthbearer_token_endpoint_url) =
            self.sasl_oauthbearer_token_endpoint_url.as_ref()
        {
            config.set(
                "sasl.oauthbearer.token.endpoint.url",
                sasl_oauthbearer_token_endpoint_url,
            );
        }
        if let Some(sasl_oauthbearer_scope) = self.sasl_oauthbearer_scope.as_ref() {
            config.set("sasl.oauthbearer.scope", sasl_oauthbearer_scope);
        }
        if let Some(sasl_oauthbearer_extensions) = self.sasl_oauthbearer_extensions.as_ref() {
            config.set("sasl.oauthbearer.extensions", sasl_oauthbearer_extensions);
        }
        // Only enable unsecured JWT when not using OIDC method.
        if !self.is_oauthbearer_oidc() {
            config.set("enable.sasl.oauthbearer.unsecure.jwt", "true");
        }
    }

    pub(crate) fn is_oauthbearer_oidc(&self) -> bool {
        self.sasl_oauthbearer_method
            .as_deref()
            .is_some_and(|m| m.eq_ignore_ascii_case("oidc"))
    }

    pub(crate) fn is_aws_msk_iam(&self) -> bool {
        if let Some(sasl_mechanism) = self.sasl_mechanism.as_ref()
            && sasl_mechanism == AWS_MSK_IAM_AUTH
        {
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct PulsarCommon {
    #[serde(rename = "topic", alias = "pulsar.topic")]
    pub topic: String,

    #[serde(rename = "service.url", alias = "pulsar.service.url")]
    pub service_url: String,

    #[serde(rename = "auth.token")]
    pub auth_token: Option<String>,
}

impl EnforceSecret for PulsarCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "pulsar.auth.token",
    };
}

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct PulsarOauthCommon {
    #[serde(rename = "oauth.issuer.url")]
    pub issuer_url: String,

    #[serde(rename = "oauth.credentials.url")]
    pub credentials_url: String,

    #[serde(rename = "oauth.audience")]
    pub audience: String,

    #[serde(rename = "oauth.scope")]
    pub scope: Option<String>,
}

fn create_credential_temp_file(credentials: &[u8]) -> std::io::Result<NamedTempFile> {
    let mut f = NamedTempFile::new()?;
    f.write_all(credentials)?;
    f.as_file().sync_all()?;
    Ok(f)
}

impl PulsarCommon {
    pub(crate) async fn build_client(
        &self,
        oauth: &Option<PulsarOauthCommon>,
        aws_auth_props: &AwsAuthProps,
        operation_retry_options: Option<OperationRetryOptions>,
    ) -> ConnectorResult<Pulsar<TokioExecutor>> {
        let mut pulsar_builder = Pulsar::builder(&self.service_url, TokioExecutor);
        let mut _temp_file = None; // Keep temp file alive

        if let Some(oauth) = oauth.as_ref() {
            let (credentials_url, temp_file) = self
                .resolve_pulsar_credentials_url(oauth, aws_auth_props)
                .await?;
            _temp_file = temp_file;

            let auth_params = OAuth2Params {
                issuer_url: oauth.issuer_url.clone(),
                credentials_url,
                audience: Some(oauth.audience.clone()),
                scope: oauth.scope.clone(),
            };

            pulsar_builder = pulsar_builder
                .with_auth_provider(OAuth2Authentication::client_credentials(auth_params));
        } else if let Some(auth_token) = &self.auth_token {
            pulsar_builder = pulsar_builder.with_auth(Authentication {
                name: "token".to_owned(),
                data: Vec::from(auth_token.as_str()),
            });
        }

        if let Some(operation_retry_options) = operation_retry_options {
            tracing::info!(
                max_retries = ?operation_retry_options.max_retries,
                retry_delay_ms = operation_retry_options.retry_delay.as_millis(),
                "applying Pulsar source operation retry override"
            );
            pulsar_builder = pulsar_builder.with_operation_retry_options(operation_retry_options);
        }

        let res = pulsar_builder.build().await.map_err(|e| anyhow!(e))?;
        drop(_temp_file); // Explicitly drop temp file after client is built
        Ok(res)
    }

    pub(crate) async fn resolve_pulsar_credentials_url(
        &self,
        oauth: &PulsarOauthCommon,
        aws_auth_props: &AwsAuthProps,
    ) -> ConnectorResult<(String, Option<NamedTempFile>)> {
        // Try parsing as URL first
        if let Ok(url) = Url::parse(&oauth.credentials_url) {
            return self
                .handle_pulsar_credentials_url(&url, aws_auth_props)
                .await;
        }

        // If not a valid URL, check if it's an absolute file path
        let path = Path::new(&oauth.credentials_url);
        if !path.is_absolute() {
            bail!("credentials_url must be a valid URL (s3://, file://) or an absolute file path");
        }

        // Verify the file exists
        if !tokio::fs::try_exists(&oauth.credentials_url)
            .await
            .unwrap_or(false)
        {
            bail!("credentials file does not exist: {}", oauth.credentials_url);
        }

        // Return absolute path with file:// prefix
        Ok((format!("file://{}", oauth.credentials_url), None))
    }

    pub(crate) async fn handle_pulsar_credentials_url(
        &self,
        url: &Url,
        aws_auth_props: &AwsAuthProps,
    ) -> ConnectorResult<(String, Option<NamedTempFile>)> {
        match url.scheme() {
            "s3" => {
                let credentials = load_file_descriptor_from_s3(url, aws_auth_props).await?;
                let temp_file = create_credential_temp_file(&credentials)
                    .context("failed to create temp file for pulsar credentials")?;

                let temp_path = temp_file
                    .path()
                    .to_str()
                    .context("temp file path is not valid UTF-8")?;

                Ok((format!("file://{}", temp_path), Some(temp_file)))
            }
            "file" => Ok((url.to_string(), None)),
            _ => bail!(
                "invalid credentials_url scheme '{}', only file://, s3://, and absolute file paths are supported",
                url.scheme()
            ),
        }
    }
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct KinesisCommon {
    #[serde(rename = "stream", alias = "kinesis.stream.name")]
    pub stream_name: String,
    #[serde(rename = "aws.region", alias = "kinesis.stream.region")]
    pub stream_region: String,
    #[serde(rename = "endpoint", alias = "kinesis.endpoint")]
    pub endpoint: Option<String>,
    #[serde(
        rename = "aws.credentials.access_key_id",
        alias = "kinesis.credentials.access"
    )]
    pub credentials_access_key: Option<String>,
    #[serde(
        rename = "aws.credentials.secret_access_key",
        alias = "kinesis.credentials.secret"
    )]
    pub credentials_secret_access_key: Option<String>,
    #[serde(
        rename = "aws.credentials.session_token",
        alias = "kinesis.credentials.session_token"
    )]
    pub session_token: Option<String>,
    #[serde(rename = "aws.credentials.role.arn", alias = "kinesis.assumerole.arn")]
    pub assume_role_arn: Option<String>,
    #[serde(
        rename = "aws.credentials.role.external_id",
        alias = "kinesis.assumerole.external_id"
    )]
    pub assume_role_external_id: Option<String>,

    // sdk options
    #[serde(
        rename = "kinesis.sdk.connect_timeout_ms",
        default = "kinesis_default_connect_timeout_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub sdk_connect_timeout_ms: u64,

    #[serde(
        rename = "kinesis.sdk.read_timeout_ms",
        default = "kinesis_default_read_timeout_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub sdk_read_timeout_ms: u64,

    #[serde(
        rename = "kinesis.sdk.operation_timeout_ms",
        default = "kinesis_default_operation_timeout_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub sdk_operation_timeout_ms: u64,

    #[serde(
        rename = "kinesis.sdk.operation_attempt_timeout_ms",
        default = "kinesis_default_operation_attempt_timeout_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub sdk_operation_attempt_timeout_ms: u64,

    #[serde(
        rename = "kinesis.sdk.max_retry_limit",
        default = "kinesis_default_max_retry_limit"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub sdk_max_retry_limit: u32,

    #[serde(
        rename = "kinesis.sdk.init_backoff_ms",
        default = "kinesis_default_init_backoff_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub sdk_init_backoff_ms: u64,

    #[serde(
        rename = "kinesis.sdk.max_backoff_ms",
        default = "kinesis_default_max_backoff_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub sdk_max_backoff_ms: u64,
}

#[derive(Debug)]
pub struct KinesisAsyncSleepImpl;

impl AsyncSleep for KinesisAsyncSleepImpl {
    fn sleep(&self, duration: Duration) -> Sleep {
        Sleep::new(async move { tokio::time::sleep(duration).await })
    }
}

const fn kinesis_default_connect_timeout_ms() -> u64 {
    10000
}

const fn kinesis_default_read_timeout_ms() -> u64 {
    10000
}

const fn kinesis_default_operation_timeout_ms() -> u64 {
    10000
}

const fn kinesis_default_operation_attempt_timeout_ms() -> u64 {
    10000
}

const fn kinesis_default_init_backoff_ms() -> u64 {
    1000
}

const fn kinesis_default_max_backoff_ms() -> u64 {
    20000
}

const fn kinesis_default_max_retry_limit() -> u32 {
    3
}

impl EnforceSecret for KinesisCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "kinesis.credentials.access",
        "kinesis.credentials.secret",
        "kinesis.credentials.session_token",
    };
}

impl KinesisCommon {
    pub(crate) async fn build_client(&self) -> ConnectorResult<KinesisClient> {
        let config = AwsAuthProps {
            region: Some(self.stream_region.clone()),
            endpoint: self.endpoint.clone(),
            access_key: self.credentials_access_key.clone(),
            secret_key: self.credentials_secret_access_key.clone(),
            session_token: self.session_token.clone(),
            arn: self.assume_role_arn.clone(),
            external_id: self.assume_role_external_id.clone(),
            profile: Default::default(),
            msk_signer_timeout_sec: Default::default(),
        };
        let aws_config = config.build_config().await?;
        let mut builder = aws_sdk_kinesis::config::Builder::from(&aws_config);
        {
            // for timeout and retry config
            let sleep_impl = SharedAsyncSleep::new(KinesisAsyncSleepImpl);
            builder.set_sleep_impl(Some(sleep_impl));
            let timeout_config = aws_smithy_types::timeout::TimeoutConfig::builder()
                .connect_timeout(Duration::from_millis(self.sdk_connect_timeout_ms))
                .read_timeout(Duration::from_millis(self.sdk_read_timeout_ms))
                .operation_timeout(Duration::from_millis(self.sdk_operation_timeout_ms))
                .operation_attempt_timeout(Duration::from_millis(
                    self.sdk_operation_attempt_timeout_ms,
                ))
                .build();
            builder.set_timeout_config(Some(timeout_config));

            let retry_config = aws_smithy_types::retry::RetryConfig::standard()
                .with_initial_backoff(Duration::from_millis(self.sdk_init_backoff_ms))
                .with_max_backoff(Duration::from_millis(self.sdk_max_backoff_ms))
                .with_max_attempts(self.sdk_max_retry_limit);
            builder.set_retry_config(Some(retry_config));
        }
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        Ok(KinesisClient::from_conf(builder.build()))
    }
}

/// Connection properties for NATS, used as a cache key for shared clients.
/// This includes all properties that affect the connection itself (not stream/subject specific).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NatsConnectionProps {
    pub server_url: String,
    pub connect_mode: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub jwt: Option<String>,
    pub nkey: Option<String>,
}

/// Shared NATS client cache.
/// Client connections are cached as `Weak` pointers in the cache.
/// NATS Connector can access this cache to reuse existing client connections,
/// and avoid exhausting host machine ports.
/// When reading from the cache, the connector should `upgrade` the weak pointer to an `Arc` reference.
/// After all strong (Arc) references are dropped, the client connection will be cleaned up.
/// Cache eviction naturally takes care of the dangling weak pointers.
pub static SHARED_NATS_CLIENT: LazyLock<MokaCache<NatsConnectionProps, Weak<async_nats::Client>>> =
    LazyLock::new(|| MokaCache::builder().build());

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct NatsCommon {
    #[serde(rename = "server_url")]
    pub server_url: String,
    #[serde(rename = "subject")]
    pub subject: String,
    #[serde(rename = "connect_mode")]
    pub connect_mode: String,
    #[serde(rename = "username")]
    pub user: Option<String>,
    #[serde(rename = "password")]
    pub password: Option<String>,
    #[serde(rename = "jwt")]
    pub jwt: Option<String>,
    #[serde(rename = "nkey")]
    pub nkey: Option<String>,
    #[serde(rename = "max_bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_bytes: Option<i64>,
    #[serde(rename = "max_messages")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_messages: Option<i64>,
    #[serde(rename = "max_messages_per_subject")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_messages_per_subject: Option<i64>,
    #[serde(rename = "max_consumers")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_consumers: Option<i32>,
    #[serde(rename = "max_message_size")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub max_message_size: Option<i32>,
    #[serde(rename = "allow_create_stream", default)]
    #[serde_as(as = "DisplayFromStr")]
    pub allow_create_stream: bool,
}

impl EnforceSecret for NatsCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "password",
        "jwt",
        "nkey",
    };
}

impl NatsCommon {
    /// Extract connection properties that can be used as a cache key.
    pub fn connection_props(&self) -> NatsConnectionProps {
        NatsConnectionProps {
            server_url: self.server_url.clone(),
            connect_mode: self.connect_mode.clone(),
            user: self.user.clone(),
            password: self.password.clone(),
            jwt: self.jwt.clone(),
            nkey: self.nkey.clone(),
        }
    }

    /// Build a new NATS client without caching.
    async fn build_client_inner(&self) -> ConnectorResult<async_nats::Client> {
        let mut connect_options = async_nats::ConnectOptions::new();
        match self.connect_mode.as_str() {
            "user_and_password" => {
                if let (Some(v_user), Some(v_password)) =
                    (self.user.as_ref(), self.password.as_ref())
                {
                    connect_options =
                        connect_options.user_and_password(v_user.into(), v_password.into())
                } else {
                    bail!(
                        "NATS connect mode `user_and_password` requires both `user` and `password`"
                    );
                }
            }

            "credential" => {
                if let (Some(v_nkey), Some(v_jwt)) = (self.nkey.as_ref(), self.jwt.as_ref()) {
                    connect_options = connect_options
                        .credentials(&self.create_credential(v_nkey, v_jwt)?)
                        .expect("failed to parse static creds")
                } else {
                    bail!("NATS connect mode `credential` requires both `nkey` and `jwt`");
                }
            }
            "plain" => {}
            _ => {
                bail!(
                    "NATS connect mode must be one of `user_and_password`, `credential`, or `plain`"
                );
            }
        };

        let servers = self.server_url.split(',').collect::<Vec<&str>>();
        let client = connect_options
            .connect(
                servers
                    .iter()
                    .map(|url| url.parse())
                    .collect::<Result<Vec<async_nats::ServerAddr>, _>>()?,
            )
            .await
            .context("failed to build the NATS client")
            .map_err(SinkError::Nats)?;
        Ok(client)
    }

    /// Build a NATS client, attempting to reuse an existing cached client if available.
    /// See `SHARED_NATS_CLIENT` for more details.
    pub(crate) async fn build_client(&self) -> ConnectorResult<Arc<async_nats::Client>> {
        let connection_props = self.connection_props();
        let mut client: Option<Arc<async_nats::Client>> = None;

        SHARED_NATS_CLIENT
            .entry_by_ref(&connection_props)
            .and_try_compute_with::<_, _, crate::error::ConnectorError>(|maybe_entry| async {
                if let Some(entry) = maybe_entry
                    && let entry_value = entry.into_value()
                    && let Some(existing_client) = entry_value.upgrade()
                {
                    match existing_client.connection_state() {
                        async_nats::connection::State::Connected => {
                            tracing::info!("reusing existing NATS client for {}", self.server_url);
                            client = Some(existing_client);
                            return Ok(Op::Nop);
                        }
                        _ => {
                            tracing::warn!(
                                server_url = self.server_url,
                                "existing NATS client is not connected",
                            );
                        }
                    }
                }
                tracing::info!(
                    server_url = self.server_url,
                    "no cached NATS client was found, or the cached client disconnected; building a new client"
                );
                let new_client = Arc::new(self.build_client_inner().await?);
                client = Some(new_client.clone());
                Ok(Op::Put(Arc::downgrade(&new_client)))
            })
            .await?;

        Ok(client.expect("client should be set"))
    }

    pub(crate) async fn build_context(&self) -> ConnectorResult<jetstream::Context> {
        let client = self.build_client().await?;
        let jetstream = async_nats::jetstream::new((*client).clone());
        Ok(jetstream)
    }

    /// Build a `JetStream` context using a pre-existing client.
    pub(crate) fn build_context_from_client(
        client: &Arc<async_nats::Client>,
    ) -> jetstream::Context {
        async_nats::jetstream::new((**client).clone())
    }

    /// Build a NATS `JetStream` consumer.
    ///
    /// If `existing_client` is provided, it will be used instead of creating/fetching a new one.
    /// This allows callers to reuse a client they already hold.
    pub(crate) async fn build_consumer(
        &self,
        stream: String,
        durable_consumer_name: String,
        split_id: String,
        start_sequence: NatsOffset,
        mut config: jetstream::consumer::pull::Config,
        existing_client: Option<Arc<async_nats::Client>>,
    ) -> ConnectorResult<(
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
        Arc<async_nats::Client>,
    )> {
        let client = match existing_client {
            Some(c) => c,
            None => self.build_client().await?,
        };
        let context = Self::build_context_from_client(&client);
        let stream = self.build_or_get_stream(context.clone(), stream).await?;
        let subject_name = self
            .subject
            .replace(',', "-")
            .replace(['.', '>', '*', ' ', '\t'], "_");
        let name = format!("risingwave-consumer-{}-{}", subject_name, split_id);

        let deliver_policy = match start_sequence {
            NatsOffset::Earliest => DeliverPolicy::All,
            NatsOffset::Latest => DeliverPolicy::New,
            NatsOffset::SequenceNumber(v) => {
                // for compatibility, we do not write to any state table now
                let parsed = v
                    .parse::<u64>()
                    .context("failed to parse nats offset as sequence number")?;
                DeliverPolicy::ByStartSequence {
                    start_sequence: 1 + parsed,
                }
            }
            NatsOffset::Timestamp(v) => DeliverPolicy::ByStartTime {
                start_time: OffsetDateTime::from_unix_timestamp_nanos(v as i128 * 1_000_000)
                    .context("invalid timestamp for nats offset")?,
            },
            NatsOffset::None => DeliverPolicy::All,
        };

        let consumer = match stream.get_consumer(&name).await {
            Ok(consumer) => consumer,
            _ => {
                stream
                    .get_or_create_consumer(&name, {
                        config.deliver_policy = deliver_policy;
                        config.durable_name = Some(durable_consumer_name);
                        config.filter_subjects =
                            self.subject.split(',').map(|s| s.to_owned()).collect();
                        config
                    })
                    .await?
            }
        };
        Ok((consumer, client))
    }

    pub(crate) async fn build_or_get_stream(
        &self,
        jetstream: jetstream::Context,
        stream_str: String,
    ) -> ConnectorResult<jetstream::stream::Stream> {
        let subjects: Vec<String> = self.subject.split(',').map(|s| s.to_owned()).collect();

        // In `SourceEnumerator`, we may create a stream
        // In `SourceReader`, the desired stream MUST exist
        if let Ok(mut stream_instance) = jetstream.get_stream(&stream_str).await {
            tracing::info!(
                "load existing nats stream ({:?}) with config {:?}",
                stream_str,
                stream_instance.info().await?
            );
            return Ok(stream_instance);
        }

        if !self.allow_create_stream {
            return Err(anyhow!(
                "stream {} not found, set `allow_create_stream` to true to create a stream",
                stream_str
            )
            .into());
        }

        let mut config = jetstream::stream::Config {
            name: stream_str.clone(),
            max_bytes: 1000000,
            subjects,
            ..Default::default()
        };
        if let Some(v) = self.max_bytes {
            config.max_bytes = v;
        }
        if let Some(v) = self.max_messages {
            config.max_messages = v;
        }
        if let Some(v) = self.max_messages_per_subject {
            config.max_messages_per_subject = v;
        }
        if let Some(v) = self.max_consumers {
            config.max_consumers = v;
        }
        if let Some(v) = self.max_message_size {
            config.max_message_size = v;
        }
        tracing::info!(
            "create nats stream ({:?}) with config {:?}",
            &stream_str,
            config
        );
        let stream = jetstream.get_or_create_stream(config).await?;
        Ok(stream)
    }

    pub(crate) fn create_credential(&self, seed: &str, jwt: &str) -> ConnectorResult<String> {
        let creds = format!(
            "-----BEGIN NATS USER JWT-----\n{}\n------END NATS USER JWT------\n\n\
                         ************************* IMPORTANT *************************\n\
                         NKEY Seed printed below can be used to sign and prove identity.\n\
                         NKEYs are sensitive and should be treated as secrets.\n\n\
                         -----BEGIN USER NKEY SEED-----\n{}\n------END USER NKEY SEED------\n\n\
                         *************************************************************",
            jwt, seed
        );
        Ok(creds)
    }
}

pub(crate) fn load_certs(
    certificates: &str,
) -> ConnectorResult<Vec<rustls_pki_types::CertificateDer<'static>>> {
    let cert_bytes = if let Some(path) = certificates.strip_prefix("fs://") {
        std::fs::read_to_string(path).map(|cert| cert.as_bytes().to_owned())?
    } else {
        certificates.as_bytes().to_owned()
    };

    CertificateDer::pem_slice_iter(&cert_bytes)
        .collect::<Result<Vec<_>, _>>()
        .context("failed to parse certificates")
        .map_err(Into::into)
}

pub(crate) fn load_private_key(
    certificate: &str,
) -> ConnectorResult<rustls_pki_types::PrivateKeyDer<'static>> {
    let cert_bytes = if let Some(path) = certificate.strip_prefix("fs://") {
        std::fs::read_to_string(path).map(|cert| cert.as_bytes().to_owned())?
    } else {
        certificate.as_bytes().to_owned()
    };

    let cert = PrivatePkcs8KeyDer::pem_slice_iter(&cert_bytes)
        .next()
        .ok_or_else(|| anyhow!("No private key found"))?
        .context("failed to parse the private key")?;
    Ok(cert.into())
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct MongodbCommon {
    /// The URL of `MongoDB`
    #[serde(rename = "mongodb.url")]
    pub connect_uri: String,
    /// The collection name where data should be written to or read from. For sinks, the format is
    /// `db_name.collection_name`. Data can also be written to dynamic collections, see `collection.name.field`
    /// for more information.
    #[serde(rename = "collection.name")]
    pub collection_name: String,
}

impl EnforceSecret for MongodbCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "mongodb.url"
    };
}

impl MongodbCommon {
    pub(crate) async fn build_client(&self) -> ConnectorResult<mongodb::Client> {
        let client = mongodb::Client::with_uri_str(&self.connect_uri).await?;

        Ok(client)
    }
}

/// TCP keepalive knobs for long-lived Postgres clients, see `create_pg_client`.
#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct TcpKeepaliveConfig {
    #[serde(rename = "tcp.keepalive.idle", default = "default_tcp_keepalive_idle")]
    #[serde_as(as = "DisplayFromStr")]
    pub tcp_keepalive_idle: u32,
    #[serde(
        rename = "tcp.keepalive.interval",
        default = "default_tcp_keepalive_interval"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub tcp_keepalive_interval: u32,
    #[serde(
        rename = "tcp.keepalive.count",
        default = "default_tcp_keepalive_count"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub tcp_keepalive_count: u32,
}

const fn default_tcp_keepalive_idle() -> u32 {
    10 * 60
}

const fn default_tcp_keepalive_interval() -> u32 {
    10
}

const fn default_tcp_keepalive_count() -> u32 {
    3
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::*;

    #[test]
    fn test_oauthbearer_oidc_does_not_set_unsecure_jwt() {
        let mut props = KafkaConnectionProps::test_default();
        props.sasl_mechanism = Some("OAUTHBEARER".to_owned());
        props.sasl_oauthbearer_method = Some("oidc".to_owned());
        props.sasl_oauthbearer_client_id = Some("my-client".to_owned());
        props.sasl_oauthbearer_client_secret = Some("my-secret".to_owned());
        props.sasl_oauthbearer_token_endpoint_url =
            Some("https://idp.example.com/token".to_owned());
        props.sasl_oauthbearer_scope = Some("kafka".to_owned());

        let mut config = rdkafka::ClientConfig::new();
        props.set_security_properties(&mut config);

        let map = config.config_map();
        assert_eq!(map.get("sasl.mechanism").unwrap(), "OAUTHBEARER");
        assert_eq!(map.get("sasl.oauthbearer.method").unwrap(), "oidc");
        assert_eq!(map.get("sasl.oauthbearer.client.id").unwrap(), "my-client");
        assert_eq!(
            map.get("sasl.oauthbearer.client.secret").unwrap(),
            "my-secret"
        );
        assert_eq!(
            map.get("sasl.oauthbearer.token.endpoint.url").unwrap(),
            "https://idp.example.com/token"
        );
        assert_eq!(map.get("sasl.oauthbearer.scope").unwrap(), "kafka");
        assert!(
            !map.contains_key("enable.sasl.oauthbearer.unsecure.jwt"),
            "unsecure JWT must not be set when method=oidc"
        );
    }

    #[test]
    fn test_default_oauthbearer_sets_unsecure_jwt() {
        let mut props = KafkaConnectionProps::test_default();
        props.sasl_mechanism = Some("OAUTHBEARER".to_owned());
        props.sasl_oathbearer_config = Some("principal=user".to_owned());

        let mut config = rdkafka::ClientConfig::new();
        props.set_security_properties(&mut config);

        let map = config.config_map();
        assert_eq!(map.get("sasl.mechanism").unwrap(), "OAUTHBEARER");
        assert_eq!(
            map.get("enable.sasl.oauthbearer.unsecure.jwt").unwrap(),
            "true"
        );
        assert!(!map.contains_key("sasl.oauthbearer.method"));
    }

    #[test]
    fn test_aws_msk_iam_unaffected_by_oidc() {
        let mut props = KafkaConnectionProps::test_default();
        props.sasl_mechanism = Some("AWS_MSK_IAM".to_owned());

        let mut config = rdkafka::ClientConfig::new();
        props.set_security_properties(&mut config);

        let map = config.config_map();
        // MSK IAM takes the early-return path
        assert_eq!(map.get("security.protocol").unwrap(), "SASL_SSL");
        assert_eq!(map.get("sasl.mechanism").unwrap(), "OAUTHBEARER");
        // Should not have any OIDC or unsecure JWT settings
        assert!(!map.contains_key("sasl.oauthbearer.method"));
        assert!(!map.contains_key("enable.sasl.oauthbearer.unsecure.jwt"));
    }

    #[test]
    fn test_oauthbearer_oidc_with_extensions() {
        let mut props = KafkaConnectionProps::test_default();
        props.sasl_mechanism = Some("OAUTHBEARER".to_owned());
        props.sasl_oauthbearer_method = Some("oidc".to_owned());
        props.sasl_oauthbearer_client_id = Some("client".to_owned());
        props.sasl_oauthbearer_client_secret = Some("".to_owned());
        props.sasl_oauthbearer_token_endpoint_url =
            Some("https://idp.example.com/token".to_owned());
        props.sasl_oauthbearer_extensions =
            Some("logicalCluster=lkc-abc,identityPoolId=pool-xyz".to_owned());

        let mut config = rdkafka::ClientConfig::new();
        props.set_security_properties(&mut config);

        let map = config.config_map();
        assert_eq!(
            map.get("sasl.oauthbearer.extensions").unwrap(),
            "logicalCluster=lkc-abc,identityPoolId=pool-xyz"
        );
    }
}
