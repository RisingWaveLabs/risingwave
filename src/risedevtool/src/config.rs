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

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Result, anyhow, bail};
use itertools::Itertools;
use yaml_rust::{Yaml, YamlEmitter, YamlLoader};

use crate::ServiceConfig;

mod dollar_expander;
mod id_expander;
mod provide_expander;
mod use_expander;
use dollar_expander::DollarExpander;
use id_expander::IdExpander;
use provide_expander::ProvideExpander;
use use_expander::UseExpander;

/// The main configuration file name.
pub const RISEDEV_CONFIG_FILE: &str = "risedev.yml";
/// The extra user profiles file name.
pub const RISEDEV_USER_PROFILES_FILE: &str = "risedev-profiles.user.yml";

pub struct ConfigExpander;

impl ConfigExpander {
    /// Load a single document YAML file.
    fn load_yaml(path: impl AsRef<Path>) -> Result<Yaml> {
        let path = path.as_ref();
        let content = fs_err::read_to_string(path)?;
        let [config]: [_; 1] = YamlLoader::load_from_str(&content)?
            .try_into()
            .map_err(|_| anyhow!("expect `{}` to have only one section", path.display()))?;
        Ok(config)
    }

    /// Transforms `risedev.yml` and `risedev-profiles.user.yml` to a fully expanded yaml file.
    ///
    /// Format:
    ///
    /// ```yaml
    /// my-profile:
    ///   config-path: src/config/ci-recovery.toml
    ///   env:
    ///     RUST_LOG: "info,risingwave_storage::hummock=off"
    ///     ENABLE_PRETTY_LOG: "true"
    ///   steps:
    ///     - use: minio
    ///     - use: sqlite
    ///     - use: meta-node
    ///       meta-backend: sqlite
    ///     - use: compute-node
    ///       parallelism: 1
    ///     - use: frontend
    /// ```
    ///
    /// # Arguments
    ///
    /// * `root` is the root directory of these YAML files.
    /// * `profile` is the selected config profile called by `risedev dev <profile>`. It is one of
    ///   the keys in the `profile` section.
    ///
    /// # Returns
    ///
    /// `(config_path, env, steps)`
    pub fn expand(
        root: impl AsRef<Path>,
        profile: &str,
    ) -> Result<(Option<String>, Vec<String>, Yaml)> {
        Self::expand_with_extra_info(root, profile, HashMap::new())
    }

    /// See [`ConfigExpander::expand`] for other information.
    ///
    /// # Arguments
    ///
    /// - `extra_info` is additional variables for variable expansion by [`DollarExpander`].
    pub fn expand_with_extra_info(
        root: impl AsRef<Path>,
        profile: &str,
        extra_info: HashMap<String, String>,
    ) -> Result<(Option<String>, Vec<String>, Yaml)> {
        let global_path = root.as_ref().join(RISEDEV_CONFIG_FILE);
        let global_yaml = Self::load_yaml(global_path)?;
        let global_config = global_yaml
            .as_hash()
            .ok_or_else(|| anyhow!("expect config to be a hashmap"))?;

        let all_profile_section = {
            let mut all = global_config
                .get(&Yaml::String("profile".to_owned()))
                .ok_or_else(|| anyhow!("expect `profile` section"))?
                .as_hash()
                .ok_or_else(|| anyhow!("expect `profile` section to be a hashmap"))?
                .to_owned();

            // Add user profiles if exists.
            let user_profiles_path = root.as_ref().join(RISEDEV_USER_PROFILES_FILE);
            if user_profiles_path.is_file() {
                let yaml = Self::load_yaml(user_profiles_path)?;
                let map = yaml.as_hash().ok_or_else(|| {
                    anyhow!("expect `{RISEDEV_USER_PROFILES_FILE}` to be a hashmap")
                })?;
                for (k, v) in map {
                    if all.insert(k.clone(), v.clone()).is_some() {
                        bail!(
                            "find duplicated config key `{k:?}` in `{RISEDEV_USER_PROFILES_FILE}`"
                        );
                    }
                }
            }

            all
        };

        let template_section = global_config
            .get(&Yaml::String("template".to_owned()))
            .ok_or_else(|| anyhow!("expect `profile` section"))?;

        let profile_section = all_profile_section
            .get(&Yaml::String(profile.to_owned()))
            .ok_or_else(|| anyhow!("profile '{}' not found", profile))?
            .as_hash()
            .ok_or_else(|| anyhow!("expect `profile` section to be a hashmap"))?;

        let config_path = profile_section
            .get(&Yaml::String("config-path".to_owned()))
            .and_then(|s| s.as_str())
            .map(|s| s.to_owned());
        let mut env = vec![];
        if let Some(env_section) = profile_section.get(&Yaml::String("env".to_owned())) {
            let env_section = env_section
                .as_hash()
                .ok_or_else(|| anyhow!("expect `env` section to be a hashmap"))?;

            for (k, v) in env_section {
                let key = k
                    .as_str()
                    .ok_or_else(|| anyhow!("expect env key to be a string"))?;
                let value = v
                    .as_str()
                    .ok_or_else(|| anyhow!("expect env value to be a string"))?;
                env.push(format!("{}={}", key, value));
            }
        }

        let steps = profile_section
            .get(&Yaml::String("steps".to_owned()))
            .ok_or_else(|| anyhow!("expect `steps` section"))?
            .clone();

        let steps = UseExpander::new(template_section)?.visit(steps)?;
        let steps = DollarExpander::new(extra_info).visit(steps)?;
        let steps = IdExpander::new(&steps)?.visit(steps)?;
        let steps = ProvideExpander::new(&steps)?.visit(steps)?;

        Ok((config_path, env, steps))
    }

    /// Parses the expanded yaml into [`ServiceConfig`]s.
    /// The order is the same as the original array's order.
    pub fn deserialize(expanded_config: &Yaml) -> Result<Vec<ServiceConfig>> {
        let steps = expanded_config
            .as_vec()
            .ok_or_else(|| anyhow!("expect steps to be an array"))?;
        let config: Vec<ServiceConfig> = steps
            .iter()
            .map(|step| {
                let use_type = step
                    .as_hash()
                    .ok_or_else(|| anyhow!("expect step to be a hashmap"))?;
                let use_type = use_type
                    .get(&Yaml::String("use".to_owned()))
                    .ok_or_else(|| anyhow!("expect `use` in step"))?;
                let use_type = use_type
                    .as_str()
                    .ok_or_else(|| anyhow!("expect `use` to be a string"))?
                    .to_owned();
                let mut out_str = String::new();
                let mut emitter = YamlEmitter::new(&mut out_str);
                emitter.dump(step)?;
                let result = match use_type.as_str() {
                    "minio" => ServiceConfig::Minio(serde_yaml::from_str(&out_str)?),
                    "sqlite" => ServiceConfig::Sqlite(serde_yaml::from_str(&out_str)?),
                    "frontend" => ServiceConfig::Frontend(serde_yaml::from_str(&out_str)?),
                    "compactor" => ServiceConfig::Compactor(serde_yaml::from_str(&out_str)?),
                    "compute-node" => ServiceConfig::ComputeNode(serde_yaml::from_str(&out_str)?),
                    "meta-node" => ServiceConfig::MetaNode(serde_yaml::from_str(&out_str)?),
                    "prometheus" => ServiceConfig::Prometheus(serde_yaml::from_str(&out_str)?),
                    "grafana" => ServiceConfig::Grafana(serde_yaml::from_str(&out_str)?),
                    "tempo" => ServiceConfig::Tempo(serde_yaml::from_str(&out_str)?),
                    "opendal" => ServiceConfig::Opendal(serde_yaml::from_str(&out_str)?),
                    "aws-s3" => ServiceConfig::AwsS3(serde_yaml::from_str(&out_str)?),
                    "moto" => ServiceConfig::Moto(serde_yaml::from_str(&out_str)?),
                    "kafka" => ServiceConfig::Kafka(serde_yaml::from_str(&out_str)?),
                    "lakekeeper" => ServiceConfig::Lakekeeper(serde_yaml::from_str(&out_str)?),
                    "pubsub" => ServiceConfig::Pubsub(serde_yaml::from_str(&out_str)?),
                    "pulsar" => ServiceConfig::Pulsar(serde_yaml::from_str(&out_str)?),
                    "redis" => ServiceConfig::Redis(serde_yaml::from_str(&out_str)?),
                    "starrocks" => ServiceConfig::Starrocks(serde_yaml::from_str(&out_str)?),
                    "mysql" => ServiceConfig::MySql(serde_yaml::from_str(&out_str)?),
                    "postgres" => ServiceConfig::Postgres(serde_yaml::from_str(&out_str)?),
                    "sqlserver" => ServiceConfig::SqlServer(serde_yaml::from_str(&out_str)?),
                    "mongodb" => ServiceConfig::MongoDb(serde_yaml::from_str(&out_str)?),
                    "elasticsearch" => {
                        ServiceConfig::ElasticSearch(serde_yaml::from_str(&out_str)?)
                    }
                    "opensearch" => ServiceConfig::OpenSearch(serde_yaml::from_str(&out_str)?),
                    "nats" => ServiceConfig::Nats(serde_yaml::from_str(&out_str)?),
                    "mqtt" => ServiceConfig::Mqtt(serde_yaml::from_str(&out_str)?),
                    "schema-registry" => {
                        ServiceConfig::SchemaRegistry(serde_yaml::from_str(&out_str)?)
                    }
                    "moat" => ServiceConfig::Moat(serde_yaml::from_str(&out_str)?),
                    other => return Err(anyhow!("unsupported use type: {}", other)),
                };
                Ok(result)
            })
            .try_collect()?;

        let mut services = HashMap::new();
        for x in &config {
            let id = x.id().to_owned();
            if services.insert(id.clone(), x).is_some() {
                return Err(anyhow!("duplicate id: {}", id));
            }
        }
        Ok(config)
    }
}
