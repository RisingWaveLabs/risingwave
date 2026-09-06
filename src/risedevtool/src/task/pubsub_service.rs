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

use super::docker_service::{DockerService, DockerServiceConfig};
use crate::PubsubConfig;

impl DockerServiceConfig for PubsubConfig {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn is_user_managed(&self) -> bool {
        self.user_managed
    }

    fn image(&self) -> String {
        self.image.clone()
    }

    fn args(&self) -> Vec<String> {
        vec![
            "gcloud".to_owned(),
            "beta".to_owned(),
            "emulators".to_owned(),
            "pubsub".to_owned(),
            "start".to_owned(),
            "--project=demo".to_owned(),
            "--host-port=0.0.0.0:8900".to_owned(),
        ]
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![(self.port.to_string(), "8900".to_owned())]
    }
}

/// Docker-backed Google Pub/Sub emulator service.
pub type PubsubService = DockerService<PubsubConfig>;
