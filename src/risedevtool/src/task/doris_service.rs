// Copyright 2026 RisingWave Labs
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
use crate::DorisConfig;

pub const DORIS_CONTAINER_HOST: &str = "127.0.0.1";
pub const DORIS_CONTAINER_HTTP_PORT: u16 = 8030;
pub const DORIS_CONTAINER_BE_HTTP_PORT: u16 = 8040;
pub const DORIS_CONTAINER_QUERY_PORT: u16 = 9030;

impl DockerServiceConfig for DorisConfig {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn is_user_managed(&self) -> bool {
        self.user_managed
    }

    fn image(&self) -> String {
        self.image.clone()
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![
            (
                self.http_port.to_string(),
                DORIS_CONTAINER_HTTP_PORT.to_string(),
            ),
            // Doris includes the BE HTTP port in stream-load redirects. The port therefore
            // cannot be remapped by Docker without making the redirected URL unreachable.
            (
                DORIS_CONTAINER_BE_HTTP_PORT.to_string(),
                DORIS_CONTAINER_BE_HTTP_PORT.to_string(),
            ),
            (
                self.query_port.to_string(),
                DORIS_CONTAINER_QUERY_PORT.to_string(),
            ),
        ]
    }
}

/// Docker-backed Doris service.
pub type DorisService = DockerService<DorisConfig>;
