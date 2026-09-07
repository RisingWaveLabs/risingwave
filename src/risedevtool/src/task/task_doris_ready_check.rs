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

use std::io::Write;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, ensure};

use super::{ExecuteContext, Task};
use crate::wait::wait;
use crate::{DORIS_CONTAINER_HOST, DORIS_CONTAINER_QUERY_PORT, DorisConfig};

pub struct DorisReadyCheckTask {
    config: DorisConfig,
}

impl DorisReadyCheckTask {
    pub fn new(config: DorisConfig) -> Self {
        Self { config }
    }
}

fn backend_is_alive(output: &str) -> bool {
    let mut lines = output.lines();
    let Some(alive_column) = lines
        .next()
        .and_then(|header| header.split('\t').position(|name| name == "Alive"))
    else {
        return false;
    };

    lines.any(|line| {
        line.split('\t')
            .nth(alive_column)
            .is_some_and(|value| value.eq_ignore_ascii_case("true") || value == "1")
    })
}

impl Task for DorisReadyCheckTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl Write>) -> Result<()> {
        let Some(id) = ctx.id.clone() else {
            panic!("Service should be set before executing DorisReadyCheckTask");
        };

        ctx.pb.set_message("waiting for FE and BE ready...");

        wait(
            || {
                let mut command = if self.config.user_managed {
                    let mut command = Command::new("mysql");
                    command
                        .arg("-h")
                        .arg(&self.config.address)
                        .arg("-P")
                        .arg(self.config.query_port.to_string())
                        .arg("-u")
                        .arg(&self.config.admin_user);
                    if !self.config.admin_password.is_empty() {
                        command.env("MYSQL_PWD", &self.config.admin_password);
                    }
                    command
                } else {
                    let mut command = Command::new("docker");
                    command.arg("exec");
                    if !self.config.admin_password.is_empty() {
                        command
                            .arg("-e")
                            .arg(format!("MYSQL_PWD={}", self.config.admin_password));
                    }
                    command
                        .arg(format!("risedev-{}", self.config.id))
                        .arg("mysql")
                        .arg("-h")
                        .arg(DORIS_CONTAINER_HOST)
                        .arg("-P")
                        .arg(DORIS_CONTAINER_QUERY_PORT.to_string())
                        .arg("-u")
                        .arg(&self.config.admin_user);
                    command
                };

                let output = command
                    .args(["-B", "--column-names", "-e", "SHOW BACKENDS"])
                    .output()
                    .context("failed to query Doris backends")?;
                ensure!(
                    output.status.success(),
                    "failed to query Doris backends: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
                ensure!(
                    backend_is_alive(&String::from_utf8_lossy(&output.stdout)),
                    "Doris BE is not alive yet"
                );
                Ok(())
            },
            &mut ctx.log,
            ctx.status_file.as_ref().unwrap(),
            &id,
            Some(Duration::from_secs(120)),
            !self.config.user_managed,
        )
        .with_context(|| format!("failed to wait for service `{id}` to be ready"))?;

        ctx.complete_spin();
        Ok(())
    }
}
