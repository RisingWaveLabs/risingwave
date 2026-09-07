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

use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use risingwave_pb::common::PbHostAddress;
use thiserror_ext::AsReport;
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tracing::error;

/// General host address and port.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostAddr {
    pub host: String,
    pub port: u16,
}

impl std::fmt::Display for HostAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}
impl From<SocketAddr> for HostAddr {
    fn from(addr: SocketAddr) -> Self {
        HostAddr {
            host: addr.ip().to_string(),
            port: addr.port(),
        }
    }
}

impl TryFrom<&str> for HostAddr {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let s = format!("http://{s}");
        let addr = url::Url::parse(&s).with_context(|| format!("failed to parse address: {s}"))?;
        Ok(HostAddr {
            host: addr.host().context("invalid host")?.to_string(),
            port: addr.port().context("invalid port")?,
        })
    }
}

impl TryFrom<&String> for HostAddr {
    type Error = anyhow::Error;

    fn try_from(s: &String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_str())
    }
}

impl FromStr for HostAddr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl From<&PbHostAddress> for HostAddr {
    fn from(addr: &PbHostAddress) -> Self {
        HostAddr {
            host: addr.get_host().clone(),
            port: addr.get_port() as u16,
        }
    }
}

impl HostAddr {
    pub fn to_protobuf(&self) -> PbHostAddress {
        PbHostAddress {
            host: self.host.clone(),
            port: self.port as i32,
        }
    }
}

pub fn is_local_address(server_addr: &HostAddr, peer_addr: &HostAddr) -> bool {
    server_addr == peer_addr
}

pub async fn try_resolve_dns(host: &str, port: i32) -> Result<SocketAddr, String> {
    let addr = format!("{}:{}", host, port);
    let mut backoff = ExponentialBackoff::from_millis(100)
        .max_delay(Duration::from_secs(3))
        .factor(5);
    const MAX_RETRY: usize = 20;
    for i in 1..=MAX_RETRY {
        let err = match addr.to_socket_addrs() {
            Ok(mut addr_iter) => {
                if let Some(addr) = addr_iter.next() {
                    return Ok(addr);
                } else {
                    format!("{} resolved to no addr", addr)
                }
            }
            Err(e) => e.to_report_string(),
        };
        // It may happen that the dns information of newly registered worker node
        // has not been propagated to the meta node and cause error. Wait for a while and retry
        let delay = backoff.next().unwrap();
        error!(
            attempt = i,
            backoff_delay = ?delay,
            err,
            addr,
            "failed to resolve the worker node address",
        );
        sleep(delay).await;
    }
    Err(format!("failed to resolve dns: {}", addr))
}

#[cfg(test)]
mod tests {
    use crate::util::addr::{HostAddr, is_local_address};

    #[test]
    fn test_is_local_address() {
        let check_local = |a: &str, b: &str, result: bool| {
            assert_eq!(
                is_local_address(&a.parse().unwrap(), &b.parse().unwrap()),
                result
            );
        };
        check_local("localhost:3456", "localhost:3456", true);
        check_local("10.11.12.13:3456", "10.11.12.13:3456", true);
        check_local("some.host.in.k8s:3456", "some.host.in.k8s:3456", true);
        check_local("some.host.in.k8s:3456", "other.host.in.k8s:3456", false);
        check_local("some.host.in.k8s:3456", "some.host.in.k8s:4567", false);
    }

    #[test]
    fn test_host_addr_convert() {
        let addr = "1.2.3.4:567";
        assert_eq!(
            addr.parse::<HostAddr>().unwrap(),
            HostAddr {
                host: String::from("1.2.3.4"),
                port: 567
            }
        );
        let addr = "test.test:12345";
        assert_eq!(
            addr.parse::<HostAddr>().unwrap(),
            HostAddr {
                host: String::from("test.test"),
                port: 12345
            }
        );
        let addr = "test.test";
        assert!(addr.parse::<HostAddr>().is_err());
        let addr = "test.test:65537";
        assert!(addr.parse::<HostAddr>().is_err());
        let addr = "test.test:";
        assert!(addr.parse::<HostAddr>().is_err());
        let addr = "test.test:12345:12345";
        assert!(addr.parse::<HostAddr>().is_err());
    }
}
