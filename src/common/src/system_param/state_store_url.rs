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

use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct StateStoreUrl<T = String>(T);

pub type StateStoreUrlRef<'a> = StateStoreUrl<&'a str>;

impl<T> From<T> for StateStoreUrl<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T: AsRef<str>> StateStoreUrl<T> {
    /// Returns the original URL, including credentials.
    pub fn expose(&self) -> &str {
        self.0.as_ref()
    }
}

impl<A, B> PartialEq<StateStoreUrl<B>> for StateStoreUrl<A>
where
    A: AsRef<str>,
    B: AsRef<str>,
{
    fn eq(&self, other: &StateStoreUrl<B>) -> bool {
        self.expose() == other.expose()
    }
}

impl<T: AsRef<str>> Eq for StateStoreUrl<T> {}

impl<T: AsRef<str>> fmt::Display for StateStoreUrl<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_redacted_state_store_url(f, self.expose())
    }
}

impl<T: AsRef<str>> fmt::Debug for StateStoreUrl<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl<T: AsRef<str>> Serialize for StateStoreUrl<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl FromStr for StateStoreUrl {
    type Err = Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.to_owned()))
    }
}

impl<T: AsRef<str>> From<StateStoreUrl<T>> for String {
    fn from(value: StateStoreUrl<T>) -> Self {
        value.expose().to_owned()
    }
}

impl From<StateStoreUrlRef<'_>> for StateStoreUrl {
    fn from(value: StateStoreUrlRef<'_>) -> Self {
        Self(value.expose().to_owned())
    }
}

fn write_redacted_state_store_url(f: &mut fmt::Formatter<'_>, url: &str) -> fmt::Result {
    let Some(scheme_end) = url.find("://") else {
        return f.write_str(url);
    };

    let authority_start = scheme_end + 3;
    let authority_end = url[authority_start..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |offset| authority_start + offset);

    let authority = &url[authority_start..authority_end];
    let Some(user_info_end) = authority.rfind('@') else {
        return f.write_str(url);
    };

    let user_info = &authority[..user_info_end];

    f.write_str(&url[..authority_start])?;

    if user_info.contains(':') {
        f.write_str("****:****")?;
    } else {
        f.write_str("****")?;
    }

    f.write_str(&authority[user_info_end..])?;
    f.write_str(&url[authority_end..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_credentials() {
        let url: StateStoreUrl = "hummock+minio://admin:secret@localhost:9000/bucket"
            .parse()
            .unwrap();

        assert_eq!(
            url.to_string(),
            "hummock+minio://****:****@localhost:9000/bucket"
        );
        assert_eq!(
            url.expose(),
            "hummock+minio://admin:secret@localhost:9000/bucket"
        );
    }

    #[test]
    fn test_url_without_credentials_is_unchanged() {
        let url: StateStoreUrl = "hummock+s3://bucket-name".parse().unwrap();

        assert_eq!(url.to_string(), "hummock+s3://bucket-name");
    }

    #[test]
    fn test_redact_username_without_password() {
        let url: StateStoreUrl = "hummock+minio://admin@localhost/bucket".parse().unwrap();

        assert_eq!(url.to_string(), "hummock+minio://****@localhost/bucket");
    }

    #[test]
    fn test_redact_encoded_credentials() {
        let url: StateStoreUrl = "hummock+minio://user:p%40ss@localhost/bucket"
            .parse()
            .unwrap();

        assert_eq!(
            url.to_string(),
            "hummock+minio://****:****@localhost/bucket"
        );
    }
}
