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

use std::time::Duration;

use tokio_retry::strategy::ExponentialBackoff;

/// Creates a backoff starting at `initial_delay`, multiplying subsequent delays by
/// `multiplier`, and capping every delay at `max_delay`.
///
/// This wraps [`ExponentialBackoff`], whose `from_millis` argument is the mathematical
/// base rather than the initial delay.
pub fn exponential_backoff(
    initial_delay: Duration,
    multiplier: u64,
    max_delay: Duration,
) -> impl Iterator<Item = Duration> + Clone {
    let initial_delay = initial_delay.min(max_delay);
    let initial_delay_ms = initial_delay.as_millis().try_into().unwrap_or(u64::MAX);

    // Keep the confusing low-level API contained in this wrapper.
    #[expect(
        clippy::disallowed_methods,
        reason = "this wrapper is the only permitted caller of the low-level API"
    )]
    let remaining_delays = ExponentialBackoff::from_millis(multiplier)
        .factor(initial_delay_ms)
        .max_delay(max_delay);

    std::iter::once(initial_delay).chain(remaining_delays)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_initial_delay_and_uses_multiplier() {
        let delays = exponential_backoff(Duration::from_millis(101), 3, Duration::from_secs(10))
            .take(4)
            .collect::<Vec<_>>();

        assert_eq!(
            delays,
            [
                Duration::from_millis(101),
                Duration::from_millis(303),
                Duration::from_millis(909),
                Duration::from_millis(2727),
            ]
        );
    }

    #[test]
    fn caps_all_delays_at_max_delay() {
        let delays = exponential_backoff(Duration::from_secs(1), 2, Duration::from_secs(10))
            .take(6)
            .collect::<Vec<_>>();

        assert_eq!(
            delays,
            [
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(10),
                Duration::from_secs(10),
            ]
        );
    }
}
