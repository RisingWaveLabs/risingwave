// Copyright 2025 RisingWave Labs
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

use std::num::{NonZeroU64, NonZeroUsize, ParseIntError};
use std::str::FromStr;

use risingwave_common::system_param::adaptive_parallelism_strategy::{
    AdaptiveParallelismStrategy, ParallelismStrategyParseError, parse_strategy,
};
use thiserror::Error;

const KEYWORD_DEFAULT: &str = "default";
const KEYWORD_ADAPTIVE: &str = "adaptive";
const KEYWORD_AUTO: &str = "auto";
const KEYWORD_DEFAULT_STRATEGY: &str = "default";

const fn non_zero_u64(parallelism: u64) -> NonZeroU64 {
    match NonZeroU64::new(parallelism) {
        Some(parallelism) => parallelism,
        None => panic!("parallelism must be non-zero"),
    }
}

const fn bounded_parallelism(parallelism: u64) -> ConfigParallelism {
    ConfigParallelism::Bounded(non_zero_u64(parallelism))
}

pub const DEFAULT_GLOBAL_STREAMING_PARALLELISM: ConfigParallelism = bounded_parallelism(64);
pub const DEFAULT_TABLE_SOURCE_STREAMING_PARALLELISM: ConfigParallelism = bounded_parallelism(4);
pub const DEFAULT_SINK_STREAMING_PARALLELISM: ConfigParallelism = bounded_parallelism(8);

#[derive(Copy, Debug, Clone, PartialEq, Default)]
pub enum ConfigParallelism {
    #[default]
    Default,
    Fixed(NonZeroU64),
    Adaptive,
    Bounded(NonZeroU64),
    Ratio(f32),
}

#[derive(Error, Debug)]
pub enum ConfigParallelismParseError {
    #[error("Unsupported parallelism: {0}")]
    UnsupportedParallelism(String),

    #[error("Parse error: {0}")]
    ParseIntError(#[from] ParseIntError),

    #[error(transparent)]
    StrategyParseError(#[from] ParallelismStrategyParseError),
}

impl FromStr for ConfigParallelism {
    type Err = ConfigParallelismParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            KEYWORD_DEFAULT => return Ok(ConfigParallelism::Default),
            KEYWORD_ADAPTIVE | KEYWORD_AUTO => return Ok(ConfigParallelism::Adaptive),
            _ => {}
        }

        match parse_strategy(s) {
            Ok(AdaptiveParallelismStrategy::Auto | AdaptiveParallelismStrategy::Full) => {
                Ok(ConfigParallelism::Adaptive)
            }
            Ok(AdaptiveParallelismStrategy::Bounded(n)) => Ok(ConfigParallelism::Bounded(
                NonZeroU64::new(n.get() as u64).unwrap(),
            )),
            Ok(AdaptiveParallelismStrategy::Ratio(r)) => Ok(ConfigParallelism::Ratio(r)),
            Err(ParallelismStrategyParseError::UnsupportedStrategy(_)) => {
                let parsed = s.parse::<u64>()?;
                if parsed == 0 {
                    Ok(ConfigParallelism::Adaptive)
                } else {
                    Ok(ConfigParallelism::Fixed(NonZeroU64::new(parsed).unwrap()))
                }
            }
            Err(err) => Err(err.into()),
        }
    }
}

impl std::fmt::Display for ConfigParallelism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ConfigParallelism::Adaptive => {
                write!(f, "{}", KEYWORD_ADAPTIVE)
            }
            ConfigParallelism::Default => {
                write!(f, "{}", KEYWORD_DEFAULT)
            }
            ConfigParallelism::Fixed(n) => {
                write!(f, "{}", n)
            }
            ConfigParallelism::Bounded(n) => {
                write!(f, "bounded({})", n)
            }
            ConfigParallelism::Ratio(r) => {
                write!(f, "ratio({})", r)
            }
        }
    }
}

impl ConfigParallelism {
    pub fn adaptive_strategy(self) -> Option<AdaptiveParallelismStrategy> {
        match self {
            Self::Default | Self::Fixed(_) => None,
            Self::Adaptive => Some(AdaptiveParallelismStrategy::Auto),
            Self::Bounded(n) => Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(n.get() as usize).unwrap(),
            )),
            Self::Ratio(r) => Some(AdaptiveParallelismStrategy::Ratio(r)),
        }
    }
}

#[derive(Copy, Default, Debug, Clone, PartialEq)]
pub enum ConfigBackfillParallelism {
    #[default]
    Default,
    Fixed(NonZeroU64),
    Adaptive,
    Bounded(NonZeroU64),
    Ratio(f32),
}

impl FromStr for ConfigBackfillParallelism {
    type Err = ConfigParallelismParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            KEYWORD_DEFAULT => return Ok(Self::Default),
            KEYWORD_ADAPTIVE | KEYWORD_AUTO => return Ok(Self::Adaptive),
            _ => {}
        }

        match parse_strategy(s) {
            Ok(AdaptiveParallelismStrategy::Auto | AdaptiveParallelismStrategy::Full) => {
                Ok(Self::Adaptive)
            }
            Ok(AdaptiveParallelismStrategy::Bounded(n)) => {
                Ok(Self::Bounded(NonZeroU64::new(n.get() as u64).unwrap()))
            }
            Ok(AdaptiveParallelismStrategy::Ratio(r)) => Ok(Self::Ratio(r)),
            Err(ParallelismStrategyParseError::UnsupportedStrategy(_)) => {
                let parsed = s.parse::<u64>()?;
                if parsed == 0 {
                    Ok(Self::Adaptive)
                } else {
                    Ok(Self::Fixed(NonZeroU64::new(parsed).unwrap()))
                }
            }
            Err(err) => Err(err.into()),
        }
    }
}

impl std::fmt::Display for ConfigBackfillParallelism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Adaptive => write!(f, "{}", KEYWORD_ADAPTIVE),
            Self::Default => write!(f, "{}", KEYWORD_DEFAULT),
            Self::Fixed(n) => write!(f, "{}", n),
            Self::Bounded(n) => write!(f, "bounded({})", n),
            Self::Ratio(r) => write!(f, "ratio({})", r),
        }
    }
}

impl ConfigBackfillParallelism {
    pub fn adaptive_strategy(self) -> Option<AdaptiveParallelismStrategy> {
        match self {
            Self::Default | Self::Fixed(_) => None,
            Self::Adaptive => Some(AdaptiveParallelismStrategy::Auto),
            Self::Bounded(n) => Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(n.get() as usize).unwrap(),
            )),
            Self::Ratio(r) => Some(AdaptiveParallelismStrategy::Ratio(r)),
        }
    }
}

#[derive(Copy, Default, Debug, Clone, PartialEq)]
pub enum ConfigAdaptiveParallelismStrategy {
    #[default]
    Default,
    Auto,
    Full,
    Bounded(NonZeroU64),
    Ratio(f32),
}

impl FromStr for ConfigAdaptiveParallelismStrategy {
    type Err = ParallelismStrategyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case(KEYWORD_DEFAULT_STRATEGY) {
            return Ok(Self::Default);
        }

        let strategy = parse_strategy(s)?;
        Ok(strategy.into())
    }
}

impl From<AdaptiveParallelismStrategy> for ConfigAdaptiveParallelismStrategy {
    fn from(value: AdaptiveParallelismStrategy) -> Self {
        match value {
            AdaptiveParallelismStrategy::Auto => Self::Auto,
            AdaptiveParallelismStrategy::Full => Self::Full,
            AdaptiveParallelismStrategy::Bounded(n) => {
                // Safe to unwrap since `n` is non-zero.
                Self::Bounded(NonZeroU64::new(n.get() as u64).unwrap())
            }
            AdaptiveParallelismStrategy::Ratio(r) => Self::Ratio(r),
        }
    }
}

impl From<ConfigAdaptiveParallelismStrategy> for Option<AdaptiveParallelismStrategy> {
    fn from(value: ConfigAdaptiveParallelismStrategy) -> Self {
        match value {
            ConfigAdaptiveParallelismStrategy::Default => None,
            ConfigAdaptiveParallelismStrategy::Auto => Some(AdaptiveParallelismStrategy::Auto),
            ConfigAdaptiveParallelismStrategy::Full => Some(AdaptiveParallelismStrategy::Full),
            ConfigAdaptiveParallelismStrategy::Bounded(n) => {
                Some(AdaptiveParallelismStrategy::Bounded(
                    NonZeroUsize::new(n.get() as usize)
                        // Bounded strategy requires non-zero; `NonZeroU64` guarantees this.
                        .unwrap(),
                ))
            }
            ConfigAdaptiveParallelismStrategy::Ratio(r) => {
                Some(AdaptiveParallelismStrategy::Ratio(r))
            }
        }
    }
}

impl std::fmt::Display for ConfigAdaptiveParallelismStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigAdaptiveParallelismStrategy::Default => {
                write!(f, "{}", KEYWORD_DEFAULT_STRATEGY)
            }
            ConfigAdaptiveParallelismStrategy::Auto => AdaptiveParallelismStrategy::Auto.fmt(f),
            ConfigAdaptiveParallelismStrategy::Full => AdaptiveParallelismStrategy::Full.fmt(f),
            ConfigAdaptiveParallelismStrategy::Bounded(n) => {
                AdaptiveParallelismStrategy::Bounded(NonZeroUsize::new(n.get() as usize).unwrap())
                    .fmt(f)
            }
            ConfigAdaptiveParallelismStrategy::Ratio(r) => {
                AdaptiveParallelismStrategy::Ratio(*r).fmt(f)
            }
        }
    }
}

pub fn migrate_legacy_global_parallelism(
    parallelism: ConfigParallelism,
    strategy: ConfigAdaptiveParallelismStrategy,
    system_strategy: AdaptiveParallelismStrategy,
) -> ConfigParallelism {
    match parallelism {
        ConfigParallelism::Fixed(_)
        | ConfigParallelism::Bounded(_)
        | ConfigParallelism::Ratio(_) => parallelism,
        ConfigParallelism::Default | ConfigParallelism::Adaptive => {
            legacy_strategy_to_parallelism(resolve_legacy_strategy(strategy, system_strategy))
        }
    }
}

pub fn migrate_legacy_type_parallelism(
    specific_parallelism: ConfigParallelism,
    specific_strategy: ConfigAdaptiveParallelismStrategy,
    global_parallelism: ConfigParallelism,
    global_strategy: ConfigAdaptiveParallelismStrategy,
    system_strategy: AdaptiveParallelismStrategy,
) -> ConfigParallelism {
    match specific_parallelism {
        ConfigParallelism::Fixed(_)
        | ConfigParallelism::Bounded(_)
        | ConfigParallelism::Ratio(_) => specific_parallelism,
        ConfigParallelism::Adaptive => legacy_strategy_to_parallelism(resolve_legacy_strategy(
            specific_strategy,
            resolve_legacy_strategy(global_strategy, system_strategy),
        )),
        ConfigParallelism::Default => {
            if matches!(
                specific_strategy,
                ConfigAdaptiveParallelismStrategy::Default
            ) || matches!(global_parallelism, ConfigParallelism::Fixed(_))
            {
                ConfigParallelism::Default
            } else {
                legacy_strategy_to_parallelism(resolve_legacy_strategy(
                    specific_strategy,
                    resolve_legacy_strategy(global_strategy, system_strategy),
                ))
            }
        }
    }
}

fn resolve_legacy_strategy(
    strategy: ConfigAdaptiveParallelismStrategy,
    fallback: AdaptiveParallelismStrategy,
) -> AdaptiveParallelismStrategy {
    Option::<AdaptiveParallelismStrategy>::from(strategy).unwrap_or(fallback)
}

fn legacy_strategy_to_parallelism(strategy: AdaptiveParallelismStrategy) -> ConfigParallelism {
    match strategy {
        AdaptiveParallelismStrategy::Auto | AdaptiveParallelismStrategy::Full => {
            ConfigParallelism::Adaptive
        }
        AdaptiveParallelismStrategy::Bounded(n) => {
            ConfigParallelism::Bounded(NonZeroU64::new(n.get() as u64).unwrap())
        }
        AdaptiveParallelismStrategy::Ratio(r) => ConfigParallelism::Ratio(r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallelism_parse_bounded() {
        let parallelism: ConfigParallelism = "Bounded(4)".parse().unwrap();
        assert_eq!(
            parallelism,
            ConfigParallelism::Bounded(NonZeroU64::new(4).unwrap())
        );
    }

    #[test]
    fn test_parallelism_parse_ratio() {
        let parallelism: ConfigParallelism = "ratio(0.5)".parse().unwrap();
        assert_eq!(parallelism, ConfigParallelism::Ratio(0.5));
    }

    #[test]
    fn test_backfill_parallelism_parse_bounded() {
        let parallelism: ConfigBackfillParallelism = "Bounded(4)".parse().unwrap();
        assert_eq!(
            parallelism,
            ConfigBackfillParallelism::Bounded(NonZeroU64::new(4).unwrap())
        );
        assert_eq!(parallelism.to_string(), "bounded(4)");
        assert_eq!(
            parallelism.adaptive_strategy(),
            Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(4).unwrap()
            ))
        );
    }

    #[test]
    fn test_backfill_parallelism_parse_ratio() {
        let parallelism: ConfigBackfillParallelism = "ratio(0.5)".parse().unwrap();
        assert_eq!(parallelism, ConfigBackfillParallelism::Ratio(0.5));
        assert_eq!(parallelism.to_string(), "ratio(0.5)");
        assert_eq!(
            parallelism.adaptive_strategy(),
            Some(AdaptiveParallelismStrategy::Ratio(0.5))
        );
    }

    #[test]
    fn test_strategy_parse_default() {
        assert_eq!(
            "default"
                .parse::<ConfigAdaptiveParallelismStrategy>()
                .unwrap(),
            ConfigAdaptiveParallelismStrategy::Default
        );
    }

    #[test]
    fn test_strategy_parse_ratio() {
        let strategy: ConfigAdaptiveParallelismStrategy = "Ratio(0.5)".parse().unwrap();
        assert_eq!(strategy, ConfigAdaptiveParallelismStrategy::Ratio(0.5));
    }

    #[test]
    fn test_strategy_into_option() {
        let opt: Option<AdaptiveParallelismStrategy> =
            ConfigAdaptiveParallelismStrategy::Full.into();
        assert_eq!(opt, Some(AdaptiveParallelismStrategy::Full));
    }

    #[test]
    fn test_migrate_legacy_global_parallelism() {
        assert_eq!(
            migrate_legacy_global_parallelism(
                ConfigParallelism::Default,
                ConfigAdaptiveParallelismStrategy::Ratio(0.5),
                AdaptiveParallelismStrategy::Bounded(NonZeroUsize::new(64).unwrap()),
            ),
            ConfigParallelism::Ratio(0.5)
        );
    }

    #[test]
    fn test_migrate_legacy_type_parallelism() {
        assert_eq!(
            migrate_legacy_type_parallelism(
                ConfigParallelism::Default,
                ConfigAdaptiveParallelismStrategy::Bounded(NonZeroU64::new(4).unwrap()),
                ConfigParallelism::Adaptive,
                ConfigAdaptiveParallelismStrategy::Default,
                AdaptiveParallelismStrategy::Auto,
            ),
            ConfigParallelism::Bounded(NonZeroU64::new(4).unwrap())
        );
        assert_eq!(
            migrate_legacy_type_parallelism(
                ConfigParallelism::Default,
                ConfigAdaptiveParallelismStrategy::Bounded(NonZeroU64::new(4).unwrap()),
                ConfigParallelism::Fixed(NonZeroU64::new(8).unwrap()),
                ConfigAdaptiveParallelismStrategy::Default,
                AdaptiveParallelismStrategy::Auto,
            ),
            ConfigParallelism::Default
        );
    }
}
