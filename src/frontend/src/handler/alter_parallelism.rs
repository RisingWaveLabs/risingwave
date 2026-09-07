// Copyright 2023 RisingWave Labs
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

use pgwire::pg_response::StatementType;
use risingwave_common::session_config::parallelism::ConfigParallelism;
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_pb::meta::table_parallelism::{
    AdaptiveParallelism, FixedParallelism, PbParallelism,
};
use risingwave_pb::meta::{PbTableParallelism, TableParallelism};
use risingwave_sqlparser::ast::{ObjectName, SetVariableValue, SetVariableValueSingle, Value};
use risingwave_sqlparser::keywords::Keyword;
use thiserror_ext::AsReport;

use super::alter_utils::resolve_streaming_job_id_for_alter;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::FragmentId;
use crate::error::{ErrorCode, Result};
use crate::handler::util::{LongRunningNotificationAction, execute_with_long_running_notification};

pub async fn handle_alter_parallelism(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    parallelism: SetVariableValue,
    stmt_type: StatementType,
    deferred: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let job_id = resolve_streaming_job_id_for_alter(&session, obj_name, stmt_type, "parallelism")?;

    let (target_parallelism, adaptive_parallelism_strategy) =
        extract_table_parallelism(parallelism)?;

    let mut builder = RwPgResponse::builder(stmt_type);

    let catalog_writer = session.catalog_writer()?;
    execute_with_long_running_notification(
        catalog_writer.alter_parallelism(
            job_id,
            target_parallelism,
            adaptive_parallelism_strategy,
            deferred,
        ),
        &session,
        "ALTER PARALLELISM",
        LongRunningNotificationAction::SuggestRecover,
    )
    .await?;

    if deferred {
        builder = builder.notice("DEFERRED is used, please ensure that automatic parallelism control is enabled on the meta, otherwise, the alter will not take effect.".to_owned());
    }

    Ok(builder.into())
}

pub async fn handle_alter_backfill_parallelism(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    parallelism: SetVariableValue,
    stmt_type: StatementType,
    deferred: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let job_id =
        resolve_streaming_job_id_for_alter(&session, obj_name, stmt_type, "backfill_parallelism")?;

    let (target_parallelism, adaptive_parallelism_strategy) =
        extract_backfill_parallelism(parallelism)?;

    let mut builder = RwPgResponse::builder(stmt_type);

    let catalog_writer = session.catalog_writer()?;
    execute_with_long_running_notification(
        catalog_writer.alter_backfill_parallelism(
            job_id,
            target_parallelism,
            adaptive_parallelism_strategy,
            deferred,
        ),
        &session,
        "ALTER BACKFILL PARALLELISM",
        LongRunningNotificationAction::SuggestRecover,
    )
    .await?;

    if deferred {
        builder = builder.notice("DEFERRED is used, please ensure that automatic parallelism control is enabled on the meta, otherwise, the alter will not take effect.".to_owned());
    }

    Ok(builder.into())
}

pub async fn handle_alter_fragment_parallelism(
    handler_args: HandlerArgs,
    fragment_ids: Vec<FragmentId>,
    parallelism: SetVariableValue,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let target_parallelism = extract_fragment_parallelism(parallelism)?;

    session
        .env()
        .meta_client()
        .alter_fragment_parallelism(fragment_ids, target_parallelism)
        .await?;

    Ok(RwPgResponse::builder(StatementType::ALTER_FRAGMENT).into())
}

fn extract_table_parallelism(
    parallelism: SetVariableValue,
) -> Result<(TableParallelism, Option<AdaptiveParallelismStrategy>)> {
    extract_job_parallelism(parallelism)
}

fn extract_backfill_parallelism(
    parallelism: SetVariableValue,
) -> Result<(
    Option<TableParallelism>,
    Option<AdaptiveParallelismStrategy>,
)> {
    match parallelism {
        SetVariableValue::Default => Ok((None, None)),
        other => {
            let (parallelism, strategy) = extract_job_parallelism(other)?;
            Ok((Some(parallelism), strategy))
        }
    }
}

fn extract_job_parallelism(
    parallelism: SetVariableValue,
) -> Result<(TableParallelism, Option<AdaptiveParallelismStrategy>)> {
    let adaptive_parallelism = PbTableParallelism {
        parallelism: Some(PbParallelism::Adaptive(AdaptiveParallelism {})),
    };

    let value = parse_single_parallelism_value(parallelism)?;
    let config_parallelism = value.parse::<ConfigParallelism>().map_err(|e| {
        ErrorCode::InvalidInputSyntax(format!(
            "target parallelism must be a valid number, adaptive, bounded(n), or ratio(r): {}",
            e.as_report()
        ))
    })?;

    let result = match config_parallelism {
        ConfigParallelism::Default | ConfigParallelism::Adaptive => (
            adaptive_parallelism,
            Some(AdaptiveParallelismStrategy::Auto),
        ),
        ConfigParallelism::Fixed(fixed_parallelism) => (
            PbTableParallelism {
                parallelism: Some(PbParallelism::Fixed(FixedParallelism {
                    parallelism: fixed_parallelism.get() as _,
                })),
            },
            None,
        ),
        ConfigParallelism::Bounded(_) | ConfigParallelism::Ratio(_) => {
            (adaptive_parallelism, config_parallelism.adaptive_strategy())
        }
    };

    Ok(result)
}

fn extract_fragment_parallelism(parallelism: SetVariableValue) -> Result<Option<TableParallelism>> {
    match parallelism {
        SetVariableValue::Default => Ok(None),
        other => extract_simple_adaptive_or_fixed_parallelism(other).map(Some),
    }
}

fn parse_single_parallelism_value(parallelism: SetVariableValue) -> Result<String> {
    match parallelism {
        SetVariableValue::Default => Ok("default".to_owned()),
        SetVariableValue::Single(value) => Ok(value.to_string_unquoted()),
        SetVariableValue::List(_) => Err(ErrorCode::InvalidInputSyntax(
            "target parallelism must be a single value".to_owned(),
        )
        .into()),
    }
}

fn extract_simple_adaptive_or_fixed_parallelism(
    parallelism: SetVariableValue,
) -> Result<TableParallelism> {
    let adaptive_parallelism = PbTableParallelism {
        parallelism: Some(PbParallelism::Adaptive(AdaptiveParallelism {})),
    };

    let target_parallelism = match parallelism {
        SetVariableValue::Single(SetVariableValueSingle::Ident(ident))
            if ident
                .real_value()
                .eq_ignore_ascii_case(&Keyword::ADAPTIVE.to_string()) =>
        {
            adaptive_parallelism
        }

        SetVariableValue::Default => adaptive_parallelism,
        SetVariableValue::Single(SetVariableValueSingle::Literal(Value::Number(v))) => {
            let fixed_parallelism = v.parse::<u32>().map_err(|e| {
                ErrorCode::InvalidInputSyntax(format!(
                    "target parallelism must be a valid number or adaptive: {}",
                    e.as_report()
                ))
            })?;

            if fixed_parallelism == 0 {
                adaptive_parallelism
            } else {
                PbTableParallelism {
                    parallelism: Some(PbParallelism::Fixed(FixedParallelism {
                        parallelism: fixed_parallelism,
                    })),
                }
            }
        }

        _ => {
            return Err(ErrorCode::InvalidInputSyntax(
                "target parallelism must be a valid number or adaptive".to_owned(),
            )
            .into());
        }
    };

    Ok(target_parallelism)
}

#[cfg(test)]
mod tests {
    use risingwave_common::system_param::adaptive_parallelism_strategy::AdaptiveParallelismStrategy;
    use risingwave_pb::meta::table_parallelism::{FixedParallelism, PbParallelism};
    use risingwave_pb::meta::{PbTableParallelism, TableParallelism};
    use risingwave_sqlparser::ast::{Ident, SetVariableValueSingle};

    use super::*;

    fn fixed_parallelism(parallelism: u32) -> TableParallelism {
        PbTableParallelism {
            parallelism: Some(PbParallelism::Fixed(FixedParallelism { parallelism })),
        }
    }

    fn adaptive_parallelism() -> TableParallelism {
        PbTableParallelism {
            parallelism: Some(PbParallelism::Adaptive(AdaptiveParallelism {})),
        }
    }

    #[test]
    fn test_extract_table_parallelism_fixed() {
        let (parallelism, strategy) = extract_table_parallelism(SetVariableValue::Single(
            SetVariableValueSingle::Literal(Value::Number("4".into())),
        ))
        .unwrap();

        assert_eq!(parallelism, fixed_parallelism(4));
        assert_eq!(strategy, None);
    }

    #[test]
    fn test_extract_table_parallelism_adaptive_variants() {
        let (parallelism, strategy) = extract_table_parallelism(SetVariableValue::Default).unwrap();
        assert_eq!(parallelism, adaptive_parallelism());
        assert_eq!(strategy, Some(AdaptiveParallelismStrategy::Auto));

        let (parallelism, strategy) = extract_table_parallelism(SetVariableValue::Single(
            SetVariableValueSingle::Ident(Ident::new_unchecked("adaptive")),
        ))
        .unwrap();
        assert_eq!(parallelism, adaptive_parallelism());
        assert_eq!(strategy, Some(AdaptiveParallelismStrategy::Auto));

        let (parallelism, strategy) = extract_table_parallelism(SetVariableValue::Single(
            SetVariableValueSingle::Raw("bounded(4)".to_owned()),
        ))
        .unwrap();
        assert_eq!(parallelism, adaptive_parallelism());
        assert_eq!(
            strategy,
            Some(AdaptiveParallelismStrategy::Bounded(4.try_into().unwrap()))
        );

        let (parallelism, strategy) = extract_table_parallelism(SetVariableValue::Single(
            SetVariableValueSingle::Raw("ratio(0.5)".to_owned()),
        ))
        .unwrap();
        assert_eq!(parallelism, adaptive_parallelism());
        assert_eq!(strategy, Some(AdaptiveParallelismStrategy::Ratio(0.5)));
    }

    #[test]
    fn test_extract_fragment_parallelism_does_not_support_bounded_ratio() {
        assert_eq!(
            extract_fragment_parallelism(SetVariableValue::Default).unwrap(),
            None
        );
        assert_eq!(
            extract_fragment_parallelism(SetVariableValue::Single(
                SetVariableValueSingle::Literal(Value::Number("0".into())),
            ))
            .unwrap(),
            Some(adaptive_parallelism())
        );
        assert!(
            extract_fragment_parallelism(SetVariableValue::Single(SetVariableValueSingle::Raw(
                "bounded(4)".to_owned()
            ),))
            .is_err()
        );
        assert!(
            extract_fragment_parallelism(SetVariableValue::Single(SetVariableValueSingle::Raw(
                "ratio(0.5)".to_owned()
            ),))
            .is_err()
        );
    }

    #[test]
    fn test_extract_backfill_parallelism_adaptive_variants() {
        let (parallelism, strategy) = extract_backfill_parallelism(SetVariableValue::Single(
            SetVariableValueSingle::Raw("bounded(4)".to_owned()),
        ))
        .unwrap();
        assert_eq!(parallelism, Some(adaptive_parallelism()));
        assert_eq!(
            strategy,
            Some(AdaptiveParallelismStrategy::Bounded(4.try_into().unwrap()))
        );

        let (parallelism, strategy) =
            extract_backfill_parallelism(SetVariableValue::Default).unwrap();
        assert_eq!(parallelism, None);
        assert_eq!(strategy, None);
    }

    #[test]
    fn test_extract_table_parallelism_rejects_list_values() {
        assert!(
            extract_table_parallelism(SetVariableValue::List(vec![
                SetVariableValueSingle::Literal(Value::Number("1".into())),
                SetVariableValueSingle::Literal(Value::Number("2".into())),
            ]))
            .is_err()
        );
    }
}
