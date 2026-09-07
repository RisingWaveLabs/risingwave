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

use std::num::NonZeroUsize;

use itertools::Itertools;
use pretty_xmlish::{Pretty, StrAssocArr};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, Interval};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_expr::ExprError;

use super::super::utils::IndicesDisplay;
use super::{GenericPlanNode, GenericPlanRef, impl_distill_unit_from_fields};
use crate::error::Result;
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, InputRefDisplay, Literal};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::ColIndexMappingRewriteExt;

/// [`HopWindow`] implements Hop Table Function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HopWindow<PlanRef> {
    pub input: PlanRef,
    pub time_col: InputRef,
    pub window_slide: Interval,
    pub window_size: Interval,
    pub window_offset: Interval,
    /// Provides mapping from input schema, `window_start`, `window_end` to output schema.
    /// For example, if we had:
    /// input schema: | 0: `trip_time` | 1: `trip_name` |
    /// `window_start`: 2
    /// `window_end`: 3
    /// output schema: | `trip_name` | `window_start` |
    /// Then, `output_indices`: [1, 2]
    pub output_indices: Vec<usize>,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for HopWindow<PlanRef> {
    fn schema(&self) -> Schema {
        let output_type = DataType::window_of(&self.time_col.data_type).unwrap();
        let mut original_schema = self.input.schema().clone();
        original_schema.fields.reserve_exact(2);
        let window_start = Field::with_name(output_type.clone(), "window_start");
        let window_end = Field::with_name(output_type, "window_end");
        original_schema.fields.push(window_start);
        original_schema.fields.push(window_end);
        self.output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let window_start_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == self.input.schema().len());
        let window_end_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == self.input.schema().len() + 1);
        if window_start_index.is_none() && window_end_index.is_none() {
            None
        } else {
            let mut pk = self
                .input
                .stream_key()?
                .iter()
                .filter_map(|&pk_idx| self.output_indices.iter().position(|&idx| idx == pk_idx))
                .collect_vec();
            if let Some(start_idx) = window_start_index {
                pk.push(start_idx);
            };
            if let Some(end_idx) = window_end_index {
                pk.push(end_idx);
            };
            Some(pk)
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let mut fd_set = self
            .i2o_col_mapping()
            .rewrite_functional_dependency_set(self.input.functional_dependency().clone());
        let (start_idx_in_output, end_idx_in_output) = {
            let internal2output = self.internal2output_col_mapping();
            (
                internal2output.try_map(self.internal_window_start_col_idx()),
                internal2output.try_map(self.internal_window_end_col_idx()),
            )
        };
        if let Some(start_idx) = start_idx_in_output
            && let Some(end_idx) = end_idx_in_output
        {
            fd_set.add_functional_dependency_by_column_indices(&[start_idx], &[end_idx]);
            fd_set.add_functional_dependency_by_column_indices(&[end_idx], &[start_idx]);
        }
        fd_set
    }
}

impl<PlanRef: GenericPlanRef> HopWindow<PlanRef> {
    pub fn clone_with_input<OtherPlanRef>(&self, input: OtherPlanRef) -> HopWindow<OtherPlanRef> {
        HopWindow {
            input,
            time_col: self.time_col.clone(),
            window_slide: self.window_slide,
            window_size: self.window_size,
            window_offset: self.window_offset,
            output_indices: self.output_indices.clone(),
        }
    }

    pub fn output_window_start_col_idx(&self) -> Option<usize> {
        self.internal2output_col_mapping()
            .try_map(self.internal_window_start_col_idx())
    }

    pub fn output_window_end_col_idx(&self) -> Option<usize> {
        self.internal2output_col_mapping()
            .try_map(self.internal_window_end_col_idx())
    }

    pub fn into_parts(self) -> (PlanRef, InputRef, Interval, Interval, Interval, Vec<usize>) {
        (
            self.input,
            self.time_col,
            self.window_slide,
            self.window_size,
            self.window_offset,
            self.output_indices,
        )
    }

    pub fn internal_window_start_col_idx(&self) -> usize {
        self.input.schema().len()
    }

    pub fn internal_window_end_col_idx(&self) -> usize {
        self.input.schema().len() + 1
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        self.output2internal_col_mapping()
            .composite(&self.internal2input_col_mapping())
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.input2internal_col_mapping()
            .composite(&self.internal2output_col_mapping())
    }

    pub fn internal_column_num(&self) -> usize {
        self.input.schema().len() + 2
    }

    pub fn output2internal_col_mapping(&self) -> ColIndexMapping {
        self.internal2output_col_mapping()
            .inverse()
            .expect("must be invertible")
    }

    pub fn internal2output_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(&self.output_indices, self.internal_column_num())
    }

    pub fn input2internal_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::identity_or_none(self.input.schema().len(), self.internal_column_num())
    }

    pub fn internal2input_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::identity_or_none(self.internal_column_num(), self.input.schema().len())
    }

    pub fn derive_window_start_and_end_exprs(&self) -> Result<(Vec<ExprImpl>, Vec<ExprImpl>)> {
        let Self {
            window_size,
            window_slide,
            window_offset,
            time_col,
            ..
        } = &self;
        let units = window_size
            .exact_div(window_slide)
            .and_then(|x| NonZeroUsize::new(usize::try_from(x).ok()?))
            .ok_or_else(|| ExprError::InvalidParam {
                name: "window",
                reason: format!(
                    "window_size {} cannot be divided by window_slide {}",
                    window_size, window_slide
                )
                .into(),
            })?
            .get();
        let window_size_expr: ExprImpl =
            Literal::new(Some((*window_size).into()), DataType::Interval).into();
        let window_slide_expr: ExprImpl =
            Literal::new(Some((*window_slide).into()), DataType::Interval).into();
        let window_offset_expr: ExprImpl =
            Literal::new(Some((*window_offset).into()), DataType::Interval).into();

        let window_size_sub_slide = FunctionCall::new(
            ExprType::Subtract,
            vec![window_size_expr, window_slide_expr.clone()],
        )?
        .into();

        let time_col_shifted = FunctionCall::new(
            ExprType::Subtract,
            vec![
                ExprImpl::InputRef(Box::new(time_col.clone())),
                window_size_sub_slide,
            ],
        )?
        .into();

        let hop_start: ExprImpl = FunctionCall::new(
            ExprType::TumbleStart,
            vec![time_col_shifted, window_slide_expr, window_offset_expr],
        )?
        .into();

        let mut window_start_exprs = Vec::with_capacity(units);
        let mut window_end_exprs = Vec::with_capacity(units);
        for i in 0..units {
            {
                let window_start_offset =
                    window_slide
                        .checked_mul_int(i)
                        .ok_or_else(|| ExprError::InvalidParam {
                            name: "window",
                            reason: format!(
                                "window_slide {} cannot be multiplied by {}",
                                window_slide, i
                            )
                            .into(),
                        })?;
                let window_start_offset_expr =
                    Literal::new(Some(window_start_offset.into()), DataType::Interval).into();
                let window_start_expr = FunctionCall::new(
                    ExprType::Add,
                    vec![hop_start.clone(), window_start_offset_expr],
                )?
                .into();
                window_start_exprs.push(window_start_expr);
            }
            {
                let window_end_offset =
                    window_slide.checked_mul_int(i + units).ok_or_else(|| {
                        ExprError::InvalidParam {
                            name: "window",
                            reason: format!(
                                "window_slide {} cannot be multiplied by {}",
                                window_slide,
                                i + units
                            )
                            .into(),
                        }
                    })?;
                let window_end_offset_expr =
                    Literal::new(Some(window_end_offset.into()), DataType::Interval).into();
                let window_end_expr = FunctionCall::new(
                    ExprType::Add,
                    vec![hop_start.clone(), window_end_offset_expr],
                )?
                .into();
                window_end_exprs.push(window_end_expr);
            }
        }
        assert_eq!(window_start_exprs.len(), window_end_exprs.len());
        Ok((window_start_exprs, window_end_exprs))
    }

    pub fn fields_pretty<'a>(&self) -> StrAssocArr<'a> {
        let mut out = Vec::with_capacity(5);
        let output_type = DataType::window_of(&self.time_col.data_type).unwrap();
        out.push((
            "time_col",
            Pretty::display(&InputRefDisplay {
                input_ref: &self.time_col,
                input_schema: self.input.schema(),
            }),
        ));
        out.push(("slide", Pretty::display(&self.window_slide)));
        out.push(("size", Pretty::display(&self.window_size)));
        if self
            .output_indices
            .iter()
            .copied()
            // Behavior is the same as `LogicalHopWindow::internal_column_num`
            .eq(0..(self.input.schema().len() + 2))
        {
            out.push(("output", Pretty::from("all")));
        } else {
            let original_schema: Schema = self
                .input
                .schema()
                .clone()
                .into_fields()
                .into_iter()
                .chain([
                    Field::with_name(output_type.clone(), "window_start"),
                    Field::with_name(output_type, "window_end"),
                ])
                .collect();
            let id = IndicesDisplay {
                indices: &self.output_indices,
                schema: &original_schema,
            };
            out.push(("output", id.distill()));
        }
        out
    }
}

impl_distill_unit_from_fields!(HopWindow, GenericPlanRef);
