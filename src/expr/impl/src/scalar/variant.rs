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

use risingwave_common::types::{ScalarRefImpl, VariantRef, VariantVal};
use risingwave_expr::expr::Context;
use risingwave_expr::{ExprError, Result, function};
use thiserror_ext::AsReport;

#[function("to_variant(any) -> variant")]
fn to_variant(input: Option<ScalarRefImpl<'_>>, ctx: &Context) -> Result<VariantVal> {
    VariantVal::try_from_scalar_ref(input, &ctx.arg_types[0]).map_err(|e| ExprError::InvalidParam {
        name: "to_variant",
        reason: e.to_report_string().into(),
    })
}

#[function("variant_get(variant, varchar) -> variant")]
fn variant_get(value: VariantRef<'_>, path: &str) -> Result<Option<VariantVal>> {
    value
        .access_path_strict(path)
        .map_err(|e| ExprError::InvalidParam {
            name: "variant_get",
            reason: e.to_report_string().into(),
        })
}

#[function("try_variant_get(variant, varchar) -> variant")]
fn try_variant_get(value: VariantRef<'_>, path: &str) -> Option<VariantVal> {
    value.access_path(path)
}

#[function("variant_typeof(variant) -> varchar")]
fn variant_typeof(value: VariantRef<'_>, writer: &mut impl std::fmt::Write) {
    writer.write_str(value.type_name()).unwrap();
}
