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

use std::collections::HashMap;

risingwave_expr_impl::enable!();

use itertools::Itertools;
use risingwave_expr::sig::{FUNCTION_REGISTRY, FuncName, FuncSign, SigDataType};
#[test]
fn test_func_sig_map() {
    // convert FUNC_SIG_MAP to a more convenient map for testing
    let mut new_map: HashMap<FuncName, HashMap<Vec<SigDataType>, Vec<FuncSign>>> = HashMap::new();
    for sig in FUNCTION_REGISTRY.iter_scalars() {
        // exclude deprecated functions
        if sig.deprecated {
            continue;
        }

        new_map
            .entry(sig.name.clone())
            .or_default()
            .entry(sig.inputs_type.clone())
            .or_default()
            .push(sig.clone());
    }

    let mut duplicated: Vec<_> = new_map
        .into_values()
        .flat_map(|funcs_with_same_name| {
            funcs_with_same_name.into_values().filter_map(|v| {
                if v.len() > 1 {
                    Some(format!(
                        "{}({}) -> {}",
                        v[0].name.as_str_name().to_ascii_lowercase(),
                        v[0].inputs_type.iter().format(", "),
                        v.iter()
                            .map(|sig| sig.ret_type.to_string())
                            .sorted() // sort the return types to make the test stable
                            .format("/")
                    ))
                } else {
                    None
                }
            })
        })
        .collect();
    duplicated.sort();

    // This snapshot shows the function signatures without a unique match. Frontend has to
    // handle them specially without relying on FuncSigMap.
    let expected = expect_test::expect![[r#"
        [
            "cast(anyarray) -> anyarray/character varying",
            "cast(bigint) -> character varying/double precision/integer/numeric/real/rw_int256/serial/smallint",
            "cast(boolean) -> character varying/integer",
            "cast(character varying) -> anyarray/bigint/boolean/bytea/character varying/date/double precision/integer/interval/jsonb/numeric/real/rw_int256/smallint/time without time zone/timestamp without time zone/variant/vector",
            "cast(date) -> character varying/timestamp without time zone",
            "cast(double precision) -> bigint/character varying/integer/numeric/real/smallint",
            "cast(integer) -> bigint/boolean/character varying/double precision/numeric/real/rw_int256/smallint",
            "cast(interval) -> character varying/time without time zone",
            "cast(jsonb) -> bigint/boolean/character varying/double precision/integer/numeric/real/smallint/variant",
            "cast(numeric) -> bigint/character varying/double precision/integer/real/smallint",
            "cast(real) -> bigint/character varying/double precision/integer/numeric/smallint",
            "cast(rw_int256) -> character varying/double precision",
            "cast(serial) -> bigint/timestamp with time zone",
            "cast(smallint) -> bigint/character varying/double precision/integer/numeric/real/rw_int256",
            "cast(time without time zone) -> character varying/interval",
            "cast(timestamp without time zone) -> character varying/date/time without time zone",
            "cast(variant) -> character varying/jsonb",
            "cast(vector) -> character varying/real[]",
            "greatest() -> bigint/boolean/bytea/character varying/date/double precision/integer/interval/numeric/real/rw_int256/serial/smallint/time without time zone/timestamp with time zone/timestamp without time zone",
            "least() -> bigint/boolean/bytea/character varying/date/double precision/integer/interval/numeric/real/rw_int256/serial/smallint/time without time zone/timestamp with time zone/timestamp without time zone",
        ]
    "#]];
    expected.assert_debug_eq(&duplicated);
}
