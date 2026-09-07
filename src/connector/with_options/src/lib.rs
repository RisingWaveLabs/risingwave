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

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

/// Annotates that the struct represents the `WITH` properties for a connector.
/// This implements a marker trait `WithOptions`.
/// It's also used to generate the `with_option_*.yaml` files.
///
/// ## Notes about how to define property structs
///
/// ### Prefer strongly-typed fields
///
/// Avoid using `HashMap` with `#[serde(flatten)]` to include all unknown fields.
/// Declare all fields explicitly instead.
///
/// The only exception now is CDC, which needs to pass a lot of options as-is to Debezium.
///
/// ### Common struct
///
/// When there are some fields can be grouped together, and/or can be shared by source and sink,
/// or by multiple connectors, define a struct for them and use the common struct as a
/// `#[serde(flatten)]` field.
///
/// Add `#[derive(WithOptions)]` to both the outer and the inner struct.
///
/// Avoid using nested `#[serde(flatten)]` field in the common struct,
/// because this will lead to unexpected serde behaviors.
/// Put all flatten fields in the top-level struct instead.
///
/// ## Field Annotations
///
/// ### Allow Alter On Fly fields (allowed in `alter table connector with`)
///
/// Use `#[with_option(allow_alter_on_fly)]` to mark fields that can be changed on the fly
/// without requiring a restart or recreation of the connector:
///
/// ```ignore
/// #[derive(WithOptions)]
/// struct MyConnectorConfig {
///     #[with_option(allow_alter_on_fly)]
///     pub rate_limit: Option<u32>,
///     pub endpoint: String,  // not allow_alter_on_fly
/// }
/// ```
///
/// ### Iceberg Engine fields
///
/// Use `#[with_option(iceberg_engine)]` to mark Iceberg sink fields that can be specified
/// in `CREATE TABLE ... ENGINE = ICEBERG`. The frontend uses the generated field list to route
/// these options from the table or source properties to the internal Iceberg sink.
#[proc_macro_derive(WithOptions, attributes(with_option))]
pub fn derive_helper_attr(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let fields = match input.data {
        syn::Data::Struct(ref data) => match data.fields {
            syn::Fields::Named(ref fields) => &fields.named,
            _ => return quote! { compile_error!("WithOptions can only be derived for structs with named fields"); }.into(),
        },
        _ => return quote! { compile_error!("WithOptions can only be derived for structs"); }.into(),
    };

    let mut assert_impls = vec![];

    for field in fields {
        let field_name = field.ident.as_ref().unwrap();

        assert_impls.push(quote!(
            crate::with_options::WithOptions::assert_receiver_is_with_options(&self.#field_name);
        ))
    }

    let struct_name = input.ident;
    // This macro is only be expected to used in risingwave_connector. This trait is also defined there.
    if input.generics.params.is_empty() {
        quote! {
            impl crate::with_options::WithOptions for #struct_name {
                fn assert_receiver_is_with_options(&self) {
                    #(#assert_impls)*
                }
            }
        }
        .into()
    } else {
        // Note: CDC properties have generics.
        let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
        quote! {
            impl #impl_generics crate::with_options::WithOptions for #struct_name #ty_generics #where_clause {
                fn assert_receiver_is_with_options(&self) {
                    #(#assert_impls)*
                }
            }
        }.into()
    }
}
