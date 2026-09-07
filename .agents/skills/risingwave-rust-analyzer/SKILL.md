---
name: risingwave-rust-analyzer
description: Use rust-analyzer CLI and editor/LSP settings to inspect, diagnose, and refactor RisingWave Rust code. Use when working in the RisingWave workspace and you need fast semantic analysis, unresolved-reference checks, macro-aware navigation, structured search/replace, or guidance on choosing the correct crate root and feature flags before heavier cargo or risedev commands.
---

# RisingWave Rust Analyzer

## Overview

Use `rust-analyzer` to get semantic feedback faster than a full workspace build. In RisingWave, always resolve the crate root and feature set first: rust-analyzer feature settings are workspace-level per linked project, so per-crate feature differences usually require separate roots or editor workspaces.

## Workflow

1. Confirm the analysis root.
2. Resolve the target crate and its features.
3. Run the narrowest rust-analyzer CLI command that answers the question.
4. Escalate to editor/LSP configuration only when CLI scope or features are insufficient.

## Step 1: Confirm the Analysis Root

Default to the repository root so rust-analyzer sees the pinned toolchain and Cargo flags:

- Read the pinned toolchain from the root-level `rust-toolchain` symlink, which points to `ci/rust-toolchain`; do not assume a specific nightly version.
- `.cargo/config.toml` sets important `rustflags`, including `+avx2`, `tokio_unstable`, and linker flags.

Use a crate directory or crate `Cargo.toml` only when you intentionally want narrower analysis and can tolerate missing cross-workspace context.

## Step 2: Resolve the Crate and Features

List workspace packages and features before tuning rust-analyzer:

- Run `scripts/list-crate-features.sh` with no arguments to list package names.
- Run `scripts/list-crate-features.sh <package>` to print the manifest path and feature map.
- Use exact Cargo package names such as `risingwave_meta`, `risingwave_stream`, or `risingwave_connector`.

Important limitation: rust-analyzer does not expose a first-class per-crate feature matrix inside one linked project. The relevant settings are:

- `rust-analyzer.cargo.features`
- `rust-analyzer.cargo.noDefaultFeatures`
- `rust-analyzer.check.features`
- `rust-analyzer.check.noDefaultFeatures`
- `rust-analyzer.check.overrideCommand`
- `rust-analyzer.linkedProjects`

These settings apply per linked project or workspace, not per package within one workspace root. When different RisingWave crates need different feature sets:

- Prefer a narrower root such as `src/meta` or that crate's `Cargo.toml`.
- Or open separate editor workspaces / linked projects for each crate variant.
- Or use `rust-analyzer.check.overrideCommand` to force diagnostics to run `cargo check -p <package> ... --message-format=json`.

For command examples and config snippets, read `references/rust-analyzer-cli.md`.

## Step 3: Use rust-analyzer CLI

Use the lightest command that answers the question:

- `rust-analyzer diagnostics .` for workspace diagnostics.
- `rust-analyzer unresolved-references .` for missing symbols/imports.
- `rust-analyzer analysis-stats . --parallel` for batch semantic analysis.
- `rust-analyzer analysis-stats . -o path/to/file.rs` to narrow to one file or subtree.
- `rust-analyzer prime-caches .` before a long editor session on a cold workspace.
- `rust-analyzer search '$a.foo($b)'` for structural search.
- `rust-analyzer ssr '$a.foo($b) ==>> bar($a, $b)'` for structural replacement.
- `cat file.rs | rust-analyzer parse` and `cat file.rs | rust-analyzer symbols` for syntax-level inspection.

Prefer CLI for quick analysis and refactor planning. Prefer the editor when you need hover, jump-to-definition, rename, recursive macro expansion, or interactive diagnostics.

## Step 4: Configure LSP Carefully

For RisingWave, keep both of these enabled unless you have a measured reason to disable them:

- `rust-analyzer.cargo.buildScripts.enable = true`
- `rust-analyzer.procMacro.enable = true`

The workspace is macro-heavy, and disabling either option causes misleading unresolved references or incomplete types.

For the `lints` crate, remember that `lints/Cargo.toml` sets `package.metadata.rust-analyzer.rustc_private = true`.

## RisingWave-Specific Heuristics

- Start from the repository root unless the request is explicitly crate-local.
- If diagnostics look wrong, compare rust-analyzer output with `cargo check -p <package>` using the same features.
- For crates with many optional integrations, such as `risingwave_connector`, resolve features before trusting diagnostics.
- For macro-heavy paths, pair rust-analyzer with `cargo expand` when the user asks how generated code really looks.
- Avoid claiming feature-specific correctness unless you have matched the target crate and features explicitly.

## Resources

- `scripts/list-crate-features.sh` lists package names, manifest paths, features, and suggested Cargo / rust-analyzer commands.
- `references/rust-analyzer-cli.md` captures the command cookbook and feature-flag caveats for this repository.
