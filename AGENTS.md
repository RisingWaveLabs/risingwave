# Coding Agent Instructions for RisingWave

## Project Overview

RisingWave is a Postgres-compatible streaming database that offers the simplest and most cost-effective way to process, analyze, and manage real-time event data — with built-in support for the Apache Iceberg™ open table format.

RisingWave components are developed in Rust and split into several crates:

1. `config` - Default configurations for servers
2. `prost` - Generated protobuf rust code (grpc and message definitions)
3. `stream` - Stream compute engine
4. `batch` - Batch compute engine for queries against materialized views
5. `frontend` - SQL query planner and scheduler
6. `storage` - Cloud native storage engine
7. `meta` - Meta engine
8. `utils` - Independent util crates
9. `cmd` - All binaries, `cmd_all` contains the all-in-one binary `risingwave`
10. `risedevtool` - Developer tool for RisingWave

## Coding Style

- Always write code comments in English.
- Write simple, easy-to-read and easy-to-maintain code.
- Use `cargo fmt` to format the code if needed.
- Follow existing code patterns and conventions in the repository.

## Build, Run, Test

You may need to learn how to build and test RisingWave when implementing features or fixing bugs.

### Build & Check

- Use `./risedev b` to build the project.
- Use `./risedev c` to check if the code follow rust-clippy rules, coding styles, etc.
- After changing config definitions or defaults under `src/common/src/config`, run `./risedev generate-example-config` to update `src/config/example.toml` and `src/config/docs.md`.

### Unit Test

- Integration tests and unit tests are valid Rust/Tokio tests, you can locate and run those related in a standard Rust way.
- Parser tests: use `./risedev update-parser-test` to regenerate expected output.
- Planner tests: use `./risedev run-planner-test [name]` to run and `./risedev do-apply-planner-test` (or `./risedev dapt`) to update expected output.

### End-to-End Test

- Use `./risedev d` to run a RisingWave instance in the background via tmux.
  - It builds RisingWave binaries if necessary. The build process can take up to 10 minutes, depending on the incremental build results. Use a large timeout for this step, and be patient.
  - It kills the previous instance, if exists.
  - Optionally pass a profile name (see `risedev.yml` and for local profile see `risedev-profiles.user.yml`) to choose external services or components. By default it uses `default`.
  - This runs in the background so you do not need a separate terminal.
  - You can connect right after the command finishes.
  - Logs are written to files in `.risingwave/log` folder.
- Use `./risedev k` to stop a RisingWave instance started by `./risedev d`.
- Only when a RisingWave instance is running, you can use `./risedev psql -c "<your query>"` to run SQL queries in RW.
- Only when a RisingWave instance is running, you can use `./risedev slt './path/to/e2e-test-file.slt'` to run end-to-end SLT tests.
  - File globs like `/**/*.slt` is allowed.
  - Failed run may leave some objects in the database that interfere with next run. Use `./risedev slt-clean ./path/to/e2e-test-file.slt` to reset the database before running tests.
- The preferred way to write tests is to write tests in SQLLogicTest format.
- Tests are put in `./e2e_test` folder.

### Sandbox escalation

When sandboxing is enabled, these commands need `require_escalated` because they bind or connect to local TCP sockets:

- `./risedev d` and `./risedev p` (uses local ports and tmux sockets)
- `./risedev psql ...` or direct `psql -h localhost -p 4566 ...` (local TCP connection)
- `./risedev slt './path/to/e2e-test-file.slt'` (connects to local TCP via psql protocol)
- Any command that checks running services via local TCP (for example, health checks or custom SQL clients)

## Pull Request Submission Labels

PR labels in this repository can affect CI coverage and documentation follow-up. To keep normal coding work lightweight, load the detailed label protocol only at PR submission time.

When opening or updating a pull request, changing PR labels, or marking a PR ready for review:

1. Read `.agents/PR_LABELING.md`.
2. Inspect the current PR labels and draft state before adding anything:
   `gh pr view <PR> --json isDraft,labels,title,body,files`
3. Add only validated missing labels. Do not remove labels that existed before the current agent operation unless explicitly instructed by a maintainer. An agent may remove a label it just added if validation shows it was a mistake, but it must report the correction.
4. In the final PR report, list labels added, labels intentionally not added, local tests run, and CI labels requested.

## Connector Development

See `docs/dev/src/connector/intro.md`.

## Repo Skills

- Use `$risingwave-rust-analyzer` for RisingWave-specific `rust-analyzer` CLI and LSP workflows, especially when a task needs fast semantic inspection, structural search/replace, or crate-specific feature guidance. Skill path: `.agents/skills/risingwave-rust-analyzer/SKILL.md`.
- Use `$fix-buildkite-ci` for Buildkite triage and focused CI fixes. Skill path: `.agents/skills/fix-buildkite-ci/SKILL.md`.
- Use `$risingwave-slt-authoring` when creating, reviewing, or fixing RisingWave SQLLogicTest (`.slt`/`.slt.serial`) files, especially tests with `system ok`, connector fixtures, CI coverage, madsim, or flaky/idempotent behavior. Skill path: `.agents/skills/risingwave-slt-authoring/SKILL.md`.
