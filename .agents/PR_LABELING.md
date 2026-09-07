# RisingWave PR Labeling Guide for Agents

Use this guide only when creating or updating a pull request, changing pull request labels, or marking a pull request ready for review.

PR labels in RisingWave are not only metadata. Some labels select Buildkite test coverage, and some labels trigger documentation follow-up automation after merge. The goal is to avoid under-testing while keeping extra coverage relevant.

This guide gives decision inputs, not a mechanical labeler. The agent opening or updating the PR is responsible for reading the diff, reasoning about the user-visible and test-risk surface, and deciding which labels to add.

## Required sources of truth

Before mutating labels, inspect the current repository and PR state instead of relying on memory:

```bash
gh label list --repo risingwavelabs/risingwave --limit 500
gh pr view <PR> --json isDraft,labels,title,body,files
```

Also inspect the repository-side rules that define label behavior:

- `.github/labeler.yml` for append-only auto-labeling rules.
- `ci/workflows/pull-request.yml` for PR Buildkite label gates and scripts.
- `ci/workflows/main-cron.yml` for main-cron label gates and scripts.
- `docs/dev/src/ci.md` for the CI labels guide.

A label is addable only if it exists in the live GitHub label list. Do not apply a label solely because it appears in workflow YAML. Workflow YAML and GitHub label state can drift.

## Label operation sequence

When opening or updating a PR:

1. List the current PR labels and draft state.
2. Derive candidate labels from changed files, PR title/body, `.github/labeler.yml`, and the CI workflow gates.
3. Decide which candidate labels are actually relevant to this PR's behavior and risk surface.
4. Validate every chosen label against the live GitHub label list.
5. Drop any chosen label that does not exist remotely.
6. Add only missing, valid, additive labels.
7. Do not remove labels that predate the current agent operation, especially auto-applied or maintainer-applied labels, unless explicitly instructed by a maintainer. If the agent added a label during the current operation and then determines that it was a mistake, remove it immediately and report the correction.
8. In the PR report, list labels added, labels intentionally not added, and the local/CI verification already run or requested.

## CI coverage labels

Use `ci/run-*` labels to request relevant CI coverage. Prefer the smallest relevant superset of coverage: tests may be slightly over-covered, but must not be under-covered.

Reasonable over-coverage means adding a relevant neighboring or broader test label for the affected subsystem. It does not mean blindly adding unrelated expensive labels or `run-all` labels.

Examples:

- Connector source changes: add the narrow source-specific label when available, for example `ci/run-e2e-kafka-source-tests`, `ci/run-e2e-pulsar-source-tests`, or `ci/run-e2e-pubsub-source-tests`. If shared source behavior is affected, also consider `ci/run-e2e-source-tests`.
- Connector sink changes: add the narrow sink-specific label when available, for example `ci/run-e2e-kafka-sink-tests`, `ci/run-e2e-postgres-sink-tests`, or `ci/run-e2e-jdbc-sink-tests`. If shared sink behavior is affected, also consider `ci/run-e2e-sink-tests`.
- Metadata migration or compatibility-sensitive changes: include backwards compatibility coverage when applicable, such as `ci/run-backwards-compat-tests`.
- SQL semantics, planner, optimizer, or expression changes: consider e2e, SQLSmith, and deterministic simulation coverage according to the changed failure mode.
- Recovery, barrier, storage, or scheduling changes: consider deterministic simulation labels such as `ci/run-integration-test-deterministic-simulation`, `ci/run-e2e-test-deterministic-simulation`, or `ci/run-recovery-test-deterministic-simulation`.
- Pure documentation or comment-only changes should not receive unrelated CI expansion labels unless docs tooling, docs SQL logic tests, or CI scripts are changed.

Do not add labels that are only present in workflow text but absent from live GitHub labels. If such a mismatch blocks a desired CI request, mention it in the PR notes instead of inventing the label.

Prefer PR labels for PR-triggered coverage requests. Treat `CI_STEPS` as a Buildkite/manual-debugging mechanism, not the default agent workflow for PR label operations.

## `ci/pr/run-selected`

`ci/pr/run-selected` is not an "extra coverage" label. It is a selector that can suppress default PR workflow steps and run only selected `ci/run-*` steps.

- Avoid `ci/pr/run-selected` by default.
- Use it only for draft PRs.
- Never add it to a non-draft PR.
- If the goal is more confidence on a normal PR, add relevant `ci/run-*` labels without `ci/pr/run-selected`, so default PR workflow coverage is preserved.

## `ci/main-cron/run-selected`

Use `ci/main-cron/run-selected` only when intentionally running selected main-cron steps for a PR.

- Do not add it alone.
- Pair it with one or more existing `ci/run-*` labels needed by the affected area.
- Prefer selected main-cron coverage for targeted validation.
- Reserve `ci/main-cron/run-all` for broad, high-risk changes where full main-cron validation is intentional.

## Test-area labels

Use `A-test` or `component/test` only when the PR changes testing framework, shared test infrastructure, CI test scripts, or miscellaneous test components. Do not add them merely because the PR includes regression tests.

Use `type/flaky-test` only when the primary purpose is fixing flaky tests.

## User-facing and documentation labels

`user-facing-changes` triggers documentation follow-up after merge. Add it only when both of these conditions are true:

1. The PR changes a supported external product interface or behavior.
2. The change warrants release-note or product-documentation follow-up.

Qualifying changes include SQL syntax or semantics, documented connector options, user-configurable defaults or semantics, client-visible API or SQL behavior, stable system catalog output, compatibility behavior, and supported operational workflows that require users to act differently.

Do not add `user-facing-changes` merely because some output is technically observable. In particular, do not add it for:

- Internal tracing logs, log wording changes, or internal-only diagnostics.
- Development-only metrics, dashboards, or observability tooling.
- Internal Rust `pub` APIs or other implementation interfaces.
- Refactoring, recovery, scheduling, or performance work that does not change a supported user or operator workflow.
- Comment, formatting, test-only, CI-only, or documentation wording changes that do not change product behavior.
- Typo or wording improvements that do not change the meaning of a supported error or response.

When the impact is ambiguous, do not add the label. Explain the possible user-facing impact in the final PR report and ask a maintainer to confirm whether documentation follow-up is needed. The threshold is whether documentation or release-note follow-up is appropriate, not whether a user could conceivably observe the diff.

Add `breaking-change` only for real compatibility-breaking or migration-impacting changes, and explain the impact in the PR body.

`user-facing-changes` and `breaking-change` can trigger documentation follow-up automation after merge. Documentation-status labels such as `📖✓`, `📖✗`, and `📖‒` are not substitutes for `user-facing-changes` or `breaking-change` unless current repository guidance explicitly says so.

## Never do this

- Do not add `ci/pr/run-selected` to a non-draft PR.
- Do not use `ci/pr/run-selected` as an extra-test label.
- Do not add labels that are absent from `gh label list --repo risingwavelabs/risingwave`.
- Do not remove labels that existed before the current operation unless explicitly instructed by a maintainer.
- Do not use unrelated expensive labels just to be safe.
