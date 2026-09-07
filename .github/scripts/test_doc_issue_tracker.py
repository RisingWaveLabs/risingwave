#!/usr/bin/env python3

import unittest

from doc_issue_tracker import (
    GitHubApiError,
    analyze_event,
    analyze_event_with_canonical_pr,
    canonical_pr_number,
    find_tracking_issues,
    merge_tracking_body,
    resolve_canonical_pr,
    tracking_marker,
)


def pull_request(
    *,
    number=100,
    base_ref="main",
    title="feat: public feature",
    body="- [x] My PR needs documentation updates.",
    labels=None,
    merged=True,
):
    return {
        "number": number,
        "base": {"ref": base_ref},
        "title": title,
        "body": body,
        "labels": [{"name": label} for label in (labels or [])],
        "merged": merged,
    }


class CanonicalPullRequestTest(unittest.TestCase):
    def test_main_pr_is_its_own_canonical_source(self):
        self.assertEqual(canonical_pr_number(pull_request()), 100)

    def test_generated_cherry_pick_body_identifies_original_pr(self):
        pr = pull_request(
            number=200,
            base_ref="release-3.0",
            body="Cherry picking #100 onto branch release-3.0",
        )
        self.assertEqual(canonical_pr_number(pr), 100)

    def test_machine_readable_original_pr_identifies_source(self):
        pr = pull_request(
            number=200,
            base_ref="release-3.0",
            body="Original-PR: risingwavelabs/risingwave#100",
        )
        self.assertEqual(canonical_pr_number(pr), 100)

    def test_release_pr_title_can_identify_original_pr(self):
        pr = pull_request(
            number=200,
            base_ref="release-3.0",
            title="cherry-pick fix: public bug (#100)",
            body="",
        )
        self.assertEqual(canonical_pr_number(pr), 100)

    def test_common_cherry_picks_body_identifies_original_pr(self):
        pr = pull_request(
            number=200,
            base_ref="release-3.0",
            title="fix: public bug (#100)",
            body="Cherry-picks #100 to `release-3.0`.",
        )
        self.assertEqual(canonical_pr_number(pr), 100)

    def test_release_pr_issue_suffix_is_not_enough_to_identify_backport(self):
        pr = pull_request(
            number=200,
            base_ref="release-3.0",
            title="fix: public bug (#100)",
            body="",
        )
        self.assertEqual(canonical_pr_number(pr), 200)

    def test_main_pr_title_reference_is_not_treated_as_backport(self):
        pr = pull_request(title="fix: public bug (#99)")
        self.assertEqual(canonical_pr_number(pr), 100)


class AnalyzeEventTest(unittest.TestCase):
    def test_merged_close_with_docs_checkbox_is_processed(self):
        result = analyze_event({"action": "closed", "pull_request": pull_request()})
        self.assertTrue(result.should_process)

    def test_relevant_label_added_after_merge_is_processed(self):
        result = analyze_event(
            {
                "action": "labeled",
                "label": {"name": "user-facing-changes"},
                "pull_request": pull_request(
                    body="", labels=["user-facing-changes"]
                ),
            }
        )
        self.assertTrue(result.should_process)

    def test_irrelevant_label_added_after_merge_is_ignored(self):
        result = analyze_event(
            {
                "action": "labeled",
                "label": {"name": "need-cherry-pick-since-release-2.8"},
                "pull_request": pull_request(labels=["user-facing-changes"]),
            }
        )
        self.assertFalse(result.should_process)
        self.assertTrue(result.reason.startswith("irrelevant_label:"))

    def test_unmerged_pr_is_ignored(self):
        result = analyze_event(
            {"action": "closed", "pull_request": pull_request(merged=False)}
        )
        self.assertFalse(result.should_process)

    def test_backport_uses_canonical_pr_checkbox(self):
        backport = pull_request(
            number=200,
            base_ref="release-3.0",
            body="Original-PR: risingwavelabs/risingwave#100",
            labels=["cherry-pick"],
        )
        canonical = pull_request(number=100)

        result = analyze_event(
            {"action": "closed", "pull_request": backport}, canonical
        )

        self.assertTrue(result.should_process)
        self.assertEqual(result.canonical_pr_number, 100)

    def test_backport_uses_canonical_pr_docs_label(self):
        backport = pull_request(
            number=200,
            base_ref="release-3.0",
            body="Original-PR: risingwavelabs/risingwave#100",
            labels=["cherry-pick"],
        )
        canonical = pull_request(
            number=100, body="", labels=["user-facing-changes"]
        )

        result = analyze_event(
            {"action": "closed", "pull_request": backport}, canonical
        )

        self.assertTrue(result.should_process)

    def test_backport_trigger_label_does_not_override_canonical_pr(self):
        backport = pull_request(
            number=200,
            base_ref="release-3.0",
            body="Original-PR: risingwavelabs/risingwave#100",
            labels=["user-facing-changes"],
        )
        canonical = pull_request(number=100, body="", labels=[])

        result = analyze_event(
            {
                "action": "labeled",
                "label": {"name": "user-facing-changes"},
                "pull_request": backport,
            },
            canonical,
        )

        self.assertFalse(result.should_process)
        self.assertEqual(result.reason, "documentation_not_required")


class AnalyzeEventWithCanonicalPullRequestTest(unittest.TestCase):
    def test_fetches_canonical_pr_for_backport_eligibility(self):
        backport = pull_request(
            number=200,
            base_ref="release-3.0",
            body="Original-PR: risingwavelabs/risingwave#100",
            labels=["cherry-pick"],
        )
        canonical = pull_request(number=100)

        class SourceApi:
            def request(self, method, path):
                self.last_request = (method, path)
                return canonical

        api = SourceApi()
        result = analyze_event_with_canonical_pr(
            api,
            "risingwavelabs/risingwave",
            {"action": "closed", "pull_request": backport},
        )

        self.assertTrue(result.should_process)
        self.assertEqual(result.canonical_pr_number, 100)
        self.assertEqual(
            api.last_request,
            ("GET", "/repos/risingwavelabs/risingwave/pulls/100"),
        )

    def test_irrelevant_backport_label_does_not_fetch_canonical_pr(self):
        backport = pull_request(
            number=200,
            base_ref="release-3.0",
            body="Original-PR: risingwavelabs/risingwave#100",
            labels=["cherry-pick"],
        )

        class SourceApi:
            def request(self, method, path):
                raise AssertionError((method, path))

        result = analyze_event_with_canonical_pr(
            SourceApi(),
            "risingwavelabs/risingwave",
            {
                "action": "labeled",
                "label": {"name": "cherry-pick"},
                "pull_request": backport,
            },
        )

        self.assertFalse(result.should_process)
        self.assertEqual(result.reason, "irrelevant_label:cherry-pick")

    def test_missing_canonical_pr_falls_back_to_trigger_pr_eligibility(self):
        backport = pull_request(
            number=200,
            base_ref="release-3.0",
            body="Original-PR: risingwavelabs/risingwave#100",
            labels=["cherry-pick"],
        )

        class SourceApi:
            def request(self, method, path):
                if path.endswith("/pulls/100"):
                    raise GitHubApiError(method, path, 404, "not found")
                if path.endswith("/pulls/200"):
                    return backport
                raise AssertionError((method, path))

        result = analyze_event_with_canonical_pr(
            SourceApi(),
            "risingwavelabs/risingwave",
            {"action": "closed", "pull_request": backport},
        )

        self.assertFalse(result.should_process)
        self.assertEqual(result.canonical_pr_number, 200)
        self.assertEqual(result.reason, "documentation_not_required")


class ResolveCanonicalPullRequestTest(unittest.TestCase):
    def test_missing_candidate_falls_back_to_trigger_pr(self):
        trigger_pr = {"number": 200}

        class SourceApi:
            def request(self, method, path):
                if path.endswith("/pulls/100"):
                    raise GitHubApiError(method, path, 404, "not found")
                if path.endswith("/pulls/200"):
                    return trigger_pr
                raise AssertionError((method, path))

        resolved = resolve_canonical_pr(
            SourceApi(), "risingwavelabs/risingwave", 100, trigger_pr
        )

        self.assertEqual(resolved, trigger_pr)

    def test_non_404_candidate_failure_is_not_hidden(self):
        trigger_pr = {"number": 200}

        class SourceApi:
            def request(self, method, path):
                raise GitHubApiError(method, path, 500, "server error")

        with self.assertRaises(GitHubApiError):
            resolve_canonical_pr(
                SourceApi(), "risingwavelabs/risingwave", 100, trigger_pr
            )


class TrackingBodyTest(unittest.TestCase):
    def test_marker_and_related_pr_are_added_once(self):
        marker = tracking_marker("risingwavelabs/risingwave", 100)
        pr = {
            "number": 100,
            "html_url": "https://github.com/risingwavelabs/risingwave/pull/100",
            "base": {"ref": "main"},
            "merged_at": "2026-07-20T00:00:00Z",
        }
        body = merge_tracking_body("Initial body\n", marker, [pr])
        repeated = merge_tracking_body(body, marker, [pr])

        self.assertEqual(body, repeated)
        self.assertEqual(body.count(marker), 1)
        self.assertEqual(body.count(pr["html_url"]), 1)

    def test_backport_is_appended_to_existing_issue(self):
        marker = tracking_marker("risingwavelabs/risingwave", 100)
        backport = {
            "number": 200,
            "html_url": "https://github.com/risingwavelabs/risingwave/pull/200",
            "base": {"ref": "release-3.0"},
            "merged_at": "2026-07-20T01:00:00Z",
        }
        body = merge_tracking_body(f"Initial body\n\n{marker}\n", marker, [backport])
        self.assertIn(backport["html_url"], body)
        self.assertIn("`release-3.0`", body)

    def test_backports_reuse_existing_related_prs_section(self):
        marker = tracking_marker("risingwavelabs/risingwave", 100)
        first_backport = {
            "number": 200,
            "html_url": "https://github.com/risingwavelabs/risingwave/pull/200",
            "base": {"ref": "release-3.0"},
            "merged_at": "2026-07-20T01:00:00Z",
        }
        second_backport = {
            "number": 201,
            "html_url": "https://github.com/risingwavelabs/risingwave/pull/201",
            "base": {"ref": "release-2.8"},
            "merged_at": "2026-07-20T02:00:00Z",
        }

        body = merge_tracking_body("Initial body\n", marker, [first_backport])
        updated = merge_tracking_body(body, marker, [second_backport])

        self.assertEqual(updated.count("## Related merged PRs"), 1)
        self.assertIn(first_backport["html_url"], updated)
        self.assertIn(second_backport["html_url"], updated)


class FindTrackingIssuesTest(unittest.TestCase):
    def test_search_results_are_filtered_and_deduplicated(self):
        marker = tracking_marker("risingwavelabs/risingwave", 100)
        canonical_url = "https://github.com/risingwavelabs/risingwave/pull/100"
        matching_issue = {"number": 1, "body": marker}
        unrelated_issue = {"number": 2, "body": "unrelated"}

        class SearchApi:
            def request(self, method, path):
                self.assert_request(method, path)
                return {"items": [matching_issue, unrelated_issue]}

            def assert_request(self, method, path):
                if method != "GET" or not path.startswith("/search/issues?"):
                    raise AssertionError((method, path))

        matches = find_tracking_issues(
            SearchApi(), "risingwavelabs/risingwave-docs", marker, canonical_url
        )

        self.assertEqual(matches, [matching_issue])

    def test_repository_scan_confirms_empty_search_result(self):
        marker = tracking_marker("risingwavelabs/risingwave", 100)
        canonical_url = "https://github.com/risingwavelabs/risingwave/pull/100"
        matching_issue = {"number": 1, "body": marker}

        class StaleSearchApi:
            def __init__(self):
                self.scan_calls = 0

            def request(self, method, path):
                if path.startswith("/search/issues?"):
                    return {"items": [], "incomplete_results": False}
                if path.startswith("/repos/risingwavelabs/risingwave-docs/issues?"):
                    self.scan_calls += 1
                    return [matching_issue]
                raise AssertionError((method, path))

        api = StaleSearchApi()
        matches = find_tracking_issues(
            api, "risingwavelabs/risingwave-docs", marker, canonical_url
        )

        self.assertEqual(matches, [matching_issue])
        self.assertEqual(api.scan_calls, 1)

    def test_repository_scan_is_used_when_search_fails(self):
        marker = tracking_marker("risingwavelabs/risingwave", 100)
        canonical_url = "https://github.com/risingwavelabs/risingwave/pull/100"
        matching_issue = {"number": 1, "body": marker}

        class FallbackApi:
            def request(self, method, path):
                if path.startswith("/search/issues?"):
                    raise RuntimeError("search unavailable")
                if path.startswith("/repos/risingwavelabs/risingwave-docs/issues?"):
                    return [matching_issue]
                raise AssertionError((method, path))

        matches = find_tracking_issues(
            FallbackApi(), "risingwavelabs/risingwave-docs", marker, canonical_url
        )

        self.assertEqual(matches, [matching_issue])

    def test_repository_scan_is_used_when_search_returns_http_error(self):
        marker = tracking_marker("risingwavelabs/risingwave", 100)
        canonical_url = "https://github.com/risingwavelabs/risingwave/pull/100"
        matching_issue = {"number": 1, "body": marker}

        class RateLimitedSearchApi:
            def request(self, method, path):
                if path.startswith("/search/issues?"):
                    raise GitHubApiError(method, path, 403, "rate limited")
                if path.startswith("/repos/risingwavelabs/risingwave-docs/issues?"):
                    return [matching_issue]
                raise AssertionError((method, path))

        matches = find_tracking_issues(
            RateLimitedSearchApi(),
            "risingwavelabs/risingwave-docs",
            marker,
            canonical_url,
        )

        self.assertEqual(matches, [matching_issue])


if __name__ == "__main__":
    unittest.main()
