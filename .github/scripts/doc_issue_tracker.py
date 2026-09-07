#!/usr/bin/env python3

"""Create or update one documentation issue for a canonical source PR."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DOC_CHECKBOX_RE = re.compile(
    r"-\s*\[x\].*My PR needs documentation updates\.", re.IGNORECASE
)
CHERRY_PICK_BODY_RES = (
    re.compile(
        r"Original-PR:\s+(?:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#(\d+)",
        re.IGNORECASE,
    ),
    re.compile(
        r"Original PR:\s+(?:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#(\d+)",
        re.IGNORECASE,
    ),
    re.compile(r"Cherry picking #(\d+) onto branch release-[^\s]+", re.IGNORECASE),
    re.compile(r"\bCherry[- ]pick(?:s|ed)?(?:\s+of)?\s+#(\d+)\b", re.IGNORECASE),
    re.compile(r"\bBackport(?:ed)?(?:\s+of)?\s+#(\d+)\b", re.IGNORECASE),
)
CHERRY_PICK_TITLE_RE = re.compile(
    r"\bcherry[- ]pick\b.*\(#(\d+)\)(?:\s+to\s+release-[^\s]+)?\s*$",
    re.IGNORECASE,
)
DOC_LABELS = {"user-facing-changes", "breaking-change"}


@dataclass(frozen=True)
class Analysis:
    should_process: bool
    canonical_pr_number: int
    reason: str


def load_event(path: str) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as event_file:
        return json.load(event_file)


def canonical_pr_number(pr: dict[str, Any]) -> int:
    """Return the original PR number for a known release-branch backport."""

    number = int(pr["number"])
    base_ref = pr.get("base", {}).get("ref", "")
    if not base_ref.startswith("release-"):
        return number

    body = pr.get("body") or ""
    for pattern in CHERRY_PICK_BODY_RES:
        if match := pattern.search(body):
            candidate = int(match.group(1))
            if candidate != number:
                return candidate

    if match := CHERRY_PICK_TITLE_RE.search(pr.get("title") or ""):
        candidate = int(match.group(1))
        if candidate != number:
            return candidate

    return number


def analyze_event(
    event: dict[str, Any], documentation_pr: dict[str, Any] | None = None
) -> Analysis:
    pr = event["pull_request"]
    number = (
        int(documentation_pr["number"])
        if documentation_pr is not None
        else canonical_pr_number(pr)
    )

    if not pr.get("merged", False):
        return Analysis(False, number, "pull_request_not_merged")

    action = event.get("action", "")
    if action == "labeled":
        added_label = event.get("label", {}).get("name", "")
        if added_label not in DOC_LABELS:
            return Analysis(False, number, f"irrelevant_label:{added_label}")
    elif action != "closed":
        return Analysis(False, number, f"unsupported_action:{action}")

    eligibility_pr = documentation_pr or pr
    labels = {label["name"] for label in eligibility_pr.get("labels", [])}
    body = eligibility_pr.get("body") or ""
    if not DOC_CHECKBOX_RE.search(body) and not labels.intersection(DOC_LABELS):
        return Analysis(False, number, "documentation_not_required")

    return Analysis(True, number, "documentation_required")


def analyze_event_with_canonical_pr(
    api: GitHubApi,
    source_repo: str,
    event: dict[str, Any],
) -> Analysis:
    trigger_pr = event["pull_request"]
    preliminary = analyze_event(event)
    if not preliminary.should_process and preliminary.reason != "documentation_not_required":
        return preliminary

    requested_number = preliminary.canonical_pr_number
    if requested_number == int(trigger_pr["number"]):
        return analyze_event(event)

    canonical_pr = resolve_canonical_pr(
        api, source_repo, requested_number, trigger_pr
    )
    return analyze_event(event, canonical_pr)


def tracking_marker(source_repo: str, canonical_number: int) -> str:
    return f"<!-- rw-doc-source-pr:{source_repo}#{canonical_number} -->"


def related_pr_line(pr: dict[str, Any]) -> str:
    number = pr["number"]
    url = pr["html_url"]
    base_ref = pr.get("base", {}).get("ref", "unknown")
    merged_at = pr.get("merged_at") or "unknown time"
    return f"- [#{number}]({url}) merged into `{base_ref}` at {merged_at}."


def merge_tracking_body(
    body: str,
    marker: str,
    related_prs: list[dict[str, Any]],
) -> str:
    missing_lines = [
        related_pr_line(pr) for pr in related_prs if pr["html_url"] not in body
    ]
    updated_body = body
    changed = False
    if marker not in updated_body:
        updated_body = updated_body.rstrip() + "\n\n" + marker
        changed = True

    if missing_lines:
        header_match = re.search(
            r"^## Related merged PRs\s*$", updated_body, re.MULTILINE
        )
        if header_match:
            next_header = re.search(
                r"^##\s+", updated_body[header_match.end() :], re.MULTILINE
            )
            insert_at = (
                header_match.end() + next_header.start()
                if next_header
                else len(updated_body)
            )
            prefix = updated_body[:insert_at].rstrip()
            suffix = updated_body[insert_at:].lstrip("\n")
            updated_body = prefix + "\n" + "\n".join(missing_lines)
            if suffix:
                updated_body += "\n\n" + suffix
        else:
            updated_body = (
                updated_body.rstrip()
                + "\n\n## Related merged PRs\n\n"
                + "\n".join(missing_lines)
            )
        changed = True

    if not changed:
        return body
    return updated_body.rstrip() + "\n"


class GitHubApiError(RuntimeError):
    def __init__(self, method: str, path: str, status: int, response_body: str):
        self.status = status
        super().__init__(
            f"GitHub API {method} {path} failed: {status} {response_body}"
        )


class GitHubApi:
    def __init__(self, token: str):
        self.token = token

    def request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> Any:
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(
            f"https://api.github.com{path}",
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            response_body = error.read().decode(errors="replace")
            raise GitHubApiError(
                method, path, error.code, response_body
            ) from error


def resolve_canonical_pr(
    api: GitHubApi,
    source_repo: str,
    requested_number: int,
    trigger_pr: dict[str, Any],
) -> dict[str, Any]:
    try:
        return api.request("GET", f"/repos/{source_repo}/pulls/{requested_number}")
    except GitHubApiError as error:
        trigger_number = int(trigger_pr["number"])
        if error.status != 404 or requested_number == trigger_number:
            raise
        print(
            f"warning: canonical PR #{requested_number} was not found; "
            f"falling back to triggering PR #{trigger_number}",
            file=sys.stderr,
        )
        return api.request("GET", f"/repos/{source_repo}/pulls/{trigger_number}")


def find_tracking_issues(
    api: GitHubApi,
    docs_repo: str,
    marker: str,
    canonical_url: str,
) -> list[dict[str, Any]]:
    needles = (marker, f"Source PR URL: {canonical_url}")
    try:
        matches_by_number: dict[int, dict[str, Any]] = {}
        for needle in needles:
            page = 1
            while True:
                query = urllib.parse.urlencode(
                    {
                        "q": f'"{needle}" in:body repo:{docs_repo} is:issue',
                        "per_page": 100,
                        "page": page,
                        "sort": "created",
                        "order": "asc",
                    }
                )
                result = api.request("GET", f"/search/issues?{query}")
                if result.get("incomplete_results", False):
                    raise RuntimeError("issue search returned incomplete results")
                issues = result.get("items", [])
                for issue in issues:
                    body = issue.get("body") or ""
                    if marker in body or f"Source PR URL: {canonical_url}" in body:
                        matches_by_number[issue["number"]] = issue
                if len(issues) < 100:
                    break
                page += 1
        matches = sorted(
            matches_by_number.values(), key=lambda issue: issue["number"]
        )
        if matches:
            return matches
        print(
            "warning: issue search returned no matches; "
            "confirming with repository scan",
            file=sys.stderr,
        )
    except RuntimeError as error:
        # GitHubApiError derives from RuntimeError, so HTTP failures such as
        # search rate limits and validation errors also use the exact scan.
        print(
            f"warning: issue search failed ({error}); falling back to repository scan",
            file=sys.stderr,
        )

    matches: list[dict[str, Any]] = []
    page = 1
    while True:
        query = urllib.parse.urlencode(
            {
                "state": "all",
                "per_page": 100,
                "page": page,
                "sort": "created",
                "direction": "asc",
            }
        )
        issues = api.request("GET", f"/repos/{docs_repo}/issues?{query}")
        for issue in issues:
            if "pull_request" in issue:
                continue
            body = issue.get("body") or ""
            if marker in body or f"Source PR URL: {canonical_url}" in body:
                matches.append(issue)
        if len(issues) < 100:
            break
        page += 1
    return matches


def write_output(name: str, value: str | int) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with Path(output_path).open("a", encoding="utf-8") as output_file:
            output_file.write(f"{name}={value}\n")
    else:
        print(f"{name}={value}")


def analyze_command(args: argparse.Namespace) -> None:
    source_token = os.environ.get("SOURCE_GITHUB_TOKEN")
    if not source_token:
        raise RuntimeError("SOURCE_GITHUB_TOKEN is required")
    analysis = analyze_event_with_canonical_pr(
        GitHubApi(source_token),
        args.source_repo,
        load_event(args.event_path),
    )
    write_output("should_process", str(analysis.should_process).lower())
    write_output("canonical_pr_number", analysis.canonical_pr_number)
    write_output("reason", analysis.reason)


def track_command(args: argparse.Namespace) -> None:
    event = load_event(args.event_path)
    trigger_pr = event["pull_request"]
    source_api = GitHubApi(args.source_token)
    docs_api = GitHubApi(args.docs_token)

    canonical_pr = resolve_canonical_pr(
        source_api,
        args.source_repo,
        args.canonical_pr_number,
        trigger_pr,
    )
    canonical_number = int(canonical_pr["number"])
    canonical_url = canonical_pr["html_url"]
    marker = tracking_marker(args.source_repo, canonical_number)
    related_prs = [canonical_pr]
    if trigger_pr["number"] != canonical_pr["number"]:
        related_prs.append(trigger_pr)

    matches = find_tracking_issues(docs_api, args.docs_repo, marker, canonical_url)
    if matches:
        issue = min(matches, key=lambda candidate: candidate["number"])
        updated_body = merge_tracking_body(issue.get("body") or "", marker, related_prs)
        if updated_body != (issue.get("body") or ""):
            docs_api.request(
                "PATCH",
                f"/repos/{args.docs_repo}/issues/{issue['number']}",
                {"body": updated_body},
            )
            action = "updated"
        else:
            action = "existing"
        if len(matches) > 1:
            duplicate_numbers = ", ".join(f"#{item['number']}" for item in matches)
            print(
                f"warning: multiple tracking issues already exist ({duplicate_numbers}); "
                f"using oldest issue #{issue['number']}",
                file=sys.stderr,
            )
    else:
        issue_body = (
            f"This issue tracks the documentation update needed for the merged PR "
            f"#{canonical_number}.\n\n"
            f"Source PR URL: {canonical_url}\n"
            f"Source PR Merged At: {canonical_pr.get('merged_at') or 'unknown'}\n\n"
            "If it is a major improvement that deserves a new page or a new section "
            "in the documentation, please check if we should label it as an "
            "experimental feature.\n"
        )
        issue_body = merge_tracking_body(issue_body, marker, related_prs)
        issue = docs_api.request(
            "POST",
            f"/repos/{args.docs_repo}/issues",
            {
                "title": f"Document: {canonical_pr['title']}",
                "body": issue_body,
            },
        )
        action = "created"

    write_output("action", action)
    write_output("canonical_pr_number", canonical_number)
    write_output("issue_number", issue["number"])
    write_output("issue_url", issue["html_url"])
    print(f"Documentation issue {action}: {issue['html_url']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    analyze = subparsers.add_parser("analyze")
    analyze.add_argument("--event-path", required=True)
    analyze.add_argument("--source-repo", required=True)
    analyze.set_defaults(func=analyze_command)

    track = subparsers.add_parser("track")
    track.add_argument("--event-path", required=True)
    track.add_argument("--source-repo", required=True)
    track.add_argument("--docs-repo", required=True)
    track.add_argument("--canonical-pr-number", required=True, type=int)
    track.add_argument("--source-token", required=True)
    track.add_argument("--docs-token", required=True)
    track.set_defaults(func=track_command)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
