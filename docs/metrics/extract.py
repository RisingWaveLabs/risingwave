#!/usr/bin/env python3
"""Extract every Prometheus metric registration in RisingWave src/ into a TSV."""
import os
import re
import sys
from pathlib import Path

# Resolve the repo root by walking up from this file (docs/metrics/extract.py).
REPO = Path(__file__).resolve().parent.parent.parent
SRC = REPO / "src"
OUT = REPO / "docs" / "metrics" / "inventory.tsv"

# Map every Rust registration macro to its Prometheus-canonical metric type.
# Consumers of the TSV (dashboard authors, alert writers) care about the
# Prometheus type — `counter`, `gauge`, or `histogram` — not the Rust spelling.
# Whether a metric is scalar or a vec is conveyed by the `labels` column being
# empty or not.
MACRO_TO_TYPE = {
    "register_int_counter_with_registry": "counter",
    "register_int_counter_vec_with_registry": "counter",
    "register_counter_with_registry": "counter",
    "register_counter_vec_with_registry": "counter",
    "register_histogram_with_registry": "histogram",
    "register_histogram_vec_with_registry": "histogram",
    "register_gauge_with_registry": "gauge",
    "register_gauge_vec_with_registry": "gauge",
    "register_int_gauge_with_registry": "gauge",
    "register_int_gauge_vec_with_registry": "gauge",
    "register_uint_gauge_with_registry": "gauge",
    "register_uint_gauge_vec_with_registry": "gauge",
    "register_guarded_histogram_vec_with_registry": "histogram",
    "register_guarded_gauge_vec_with_registry": "gauge",
    "register_guarded_int_gauge_vec_with_registry": "gauge",
    "register_guarded_uint_gauge_vec_with_registry": "gauge",
    "register_guarded_int_counter_vec_with_registry": "counter",
}

MACRO_RE = re.compile(
    r"\b(" + "|".join(re.escape(m) for m in MACRO_TO_TYPE) + r")!\s*\("
)


def find_matching_paren(text: str, open_idx: int) -> int:
    depth = 0
    i = open_idx
    in_str = False
    raw_hashes = 0
    while i < len(text):
        c = text[i]
        if in_str:
            if c == '\\' and raw_hashes == 0:
                i += 2
                continue
            if c == '"':
                if raw_hashes == 0:
                    in_str = False
                else:
                    if text[i + 1 : i + 1 + raw_hashes] == "#" * raw_hashes:
                        in_str = False
                        i += 1 + raw_hashes
                        continue
            i += 1
            continue
        if c == '"':
            in_str = True
            raw_hashes = 0
            j = i - 1
            hashes = 0
            while j >= 0 and text[j] == "#":
                hashes += 1
                j -= 1
            if j >= 0 and text[j] == "r":
                raw_hashes = hashes
            i += 1
            continue
        if c == "/" and i + 1 < len(text):
            nxt = text[i + 1]
            if nxt == "/":
                nl = text.find("\n", i)
                i = nl if nl >= 0 else len(text)
                continue
            if nxt == "*":
                end = text.find("*/", i + 2)
                i = (end + 2) if end >= 0 else len(text)
                continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def split_top_args(args: str):
    parts = []
    depth = 0
    in_str = False
    raw_hashes = 0
    buf = []
    i = 0
    while i < len(args):
        c = args[i]
        if in_str:
            buf.append(c)
            if c == '\\' and raw_hashes == 0:
                if i + 1 < len(args):
                    buf.append(args[i + 1])
                    i += 2
                    continue
            elif c == '"':
                if raw_hashes == 0:
                    in_str = False
                else:
                    if args[i + 1 : i + 1 + raw_hashes] == "#" * raw_hashes:
                        in_str = False
                        buf.append("#" * raw_hashes)
                        i += 1 + raw_hashes
                        continue
            i += 1
            continue
        if c == '"':
            in_str = True
            raw_hashes = 0
            j = i - 1
            hashes = 0
            while j >= 0 and args[j] == "#":
                hashes += 1
                j -= 1
            if j >= 0 and args[j] == "r":
                raw_hashes = hashes
            buf.append(c)
            i += 1
            continue
        # Skip comments (not in string).
        if c == "/" and i + 1 < len(args):
            nxt = args[i + 1]
            if nxt == "/":
                nl = args.find("\n", i)
                i = nl if nl >= 0 else len(args)
                continue
            if nxt == "*":
                end = args.find("*/", i + 2)
                i = (end + 2) if end >= 0 else len(args)
                continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    if buf:
        tail = "".join(buf).strip()
        if tail:
            parts.append(tail)
    return parts


STRING_RE = re.compile(r'(?:r(#*)"((?:[^"]|\\")*)"\1|"((?:[^"\\]|\\.)*)")', re.DOTALL)


def unescape_rust_string(s: str, raw: bool) -> str:
    if raw:
        return s
    # Handle line-continuation `\<newline>[whitespace]*` -> '' then common escapes.
    out = []
    i = 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            nxt = s[i + 1]
            if nxt == "\n":
                j = i + 2
                while j < len(s) and s[j] in " \t":
                    j += 1
                i = j
                continue
            if nxt == "n":
                out.append("\n")
                i += 2
                continue
            if nxt == "t":
                out.append("\t")
                i += 2
                continue
            if nxt == "r":
                out.append("\r")
                i += 2
                continue
            if nxt == "\\":
                out.append("\\")
                i += 2
                continue
            if nxt == '"':
                out.append('"')
                i += 2
                continue
            if nxt == "'":
                out.append("'")
                i += 2
                continue
            if nxt == "0":
                out.append("\0")
                i += 2
                continue
            # Unicode / hex escapes: leave as-is; rare in help strings.
            out.append(c)
            out.append(nxt)
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def extract_string(expr: str):
    expr = expr.strip()
    m = STRING_RE.match(expr)
    if not m:
        return None
    if m.group(2) is not None:
        return unescape_rust_string(m.group(2), raw=True)
    return unescape_rust_string(m.group(3), raw=False)


def extract_all_strings(expr: str):
    out = []
    for m in STRING_RE.finditer(expr):
        if m.group(2) is not None:
            out.append(unescape_rust_string(m.group(2), raw=True))
        else:
            out.append(unescape_rust_string(m.group(3), raw=False))
    return out


OPTS_RE = re.compile(r"\b(?:prometheus::)?(?:histogram_opts|opts)\s*!\s*\(")
GET_METRIC_NAME_RE = re.compile(r'get_metric_name\s*\(\s*"([^"]+)"\s*\)')
LET_OPTS_RE = re.compile(
    r"\blet\s+(?:mut\s+)?(\w+)\s*=\s*(?:prometheus::)?(?:histogram_opts|opts)\s*!\s*\("
)
LET_IDENT_RE = re.compile(r"\blet\s+(?:mut\s+)?(\w+)\s*=")
ANY_OPTS_RE = re.compile(r"\b(?:prometheus::)?(?:histogram_opts|opts)\s*!\s*\(")


def parse_opts_block(expr: str):
    m = OPTS_RE.search(expr)
    if not m:
        return None, None
    open_idx = m.end() - 1
    close = find_matching_paren(expr, open_idx)
    if close < 0:
        return None, None
    inner = expr[open_idx + 1 : close]
    args = split_top_args(inner)
    name = extract_string(args[0]) if len(args) >= 1 else None
    help_s = extract_string(args[1]) if len(args) >= 2 else None
    return name, help_s


def resolve_opts_identifier(text: str, macro_start: int, ident: str):
    """Search backward from macro_start for `let <ident> = ...` and find the first
    `histogram_opts!(...)` or `opts!(...)` call within that binding's expression.
    Return (name, help) or (None, None)."""
    start = max(0, macro_start - 32768)
    region = text[start:macro_start]
    best_pos = -1
    for m in LET_IDENT_RE.finditer(region):
        if m.group(1) == ident:
            best_pos = m.end()
    if best_pos < 0:
        return None, None
    # Find first opts! invocation after best_pos within region.
    sub = region[best_pos:]
    om = ANY_OPTS_RE.search(sub)
    if not om:
        return None, None
    open_idx = best_pos + om.end() - 1
    close = find_matching_paren(region, open_idx)
    if close < 0:
        return None, None
    inner = region[open_idx + 1 : close]
    args = split_top_args(inner)
    name = extract_string(args[0]) if len(args) >= 1 else None
    help_s = extract_string(args[1]) if len(args) >= 2 else None
    return name, help_s


def find_get_metric_name_closure(text: str, macro_start: int):
    """Detect `let get_metric_name = |name: &str| format!("rdkafka_{}_{}", path, name);` above.
    Return template string like 'rdkafka_{path}_{}' or None (where second {} filled by the
    actual arg). Simpler: just return the format string if found."""
    start = max(0, macro_start - 4096)
    region = text[start:macro_start]
    m = re.search(
        r'let\s+get_metric_name\s*=\s*\|[^|]*\|\s*format!\s*\(\s*"([^"]+)"',
        region,
    )
    if not m:
        return None
    return m.group(1)


LET_ARRAY_RE = re.compile(r"\blet\s+(?:mut\s+)?(\w+)(?:\s*:[^=]+)?\s*=\s*&?\s*\[")


def resolve_labels_identifier(text: str, macro_start: int, ident: str):
    """Resolve `let <ident> = ["a", "b", ...];` bound before macro_start.
    Returns comma-separated labels or '' if not found."""
    start = max(0, macro_start - 32768)
    region = text[start:macro_start]
    best_pos = -1
    for m in LET_ARRAY_RE.finditer(region):
        if m.group(1) == ident:
            # End of match might be just after '[' (or after '&['); back up to the '['.
            pos = m.end() - 1
            while pos > m.start() and region[pos] != "[":
                pos -= 1
            best_pos = pos
    if best_pos < 0:
        return ""
    # Find matching ']'.
    depth = 0
    i = best_pos
    while i < len(region):
        c = region[i]
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                inner = region[best_pos + 1 : i]
                strings = extract_all_strings(inner)
                return ",".join(strings)
        i += 1
    return ""


def extract_labels(expr: str, text: str = "", macro_start: int = -1):
    expr_s = expr.strip()
    strings = extract_all_strings(expr_s)
    if strings:
        return ",".join(strings)
    # Try to resolve `&ident` or `ident`
    m = re.match(r"^\s*&?\s*([A-Za-z_]\w*)\s*(?:\[[^\]]*\])?\s*$", expr_s)
    if m and text:
        return resolve_labels_identifier(text, macro_start, m.group(1))
    return ""


def clean_field(s: str) -> str:
    # Normalize control chars so the TSV stays valid but preserve verbatim
    # whitespace (including leading/trailing spaces) so help strings match source.
    return s.replace("\t", " ").replace("\r", " ").replace("\n", " ")


def compute_test_ranges(text: str):
    """Return a list of (start_offset, end_offset) ranges that are inside
    a #[cfg(test)] attribute's item (mod or fn), so we can skip them."""
    ranges = []
    i = 0
    while True:
        idx = text.find("#[cfg(test)]", i)
        if idx < 0:
            break
        # Advance to start of next item — find next '{' (for mod) or start of fn block.
        j = idx + len("#[cfg(test)]")
        # Find the item's opening brace.
        brace = text.find("{", j)
        if brace < 0:
            break
        # Find matching closing brace.
        depth = 1
        k = brace + 1
        while k < len(text) and depth > 0:
            c = text[k]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
            k += 1
        ranges.append((idx, k))
        i = k
    return ranges


def in_any_range(offset: int, ranges):
    for s, e in ranges:
        if s <= offset < e:
            return True
    return False


def process_file(path: Path, rel: str):
    text = path.read_text(encoding="utf-8", errors="replace")
    test_ranges = compute_test_ranges(text)
    line_starts = [0]
    for i, c in enumerate(text):
        if c == "\n":
            line_starts.append(i + 1)

    def line_of(offset: int) -> int:
        lo, hi = 0, len(line_starts) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if line_starts[mid] <= offset:
                lo = mid + 1
            else:
                hi = mid - 1
        return hi + 1

    rows = []
    for m in MACRO_RE.finditer(text):
        if in_any_range(m.start(), test_ranges):
            continue
        macro = m.group(1)
        mtype = MACRO_TO_TYPE[macro]
        is_vec = "vec" in macro
        is_hist = "histogram" in macro
        open_idx = m.end() - 1
        close_idx = find_matching_paren(text, open_idx)
        if close_idx < 0:
            continue
        args_raw = text[open_idx + 1 : close_idx]
        args = [a for a in split_top_args(args_raw) if a]
        name = None
        help_s = None
        labels = ""
        buckets = ""
        if args:
            first = args[0].strip()
            s = extract_string(first)
            if s is not None:
                name = s
                if len(args) >= 2:
                    help_s = extract_string(args[1])
                if is_vec and len(args) >= 3:
                    labels = extract_labels(args[2], text, m.start())
                if is_hist and not is_vec and len(args) >= 3 and "vec!" in args[2]:
                    buckets = clean_field(args[2])
            else:
                # Handle get_metric_name("X") templated names (kafka stats)
                gm = GET_METRIC_NAME_RE.search(first)
                if gm and not OPTS_RE.search(first):
                    tmpl = find_get_metric_name_closure(text, m.start())
                    suffix = gm.group(1)
                    if tmpl:
                        # tmpl looks like "rdkafka_{}_{}" where first {} is runtime path,
                        # second is the suffix. Represent as rdkafka_<PATH>_<suffix>.
                        # Strip known prefix pattern and render a template.
                        name = tmpl.replace("{}", "<PATH>", 1).replace("{}", suffix, 1)
                    else:
                        name = f"<templated>_{suffix}"
                    # help is second arg
                    if len(args) >= 2:
                        help_s = extract_string(args[1])
                    if is_vec and len(args) >= 3:
                        labels = extract_labels(args[2], text, m.start())
                elif OPTS_RE.search(first):
                    n, h = parse_opts_block(first)
                    name = n
                    help_s = h
                    if is_vec and len(args) >= 2:
                        labels = extract_labels(args[1], text, m.start())
                else:
                    # Bare identifier (e.g. `opts`) — resolve via backward scan.
                    ident_match = re.match(r"^([A-Za-z_]\w*)$", first)
                    if ident_match:
                        n, h = resolve_opts_identifier(
                            text, m.start(), ident_match.group(1)
                        )
                        name = n
                        help_s = h
                        if is_vec and len(args) >= 2:
                            labels = extract_labels(args[1], text, m.start())
        rows.append(
            (
                clean_field(name or ""),
                mtype,
                clean_field(labels),
                clean_field(help_s or ""),
                clean_field(buckets),
                rel,
                line_of(m.start()),
            )
        )
    return rows


WRAPPER_SITES = {
    # (file, line) — these register_* invocations are inside wrapper defs, not real metrics.
    ("src/common/metrics/src/error_metrics.rs", 29),
    ("src/common/metrics/src/metrics.rs", 37),
    # `"test"` fixtures exposed via test_*_vec() helpers on LabelGuarded* types.
    ("src/common/metrics/src/guarded_metrics.rs", 293),
    ("src/common/metrics/src/guarded_metrics.rs", 301),
    ("src/common/metrics/src/guarded_metrics.rs", 309),
    ("src/common/metrics/src/guarded_metrics.rs", 317),
}


ERROR_METRIC_NEW_RE = re.compile(r"ErrorMetric::new\s*\(")

DIRECT_NEW_TYPES = {
    "IntCounter": "counter",
    "Counter": "counter",
    "IntGauge": "gauge",
    "Gauge": "gauge",
    "UintGauge": "gauge",
    "TrAdderGauge": "gauge",
    "GenericCounter": "counter",
    "GenericGauge": "gauge",
    "Histogram": "histogram",
}
DIRECT_NEW_RE = re.compile(
    r"\b(" + "|".join(re.escape(t) for t in DIRECT_NEW_TYPES) + r")\s*(?:::<[^>]*>)?\s*::\s*new\s*\("
)
DIRECT_WITH_OPTS_RE = re.compile(
    r"\b(" + "|".join(re.escape(t) for t in DIRECT_NEW_TYPES) + r")\s*(?:::<[^>]*>)?\s*::\s*with_opts\s*\("
)


def scan_direct_constructions(path: Path, rel: str, test_ranges):
    """Capture `TypeName::new("name", "help", ...)` and `TypeName::with_opts(Opts::new("name", "help", ...))`
    patterns for IntCounter / IntGauge / UintGauge / TrAdderGauge / GenericCounter / etc."""
    text = path.read_text(encoding="utf-8", errors="replace")
    line_starts = [0]
    for i, c in enumerate(text):
        if c == "\n":
            line_starts.append(i + 1)

    def line_of(offset: int) -> int:
        lo, hi = 0, len(line_starts) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if line_starts[mid] <= offset:
                lo = mid + 1
            else:
                hi = mid - 1
        return hi + 1

    rows = []

    for m in DIRECT_NEW_RE.finditer(text):
        if in_any_range(m.start(), test_ranges):
            continue
        type_name = m.group(1)
        open_idx = m.end() - 1
        close_idx = find_matching_paren(text, open_idx)
        if close_idx < 0:
            continue
        args = [a for a in split_top_args(text[open_idx + 1 : close_idx]) if a]
        if len(args) < 2:
            continue
        name = extract_string(args[0])
        if not name:
            continue
        help_s = extract_string(args[1]) or ""
        rows.append(
            (
                clean_field(name),
                DIRECT_NEW_TYPES[type_name],
                "",
                clean_field(help_s),
                "",
                rel,
                line_of(m.start()),
            )
        )

    for m in DIRECT_WITH_OPTS_RE.finditer(text):
        if in_any_range(m.start(), test_ranges):
            continue
        type_name = m.group(1)
        open_idx = m.end() - 1
        close_idx = find_matching_paren(text, open_idx)
        if close_idx < 0:
            continue
        inner = text[open_idx + 1 : close_idx]
        # inner is Opts::new("n", "h") or opts!("n", "h")
        om = re.match(r"\s*(?:prometheus::)?(?:Opts|HistogramOpts)\s*::\s*new\s*\(", inner)
        if om:
            open2 = om.end() - 1
            close2 = find_matching_paren(inner, open2)
            if close2 < 0:
                continue
            sub_args = [a for a in split_top_args(inner[open2 + 1 : close2]) if a]
            if len(sub_args) < 2:
                continue
            name = extract_string(sub_args[0])
            if not name:
                continue
            help_s = extract_string(sub_args[1]) or ""
            rows.append(
                (
                    clean_field(name),
                    DIRECT_NEW_TYPES[type_name],
                    "",
                    clean_field(help_s),
                    "",
                    rel,
                    line_of(m.start()),
                )
            )

    return rows


def scan_error_metric_callsites():
    """Find ErrorMetric::new('name', 'help', &['labels'], ...) calls and emit rows."""
    rows = []
    target = REPO / "src/common/metrics/src/error_metrics.rs"
    text = target.read_text(encoding="utf-8", errors="replace")
    line_starts = [0]
    for i, c in enumerate(text):
        if c == "\n":
            line_starts.append(i + 1)

    def line_of(offset: int) -> int:
        lo, hi = 0, len(line_starts) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if line_starts[mid] <= offset:
                lo = mid + 1
            else:
                hi = mid - 1
        return hi + 1

    for m in ERROR_METRIC_NEW_RE.finditer(text):
        open_idx = m.end() - 1
        close_idx = find_matching_paren(text, open_idx)
        if close_idx < 0:
            continue
        args = [a for a in split_top_args(text[open_idx + 1 : close_idx]) if a]
        if len(args) < 3:
            continue
        name = extract_string(args[0])
        if not name:
            continue
        help_s = extract_string(args[1]) or ""
        labels = extract_labels(args[2])
        rows.append(
            (
                clean_field(name),
                "counter",
                clean_field(labels),
                clean_field(help_s),
                "",
                "src/common/metrics/src/error_metrics.rs",
                line_of(m.start()),
            )
        )
    return rows


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Extract every Prometheus metric registration in RisingWave src/. "
            "By default writes to docs/metrics/inventory.tsv and prints a summary "
            "to stdout. With --stdout, writes the TSV to stdout (no summary, "
            "suitable for CI diff)."
        )
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Write the TSV to stdout instead of the inventory file. "
        "Suppresses the summary so CI can diff against the committed file.",
    )
    args = parser.parse_args()

    rows = []
    for p in SRC.rglob("*.rs"):
        rel = str(p.relative_to(REPO))
        if "/target/" in rel or rel.startswith("target/"):
            continue
        text = p.read_text(encoding="utf-8", errors="replace")
        tr = compute_test_ranges(text)
        rows.extend(process_file(p, rel))
        # Direct-construction patterns only in files that look like metric/monitor code
        # to avoid false positives (IntCounter::new exists elsewhere as a helper).
        if any(
            frag in rel
            for frag in ("monitor/", "metrics/", "/metrics.rs", "/stats.rs")
        ):
            rows.extend(scan_direct_constructions(p, rel, tr))
    # Drop wrapper definition sites.
    rows = [r for r in rows if (r[5], r[6]) not in WRAPPER_SITES]
    # Add ErrorMetric::new callsites.
    rows.extend(scan_error_metric_callsites())
    # Dedup: same (name, file, line) can appear from overlapping patterns.
    seen = set()
    deduped = []
    for r in rows:
        key = (r[0], r[5], r[6])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    rows = deduped

    rows.sort(key=lambda r: (r[5], r[0]))

    header = "metric_name\ttype\tlabels\thelp\tbuckets\tfile\n"
    if args.stdout:
        out = sys.stdout
        out.write(header)
        for r in rows:
            # Drop the line column (index 6) — file path + metric name is enough
            # to locate, and line numbers churn on every unrelated edit.
            out.write("\t".join(str(x) for x in r[:6]) + "\n")
        return

    with OUT.open("w", encoding="utf-8") as f:
        f.write(header)
        for r in rows:
            f.write("\t".join(str(x) for x in r[:6]) + "\n")

    print(f"total_rows\t{len(rows)}")
    from collections import Counter

    by_type = Counter(r[1] for r in rows)
    print("by_type:")
    for t, n in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {t}\t{n}")
    by_crate = Counter(
        r[5].split("/")[1] if r[5].startswith("src/") else "other" for r in rows
    )
    print("by_crate:")
    for c, n in sorted(by_crate.items(), key=lambda x: -x[1]):
        print(f"  {c}\t{n}")
    name_count = Counter(r[0] for r in rows if r[0])
    dups = [(n, c) for n, c in name_count.items() if c > 1]
    dups.sort(key=lambda x: -x[1])
    print(f"duplicate_names\t{len(dups)}")
    for n, c in dups[:15]:
        print(f"  {n}\t{c}")
    missing_name = [r for r in rows if not r[0]]
    print(f"missing_name_rows\t{len(missing_name)}")
    for r in missing_name[:10]:
        print(f"  {r[5]}:{r[6]}\t{r[1]}")


if __name__ == "__main__":
    main()
