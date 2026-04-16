#!/usr/bin/env python3
"""Enforce per-file statement coverage across all sbt-scoverage reports.

Parses every `scoverage.xml` found under any subproject's
`target/scala-*/scoverage-report/` directory, computes statement coverage per
source file, and fails (exit 1) if any file is below the configured threshold.

Exclusions: sbt-scoverage's `coverageExcludedFiles` setting already omits
excluded files from the XML, so they are naturally skipped.

Usage:
    python3 scripts/check-coverage.py                 # default threshold 95%
    python3 scripts/check-coverage.py --min 90        # custom threshold
    python3 scripts/check-coverage.py --reports 'a/b/scoverage.xml' ...
"""
from __future__ import annotations

import argparse
import glob
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


DEFAULT_GLOB = "**/target/scala-*/scoverage-report/scoverage.xml"


@dataclass(frozen=True)
class FileCoverage:
    filename: str
    statement_rate: float
    statements_invoked: int
    statement_count: int
    report_path: Path


def parse_report(xml_path: Path) -> list[FileCoverage]:
    """Extract per-class coverage records from a scoverage.xml file."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    out: list[FileCoverage] = []
    for cls in root.iter("class"):
        filename = cls.get("filename", "")
        if not filename:
            continue
        try:
            rate = float(cls.get("statement-rate", "0"))
            invoked = int(cls.get("statements-invoked", "0"))
            count = int(cls.get("statement-count", "0"))
        except ValueError:
            continue
        if count == 0:
            # Files with no instrumented statements (e.g., marker traits)
            # are vacuously covered — skip so they don't dilute the report.
            continue
        out.append(
            FileCoverage(
                filename=filename,
                statement_rate=rate,
                statements_invoked=invoked,
                statement_count=count,
                report_path=xml_path,
            )
        )
    return out


def aggregate(reports: list[Path]) -> dict[str, FileCoverage]:
    """Merge reports, keeping the highest coverage record per file.

    The same shared-project source file (e.g., in `bills-common`) appears in
    multiple downstream subprojects' reports. Taking the max avoids a file
    being penalized because one downstream subproject only exercised part of
    it — what matters is the union of test coverage across the whole build.
    """
    best: dict[str, FileCoverage] = {}
    for path in reports:
        for cov in parse_report(path):
            prior = best.get(cov.filename)
            if prior is None or cov.statement_rate > prior.statement_rate:
                best[cov.filename] = cov
    return best


def format_table(rows: list[FileCoverage]) -> str:
    if not rows:
        return ""
    name_width = max(len(r.filename) for r in rows)
    lines = [
        f"  {'%':>6}  {'invoked/total':>14}  {'file':<{name_width}}",
        f"  {'-' * 6}  {'-' * 14}  {'-' * name_width}",
    ]
    for r in rows:
        lines.append(
            f"  {r.statement_rate:6.2f}  "
            f"{r.statements_invoked:>6}/{r.statement_count:<6}  "
            f"{r.filename:<{name_width}}"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--min",
        type=float,
        default=95.0,
        help="Minimum per-file statement coverage percentage (default: 95.0)",
    )
    parser.add_argument(
        "--reports",
        nargs="+",
        default=None,
        help=f"scoverage.xml paths or globs (default: {DEFAULT_GLOB})",
    )
    args = parser.parse_args()

    # Resolve report paths. If the user passed explicit paths, use them as-is;
    # otherwise glob from the current working directory.
    if args.reports:
        paths: list[Path] = []
        for pattern in args.reports:
            matches = glob.glob(pattern, recursive=True)
            if matches:
                paths.extend(Path(m) for m in matches)
            elif Path(pattern).exists():
                paths.append(Path(pattern))
    else:
        paths = [Path(p) for p in glob.glob(DEFAULT_GLOB, recursive=True)]

    if not paths:
        pattern_desc = " ".join(args.reports) if args.reports else DEFAULT_GLOB
        print(
            f"ERROR: no scoverage reports found matching: {pattern_desc}\n"
            f"  Run `sbt coverage test coverageReport` first.",
            file=sys.stderr,
        )
        return 2

    print(f"Loading {len(paths)} scoverage report(s):")
    for p in sorted(paths):
        print(f"  {p}")
    print()

    file_coverage = aggregate(paths)
    if not file_coverage:
        print("ERROR: reports contained no coverable files.", file=sys.stderr)
        return 2

    violations = sorted(
        (cov for cov in file_coverage.values() if cov.statement_rate < args.min),
        key=lambda c: (c.statement_rate, c.filename),
    )

    print(
        f"Checked {len(file_coverage)} file(s) "
        f"against minimum {args.min:.2f}% statement coverage.\n"
    )

    if violations:
        print(f"FAIL: {len(violations)} file(s) below threshold:\n")
        print(format_table(violations))
        print()
        return 1

    print("PASS: all files meet minimum coverage threshold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
