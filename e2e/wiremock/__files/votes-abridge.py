#!/usr/bin/env python3
"""Abridge the votes fixtures copied from
`votes-pipeline/src/test/resources/wiremock/votes/__files/` so the docker-compose
E2E run only has to process the votes we actually have detail fixtures for.

- House list: keep only the roll call numbers present as members fixtures.
- Senate index XML: keep only the vote numbers present as detail XML fixtures.

Does NOT modify:
- The copied detail fixtures (they're already one-vote-per-file).
- The votes-pipeline's own test-resources fixtures (those are used by
  VotesPipelineE2ESpec which drives WireMock programmatically — it doesn't read
  the list / menu files; the E2E spec synthesises small mini-lists per scenario).

Input:  e2e/wiremock/__files/{house,senate}/*  (as copied from votes-pipeline)
Output: same files, overwritten with abridged content.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

HOUSE = Path(__file__).parent / "house"
SENATE = Path(__file__).parent / "senate"

# House votes we have member fixtures for — inferred from file names
# `house-vote-119-1-{roll}-members-*.json`.
HOUSE_ROLLS = set()
for f in HOUSE.glob("house-vote-119-1-*-members-*.json"):
    m = re.match(r"house-vote-119-1-(\d+)-members-", f.name)
    if m:
        HOUSE_ROLLS.add(int(m.group(1)))

# Senate votes we have detail XML for — inferred from file names
# `vote-119-1-{NNNNN}-*.xml`.
SENATE_VOTES = set()
for f in SENATE.glob("vote-119-1-*.xml"):
    m = re.match(r"vote-119-1-(\d+)-", f.name)
    if m:
        SENATE_VOTES.add(int(m.group(1)))


def abridge_house_list() -> None:
    path = HOUSE / "house-vote-119-1-page1.json"
    if not path.exists():
        return
    d = json.loads(path.read_text(encoding="utf-8"))
    votes = d.get("houseRollCallVotes", [])
    kept = [v for v in votes if v.get("rollCallNumber") in HOUSE_ROLLS]
    d["houseRollCallVotes"] = kept
    d["pagination"] = {"count": len(kept)}
    path.write_text(json.dumps(d, indent=2), encoding="utf-8")
    print(f"house list: {len(votes)} -> {len(kept)} (kept rolls {sorted(HOUSE_ROLLS)})")


def abridge_senate_index() -> None:
    path = SENATE / "vote-menu-119-1.xml"
    if not path.exists():
        return
    raw = path.read_text(encoding="utf-8")
    # Match each <vote>...</vote> block (non-greedy, multi-line).
    vote_pattern = re.compile(r"\s*<vote>.*?</vote>", re.DOTALL)
    kept_blocks = []
    for m in vote_pattern.finditer(raw):
        block = m.group(0)
        num_match = re.search(r"<vote_number>(\d+)</vote_number>", block)
        if num_match and int(num_match.group(1)) in SENATE_VOTES:
            kept_blocks.append(block)
    # Replace the entire <votes>...</votes> section with the kept blocks.
    votes_section = re.compile(r"<votes>.*?</votes>", re.DOTALL)
    new_inner = "\n".join(kept_blocks) + "\n  "
    replacement = f"<votes>{new_inner}</votes>"
    new_raw = votes_section.sub(replacement, raw, count=1)
    path.write_text(new_raw, encoding="utf-8")
    print(f"senate index: trimmed to {len(kept_blocks)} votes (kept {sorted(SENATE_VOTES)})")


def main() -> None:
    abridge_house_list()
    abridge_senate_index()


if __name__ == "__main__":
    main()
