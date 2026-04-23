#!/usr/bin/env python3
"""Abridge real Congress.gov responses for use as WireMock fixtures.

- Trims long arrays to representative samples so fixture files stay readable.
- Rewrites `https://api.congress.gov` -> `http://wiremock:8080` so HATEOAS
  navigation loops back into WireMock instead of hitting the real API.
- Rewrites `https://www.congress.gov` -> `http://wiremock:8080/www` for any
  legislationUrl fields (harmless if unused; keeps the container network closed).
- Synthesises a bill-list response containing only the single HR-1 item.

Input:  e2e/wiremock/__files/.raw/*.json     (raw Congress.gov responses)
Output: e2e/wiremock/__files/*.json          (abridged fixtures)
"""

from __future__ import annotations

import json
import re
from pathlib import Path

RAW = Path(__file__).parent
OUT = RAW.parent

URL_REWRITES = [
    ("https://api.congress.gov", "http://wiremock:8080"),
    ("https://www.congress.gov", "http://wiremock:8080/www"),
]


def rewrite_urls(obj):
    if isinstance(obj, dict):
        return {k: rewrite_urls(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [rewrite_urls(x) for x in obj]
    if isinstance(obj, str):
        out = obj
        for src, dst in URL_REWRITES:
            out = out.replace(src, dst)
        return out
    return obj


def trim_list(d: dict, key: str, keep: int) -> dict:
    if key in d and isinstance(d[key], list) and len(d[key]) > keep:
        d[key] = d[key][:keep]
    return d


def load(name: str) -> dict:
    return json.loads((RAW / name).read_text(encoding="utf-8"))


def save(name: str, obj) -> None:
    (OUT / name).write_text(
        json.dumps(obj, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    size = (OUT / name).stat().st_size
    print(f"wrote {name} ({size} bytes)")


def main() -> None:
    # --- detail ---
    detail = load("detail.json")
    # Drop the `request` envelope (WireMock doesn't need it, it's echo metadata).
    detail.pop("request", None)
    # Trim long nested arrays inside bill{}
    bill = detail.get("bill", {})
    trim_list(bill, "sponsors", 1)
    trim_list(bill, "titles", 2)
    trim_list(bill, "committees", 1)
    if "subjects" in bill and isinstance(bill["subjects"], dict):
        trim_list(bill["subjects"], "legislativeSubjects", 3)
    detail = rewrite_urls(detail)
    save("bill-detail-response.json", detail)

    # --- cosponsors: keep 3 ---
    cos = load("cosponsors.json")
    cos.pop("request", None)
    trim_list(cos, "cosponsors", 3)
    # Pagination count reflects original total; keep it honest.
    cos = rewrite_urls(cos)
    save("bill-cosponsors-response.json", cos)

    # --- summaries: keep both (only 2) ---
    sums = load("summaries.json")
    sums.pop("request", None)
    sums = rewrite_urls(sums)
    save("bill-summaries-response.json", sums)

    # --- actions: keep 5 ---
    acts = load("actions.json")
    acts.pop("request", None)
    trim_list(acts, "actions", 5)
    acts = rewrite_urls(acts)
    save("bill-actions-response.json", acts)

    # --- text: keep both versions ---
    txt = load("text.json")
    txt.pop("request", None)
    txt = rewrite_urls(txt)
    save("bill-text-versions-response.json", txt)

    # --- list: synthesize one-entry list from detail ---
    src = detail["bill"]
    list_response = {
        "bills": [
            {
                "congress": src["congress"],
                "number": src["number"],
                "originChamber": src["originChamber"],
                "originChamberCode": src["originChamberCode"],
                "title": src["title"],
                "type": src["type"],
                "updateDate": src["updateDate"],
                "updateDateIncludingText": src["updateDateIncludingText"],
                "url": f"http://wiremock:8080/v3/bill/{src['congress']}/{src['type'].lower()}/{src['number']}?format=json",
                "latestAction": src.get("latestAction"),
            }
        ],
        "pagination": {"count": 1, "next": None},
    }
    save("bill-list-response.json", list_response)


if __name__ == "__main__":
    main()
