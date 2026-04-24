#!/usr/bin/env python3
"""Abridge real Congress.gov + senate.gov responses for use as WireMock fixtures.

Three endpoints covered here (compare bills-abridge.py for the bill-side):
  - /v3/member/congress/119     (member list)
  - /v3/member/{bioguideId}     (member detail; HATEOAS-followed from list)
  - senate.gov /about/senator-lookup.xml    (lis-mapping-refresher)

- Trims long nested arrays in member detail (partyHistory, terms, sponsored-
  legislation).
- Rewrites `https://api.congress.gov` -> `http://wiremock:8080` so HATEOAS
  navigation (list -> detail) loops back into WireMock instead of hitting the
  real API.
- Trims senator-lookup.xml to 5 representative currently-serving senators with
  <lisid> set (the pipeline's SenatorXmlParser silently drops senators without
  a lisid). Historical senators without modern LIS IDs would bloat the fixture
  without exercising new code paths.

NOTE on senator-lookup.xml vs senators_cfm.xml:
  lis-mapping-refresher's SenatorXmlParser expects the senator-lookup.xml
  schema (<senators><senator><full_name><first_name/>...). The production
  application.conf default URL is `senators_cfm.xml` which has a completely
  different schema (<contact_information><member>...) — latent bug surfaced
  by this Tier 3 run. Compose overrides via PIPELINE_SENATOR_XML_URL so we
  point at the right URL shape. Prod-config fix tracked separately.

Input:  e2e/wiremock/__files/.raw/{members-list,member-detail}.json + senator-lookup.xml
Output: e2e/wiremock/__files/members/*.json + senate/senator-lookup.xml
"""

from __future__ import annotations

import json
import re
from pathlib import Path

RAW = Path(__file__).parent / ".raw"
OUT_MEMBERS = Path(__file__).parent / "members"
OUT_SENATE = Path(__file__).parent / "senate"

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


def save(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    size = path.stat().st_size
    print(f"wrote {path.relative_to(Path(__file__).parent)} ({size} bytes)")


def abridge_members_list() -> None:
    data = load("members-list.json")
    data.pop("request", None)
    # Keep ONLY the first member so every list-follow lands on a detail fixture
    # we actually have. With multiple members, the WireMock mapping
    # /v3/member/[A-Z][0-9]{6} would serve the same detail body for every
    # bioguide — which then confuses the pipeline's post-upsert re-read by
    # natural_key (the second member's natural_key never lands in the DB).
    members = data.get("members", [])
    data["members"] = members[:1]
    # Keep pagination.count honest — still reflects original total, but the
    # pipeline only paginates by `next` URL which is None at limit=2.
    data = rewrite_urls(data)
    save(OUT_MEMBERS / "members-list-response.json", data)


def abridge_member_detail() -> None:
    data = load("member-detail.json")
    data.pop("request", None)
    member = data.get("member", {})
    # Trim long nested arrays — they're not required for the pipeline's flow
    # but they do bloat the fixture.
    trim_list(member, "partyHistory", 2)
    trim_list(member, "terms", 2)
    trim_list(member, "leadership", 2)
    # sponsoredLegislation + cosponsoredLegislation are summary-only structures
    # (count + url), not arrays.
    data = rewrite_urls(data)
    save(OUT_MEMBERS / "member-detail-response.json", data)


def abridge_senator_lookup_xml() -> None:
    """
    senator-lookup.xml is the senate.gov senator feed that lis-mapping-
    refresher's `SenatorXmlParser` actually reads. Structure:

      <?xml version="1.0" encoding="UTF-8"?>
      <senators>
        <senator>
          <full_name>
            <first_name>Tammy</first_name>
            <last_name>Baldwin</last_name>
          </full_name>
          <party>D</party>
          <state>WI</state>
          <bioguide>B001230</bioguide>
          <lisid>S354</lisid>
          <service_dates>
            <service_date class="1">
              <begin_date .../>
              <end_date year="" .../>   <!-- empty year == currently serving -->
            </service_date>
          </service_dates>
        </senator>
      </senators>

    Full feed has ~1919 senators (historical + current); the parser silently
    drops senators without <lisid>, leaving ~131 usable blocks. Trim to 5
    representative currently-serving senators (non-empty lisid AND at least
    one <end_date year=""> tag) so the fixture stays readable but every
    parser code path fires.
    """
    import xml.etree.ElementTree as ET
    raw = (RAW / "senator-lookup.xml").read_text(encoding="utf-8")
    tree = ET.fromstring(raw)
    senators = tree.findall("senator")
    # Select: lisid present + at least one service_date/end_date has year="".
    usable: list[ET.Element] = []
    for s in senators:
        lisid = s.find("lisid")
        if lisid is None or not (lisid.text or "").strip():
            continue
        current = any(
            (ed.get("year") or "") == ""
            for sd in s.findall("service_dates/service_date")
            for ed in sd.findall("end_date")
        )
        if current:
            usable.append(s)
    kept = usable[:5]
    # Rebuild the document with just our kept senators, re-serialized cleanly.
    out_root = ET.Element("senators")
    for s in kept:
        out_root.append(s)
    new_raw = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        + ET.tostring(out_root, encoding="unicode")
        + "\n"
    )
    OUT_SENATE.mkdir(parents=True, exist_ok=True)
    out_path = OUT_SENATE / "senator-lookup.xml"
    out_path.write_text(new_raw, encoding="utf-8")
    size = out_path.stat().st_size
    print(
        f"wrote senate/senator-lookup.xml ({size} bytes, {len(kept)} of {len(senators)} "
        f"<senator> blocks kept — {len(usable)} usable (lisid set + currently serving)"
    )


def main() -> None:
    abridge_members_list()
    abridge_member_detail()
    abridge_senator_lookup_xml()


if __name__ == "__main__":
    main()
