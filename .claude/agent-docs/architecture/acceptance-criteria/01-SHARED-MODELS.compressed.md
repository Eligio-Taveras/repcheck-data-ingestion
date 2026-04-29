<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/acceptance-criteria/01-SHARED-MODELS.md -->

# `repcheck-shared-models` — Compressed Context

Root domain library. Published to GitHub Packages. No repcheck dependencies. Package: `repcheck.shared.models` with sub-packages: `congress.{bill,member,vote,amendment,committee,common,xml}`, `user`, `llm`, `analysis`, `prompt`, `placeholder`, `codecs`, `constants`.

| Area | Status | Description |
|------|--------|-------------|
| 1.1 Legislative DTOs | Migrate + Extend | Congress.gov API response shapes + chamber XML source DTOs |
| 1.2 Legislative DOs | Migrate + Extend | AlloyDB storage shapes + `toDO` conversions |
| 1.3 Committee DTOs & DOs | New | Committee membership from official chamber XML feeds |
| 1.4 Bill Text Versioning | New | Multi-version text storage + text diff analysis |
| 1.5 User Domain Objects | New | User, preference, Q&A, stance domain objects |
| 1.6 LLM Output Schemas | New | Structured JSON schemas for LLM responses |
| 1.7 Prompt Chain Base Traits | New | InstructionBlock, PromptProfile, ChainAssembler |
| 1.8 Shared Serializers & Constants | Migrate + Extend | Circe codecs, enums, constants |
| 1.9 Analysis Domain Objects | New | BillAnalysisDO, BillFindingDO, BillConceptSummaryDO, AmendmentFindingDO, FindingTypeDO |

## Routing

- Congress.gov API DTOs (bill, member, vote, amendment) → 1.1
- AlloyDB storage DOs, toDO conversions → 1.2
- Committee membership (chamber XML) → 1.3
- Multi-version bill text, text diffs → 1.4
- User, preference, Q&A, stance DOs → 1.5
- LLM structured response schemas → 1.6
- InstructionBlock, PromptProfile, ChainAssembler → 1.7
- Circe codecs, enums, constants → 1.8
- Analysis DOs (bill, findings, concepts, fiscal, amendments) → 1.9

## Build & Quality

- Publish versioned artifact to GitHub Packages.
- Dependencies: Circe (semi-auto), Cats (core only), Doobie (Read/Write).
- All code passes `sbt compile` (WartRemover + tpolecat), `scalafmtCheckAll`, `scalafixAll --check`.
- Test coverage >90% (Codecov patch enforcement).
- No `@nowarn` or `@SuppressWarnings`.
- Curly brace syntax only.
- All public classes require ScalaDoc (purpose + source: Congress.gov schema, DB table, or behavioral spec).