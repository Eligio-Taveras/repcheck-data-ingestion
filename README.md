# repcheck-data-ingestion

A new RepCheck module

Part of the [RepCheck](https://github.com/Eligio-Taveras) platform -- a citizen-facing system that helps users understand how their legislators vote relative to their personal political interests.

## Dependencies

This module depends on the following shared libraries:

- **[shared-models](https://github.com/Eligio-Taveras/repcheck-shared-models)** -- Domain types (DTOs, DOs, enums, type classes)
- **[pipeline-models](https://github.com/Eligio-Taveras/repcheck-pipeline-models)** -- Pipeline operational types (events, retry, error classification, workflow)

## Tech Stack

| Concern | Technology |
|---------|-----------|
| Language | Scala 3.4.1 |
| Effect system | Cats Effect (tagless final `F[_]`) |
| HTTP | http4s Ember |
| JSON | Circe (semi-auto derivation) |
| Streaming | FS2 |
| Config | PureConfig (auto-derivation) |
| Testing | ScalaTest + MockitoScala + WireMock |
| Build | SBT 1.9.9 |
| Linting | WartRemover, Scalafix, tpolecat |
| Container | Google Distroless Java 21 |

## Prerequisites

- JDK 21 (Temurin recommended)
- SBT 1.9.9

## Build Commands

```bash
sbt compile              # Compile with WartRemover + tpolecat
sbt test                 # Run all tests
sbt scalafmtCheckAll     # Check formatting (fails if unformatted)
sbt scalafmtAll          # Auto-format all source files
sbt scalafixAll --check  # Check import ordering and lint rules
sbt scalafixAll          # Auto-fix import ordering
sbt coverage test coverageReport  # Run tests with coverage
```

## Project Structure

```
repcheck-data-ingestion/
  src/
    main/scala/          # Application code
    test/scala/          # Tests
doc-generator/           # Doc compression utility
docs/                    # Full documentation
.claude/agent-docs/      # Compressed docs for agents
```

## CI Checks

Before pushing, always run the full CI check suite:

```bash
source scripts/ci-functions.sh
CreatePR "title" "body"   # For new PRs: runs checks, pushes, creates PR
pushToPR                   # For existing PRs: runs checks, pushes
```

## Publishing

Published to [GitHub Packages](https://github.com/Eligio-Taveras/repcheck-data-ingestion/packages) via sbt-dynver (git-based semver). Add as a dependency:

```scala
libraryDependencies += "com.repcheck" %% "repcheck-data-ingestion" % "<version>"
```

## Documentation

See `CLAUDE.md` for the agent routing guide, coding conventions, and task routing table.
