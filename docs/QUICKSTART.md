# RepCheck — Developer Quick Start

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| JDK | 21+ | [Temurin](https://adoptium.net/) |
| SBT | 1.9+ | [sbt](https://www.scala-sbt.org/download.html) |
| Git | 2.x | [git-scm.com](https://git-scm.com/) |
| Anthropic API Key | — | [console.anthropic.com](https://console.anthropic.com/) (only needed for doc compression) |

## Clone and Build

```bash
git clone git@github.com:Eligio-Taveras/votr.git
cd votr
sbt compile
```

This compiles all sub-projects with WartRemover and tpolecat checks enabled.

## Run Tests

```bash
sbt test
```

## Check Linting

```bash
sbt "scalafixAll --check"   # Check import ordering
sbt scalafixAll              # Auto-fix import ordering
```

## Sub-Projects

| Project | Description | Run |
|---------|-------------|-----|
| `billIdentifier` | Bill ingestion pipeline | `sbt "billIdentifier/run '{...}'"` |
| `govApis` | Congress.gov API client library | (library — no main) |
| `docGenerator` | Agent doc compression utility | See below |

## Environment Variables

### Required for bill ingestion
- `CONGRESS_API_KEY` — Congress.gov API key ([api.congress.gov](https://api.congress.gov/))

### Required for doc compression
- `ANTHROPIC_API_KEY` — Anthropic API key ([console.anthropic.com](https://console.anthropic.com/))

**PowerShell** — add to your `$PROFILE`:
```powershell
$env:ANTHROPIC_API_KEY = "sk-ant-..."
$env:CONGRESS_API_KEY = "your-congress-key"
```

**Bash/Zsh** — add to `~/.bashrc` or `~/.zshrc`:
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
export CONGRESS_API_KEY="your-congress-key"
```

## Generating Agent Docs

The doc compressor uses Claude Haiku to create token-efficient versions of documentation for AI coding agents. Run it manually after updating any files in `docs/` or `CLAUDE.md`.

**PowerShell:**
```powershell
.\scripts\generate-agent-docs.ps1
```

**Bash:**
```bash
./scripts/generate-agent-docs.sh
```

Both accept an optional compression threshold (default 0.20 = 20% average reduction):
```bash
./scripts/generate-agent-docs.sh 0.15
```

Output goes to `.claude/agent-docs/`. Commit the results after reviewing.

## Project Layout

```
votr/
├── bill-identifier/       # Bill ingestion Cloud Run Job
├── gov-apis/              # Congress.gov API clients + DTOs + DOs
├── doc-generator/         # Agent doc compression utility
├── blog/                  # Astro blog (deploys to Vercel from main)
├── docs/
│   ├── architecture/      # SYSTEM_DESIGN.md, SCALA_CODE_PATTERNS.md
│   └── templates/         # Annotated references + skeleton templates
├── .claude/
│   └── agent-docs/        # Compressed docs for AI agents (generated)
├── scripts/               # Developer utility scripts
├── CLAUDE.md              # Agent routing guide (auto-loaded by Claude Code)
└── build.sbt              # Multi-project SBT build
```

## Key Documentation

- **[SYSTEM_DESIGN.md](architecture/SYSTEM_DESIGN.md)** — Full architecture with Mermaid diagrams
- **[SCALA_CODE_PATTERNS.md](architecture/SCALA_CODE_PATTERNS.md)** — All code conventions
- **[PATTERNS_GUIDE.md](templates/PATTERNS_GUIDE.md)** — Template index and decision reference
- **[CLAUDE.md](../CLAUDE.md)** — Agent task routing (loaded automatically by Claude Code)

## CI/CD

GitHub Actions runs on every push to `main` and `feature/**` branches:
1. Compile with WartRemover + tpolecat
2. Check Scalafix rules
3. Run tests
4. Compile doc generator (separate job)

The blog deploys to Vercel automatically on merge to `main`.
