<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/annotated/deployment-architecture.md -->

# Deployment Architecture — Annotated Reference

## When to Use This Guide
- Setting up CI/CD for a new repository
- Containerizing a pipeline application
- Deploying prompt configs to GCS
- Publishing a shared library to GitHub Packages
- Understanding the promotion pipeline (dev → staging → prod)

## Deployment Archetypes

| Archetype | Repos | Deploy Target | Trigger | Artifact |
|-----------|-------|---------------|---------|----------|
| **Library** | repcheck-shared-models, repcheck-pipeline-models | GitHub Packages (Maven) | Tag push (`v*`) | JAR published to Maven |
| **Config** | repcheck-prompt-engine-bills, repcheck-prompt-engine-users | GCS bucket `repcheck-prompt-configs` | Merge to main | YAML/JSON files synced to GCS |
| **Pipeline** | repcheck-data-ingestion, repcheck-llm-analysis, repcheck-scoring-engine | Cloud Run Job via Artifact Registry | Merge to main | Docker image (Distroless Java 21) |
| **Service** | repcheck-api-server (future) | Cloud Run Service via Artifact Registry | Merge to main | Docker image (Distroless Java 21) + HTTP port |

## Archetype 1: Library

**What gets built:** SBT compiles project and publishes versioned JAR.

**How it deploys:** `git tag v1.2.0 → push tag → GitHub Actions → sbt publish → GitHub Packages Maven`

**Secrets needed:** `GITHUB_TOKEN` (automatic)

**Version strategy:** Semantic versioning via git tags. The `version` in `build.sbt` must match the tag.

**Consuming repos** declare dependency in `build.sbt`:
```scala
libraryDependencies += "com.repcheck" %% "shared-models" % "1.2.0"
```

## Archetype 2: Config (Prompt Engines)

**What gets built:** Prompt config files (YAML/JSON) synced directly to GCS. No compilation.

**How it deploys:** `merge to main → GitHub Actions → gcloud auth → gsutil rsync to GCS bucket`

**GCS layout:**
```
gs://repcheck-prompt-configs/
├── bills/
│   ├── blocks/           # Individual prompt fragments
│   │   ├── fiscal-lens.yaml
│   │   └── rights-lens.yaml
│   └── profiles/         # Composed prompt profiles
│       └── full-analysis.yaml
└── users/
    ├── blocks/
    └── profiles/
```

**Secrets needed:** Workload Identity Federation

**Version strategy:** Semver embedded in filenames. Version configurable per consuming app. CI deploys default version.

## Archetype 3: Pipeline (Cloud Run Jobs)

**What gets built:** SBT compiles + sbt-assembly creates fat JAR. Multi-stage Docker build packages into Distroless runtime image.

**How it deploys:** `merge to main → GitHub Actions → Docker build → push to Artifact Registry → gcloud run jobs update`

**Base images:**
- Build stage: `eclipse-temurin:21-jdk` (full JDK for SBT compilation)
- Runtime stage: `gcr.io/distroless/java21-debian12` (no shell, no package manager, ~130MB)

**Why Distroless:** Minimal attack surface (no shell injection), smaller image (~130MB vs ~300MB), Google maintains patches. Trade-off: cannot `docker exec` into container (use Cloud Logging instead).

**SBT Assembly requirement:** Add to `project/plugins.sbt`:
```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
```

Add to `build.sbt`:
```scala
lazy val app = project
  .settings(
    assembly / mainClass := Some("com.repcheck.PLACEHOLDER_MAIN"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
```

**JVM tuning for Cloud Run:**
```
-XX:MaxRAMPercentage=75.0    # Use 75% of container memory limit
```
JDK 21 reads cgroup memory limits automatically.

**Secrets needed:** Workload Identity Federation for Artifact Registry push + Cloud Run deploy. Runtime env vars injected by Cloud Run: `ALLOYDB_URL`, `GOOGLE_CLOUD_PROJECT`, pipeline-specific configs.

## Archetype 4: Service (Future — Cloud Run Service)

**Differences from Pipeline archetype:**
- `EXPOSE 8080` in Dockerfile (http4s Ember listens on 8080)
- Cloud Run Service (not Job) — long-running, auto-scaling
- Health check endpoint: `GET /health` returns 200
- `min-instances: 1` to avoid cold start latency
- Revision-based traffic splitting for canary deploys

Full spec when `repcheck-api-server` development begins.

## GCP Authentication — Workload Identity Federation

Keyless authentication from GitHub Actions to GCP via OIDC token exchange. No service account JSON keys stored as secrets.

### How it works:
1. GitHub Actions generates OIDC token (built-in)
2. GCP exchanges OIDC token for short-lived GCP credentials
3. Credentials scoped to specific service account with least-privilege IAM roles

### One-time GCP setup:

```bash
# 1. Create Workload Identity Pool
gcloud iam workload-identity-pools create "github-pool" \
  --project="repcheck-PLACEHOLDER_ENV" \
  --location="global" \
  --display-name="GitHub Actions Pool"

# 2. Create OIDC Provider
gcloud iam workload-identity-pools providers create-oidc "github-provider" \
  --project="repcheck-PLACEHOLDER_ENV" \
  --location="global" \
  --workload-identity-pool="github-pool" \
  --display-name="GitHub Provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --issuer-uri="https://token.actions.githubusercontent.com"

# 3. Create Service Account
gcloud iam service-accounts create "repcheck-deployer" \
  --project="repcheck-PLACEHOLDER_ENV" \
  --display-name="RepCheck CI/CD Deployer"

# 4. Grant IAM roles
gcloud projects add-iam-policy-binding "repcheck-PLACEHOLDER_ENV" \
  --member="serviceAccount:repcheck-deployer@repcheck-PLACEHOLDER_ENV.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding "repcheck-PLACEHOLDER_ENV" \
  --member="serviceAccount:repcheck-deployer@repcheck-PLACEHOLDER_ENV.iam.gserviceaccount.com" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding "repcheck-PLACEHOLDER_ENV" \
  --member="serviceAccount:repcheck-deployer@repcheck-PLACEHOLDER_ENV.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# 5. Allow GitHub repo to impersonate service account
gcloud iam service-accounts add-iam-policy-binding \
  "repcheck-deployer@repcheck-PLACEHOLDER_ENV.iam.gserviceaccount.com" \
  --project="repcheck-PLACEHOLDER_ENV" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/PLACEHOLDER_PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/attribute.repository/Eligio-Taveras/PLACEHOLDER_REPO"
```

### GitHub Actions usage:
```yaml
- id: auth
  uses: google-github-actions/auth@v2
  with:
    workload_identity_provider: "projects/PLACEHOLDER_PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/providers/github-provider"
    service_account: "repcheck-deployer@repcheck-PLACEHOLDER_ENV.iam.gserviceaccount.com"
```

## GCP Resource Naming

### Projects (3 environments):

| Environment | GCP Project ID | Purpose |
|-------------|---------------|---------|
| Dev | `repcheck-dev` | Auto-deploy on merge to main. E2E tests run here. |
| Staging | `repcheck-staging` | Auto-promote from dev when e2e tests pass. Pre-prod validation. |
| Prod | `repcheck-prod` | Manual approval gate. Production traffic. |

### Artifact Registry:
```
us-central1-docker.pkg.dev/repcheck-{env}/repcheck-images/{image-name}:{tag}
```

### Service Accounts:
- **Deployer (CI/CD):** `repcheck-deployer@repcheck-{env}.iam.gserviceaccount.com`
- **Pipeline runtime:** `repcheck-pipeline-sa@repcheck-{env}.iam.gserviceaccount.com`

### Cloud Run Jobs:
```
repcheck-{pipeline-name}    # e.g., repcheck-bills-pipeline, repcheck-scoring-pipeline
```

### Pub/Sub Topics:
```
bill-events, vote-events, analysis-events, user-events
```

### AlloyDB Tables:
Use constants from `pipeline-models` `Tables` object (never hardcode strings).

### GCS Buckets:
```
repcheck-snapshots-{env}
repcheck-prompt-configs-{env}
```

## Promotion Pipeline

```
merge to main
    │
    ▼
┌─────────────────┐
│  Deploy to Dev   │  (auto, on every merge)
│  repcheck-dev    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Run E2E Tests   │  (auto, against dev environment)
│  Tagged E2ETest  │
└────────┬────────┘
         │ pass
         ▼
┌─────────────────────┐
│  Deploy to Staging   │  (auto, if e2e tests pass)
│  repcheck-staging    │
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  Manual Approval     │  (GitHub Environment protection rule)
│  Gate                │
└────────┬────────────┘
         │ approved
         ▼
┌─────────────────────┐
│  Deploy to Prod      │  (on approval)
│  repcheck-prod       │
└─────────────────────┘
```

**Implementation:** GitHub Environments (`dev`, `staging`, `production`) with protection rules. The `production` environment requires manual approval from designated reviewers.

## Source Files

```
docs/templates/skeletons/dockerfile-pipeline.txt
docs/templates/skeletons/docker-compose-local-dev.yml
docs/templates/skeletons/cloud-run-job.yaml
docs/templates/skeletons/github-actions-deploy.yml
docs/templates/skeletons/github-actions-bug-on-failure.yml
```