# Deployment Architecture — Annotated Reference

## Pattern Summary

RepCheck uses four deployment archetypes across its repositories. Each repo maps to exactly one archetype, which determines how it is built, packaged, and deployed. All pipeline and service repos use Google Distroless Java 21 as the runtime base image for minimal attack surface.

## When to Use This Guide

- Setting up CI/CD for a new repository
- Containerizing a pipeline application
- Deploying prompt configs to GCS
- Publishing a shared library to GitHub Packages
- Understanding the promotion pipeline (dev → staging → prod)

## Source Files

```
docs/templates/skeletons/dockerfile-pipeline.txt
docs/templates/skeletons/docker-compose-local-dev.yml
docs/templates/skeletons/cloud-run-job.yaml
docs/templates/skeletons/github-actions-deploy.yml
docs/templates/skeletons/github-actions-bug-on-failure.yml
```

---

## Deployment Archetypes

| Archetype | Repos | Deploy Target | Trigger | Artifact |
|-----------|-------|---------------|---------|----------|
| **Library** | repcheck-shared-models, repcheck-pipeline-models | GitHub Packages (Maven) | Tag push (`v*`) | JAR published to Maven |
| **Config** | repcheck-prompt-engine-bills, repcheck-prompt-engine-users | GCS bucket `repcheck-prompt-configs` | Merge to main | YAML/JSON files synced to GCS |
| **Pipeline** | repcheck-data-ingestion, repcheck-llm-analysis, repcheck-scoring-engine | Cloud Run Job via Artifact Registry | Merge to main | Docker image (Distroless Java 21) |
| **Service** | repcheck-api-server (future) | Cloud Run Service via Artifact Registry | Merge to main | Docker image (Distroless Java 21) + HTTP port |

---

## Archetype 1: Library

**What gets built:** SBT compiles the project and publishes a versioned JAR.

**How it deploys:**
```
git tag v1.2.0 → push tag → GitHub Actions → sbt publish → GitHub Packages Maven
```

**Secrets needed:**
- `GITHUB_TOKEN` (automatic in GitHub Actions)

**Version strategy:** Semantic versioning via git tags. The `version` in `build.sbt` must match the tag.

**Consuming repos** declare the dependency in their `build.sbt`:
```scala
libraryDependencies += "com.repcheck" %% "shared-models" % "1.2.0"
```

---

## Archetype 2: Config (Prompt Engines)

**What gets built:** Nothing compiled. Prompt config files (YAML/JSON) are synced directly to GCS.

**How it deploys:**
```
merge to main → GitHub Actions → gcloud auth → gsutil rsync to GCS bucket
```

**GCS layout:**
```
gs://repcheck-prompt-configs/
├── bills/
│   ├── blocks/           ← Individual prompt fragments
│   │   ├── fiscal-lens.yaml
│   │   └── rights-lens.yaml
│   └── profiles/         ← Composed prompt profiles
│       └── full-analysis.yaml
└── users/
    ├── blocks/
    └── profiles/
```

**Secrets needed:**
- Workload Identity Federation (see GCP Authentication below)

**Version strategy:** Semver embedded in filenames. Version configurable per consuming app. CI deploys the default version.

---

## Archetype 3: Pipeline (Cloud Run Jobs)

**What gets built:** SBT compiles + sbt-assembly creates a fat JAR. Multi-stage Docker build packages it into a Distroless runtime image.

**How it deploys:**
```
merge to main → GitHub Actions → Docker build → push to Artifact Registry → gcloud run jobs update
```

**Base images:**
- Build stage: `eclipse-temurin:21-jdk` (full JDK for SBT compilation)
- Runtime stage: `gcr.io/distroless/java21-debian12` (no shell, no package manager, ~130MB)

**Why Distroless:**
- Minimal attack surface (no shell = no shell injection)
- Smaller image size (~130MB vs ~300MB for JRE-based)
- Google maintains security patches
- Trade-off: cannot `docker exec` into the container for debugging (use Cloud Logging instead)

**SBT Assembly requirement:** Each pipeline repo needs in `project/plugins.sbt`:
```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
```

And in `build.sbt`:
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
JDK 21 is container-aware by default — it reads cgroup memory limits.

**Secrets needed:**
- Workload Identity Federation for Artifact Registry push + Cloud Run deploy
- Runtime env vars injected by Cloud Run: `ALLOYDB_URL`, `GOOGLE_CLOUD_PROJECT`, pipeline-specific configs

---

## Archetype 4: Service (Future — Cloud Run Service)

**Differences from Pipeline archetype:**
- `EXPOSE 8080` in Dockerfile (http4s Ember listens on port 8080)
- Cloud Run Service (not Job) — long-running, auto-scaling
- Health check endpoint: `GET /health` returns 200
- `min-instances: 1` to avoid cold start latency (configurable)
- Revision-based traffic splitting for canary deploys

This archetype will be fully specified when `repcheck-api-server` development begins.

---

## GCP Authentication — Workload Identity Federation

We use Workload Identity Federation (WIF) for keyless authentication from GitHub Actions to GCP. No service account JSON keys are stored as secrets.

### How it works:
1. GitHub Actions generates an OIDC token (built-in, no setup needed)
2. GCP exchanges the OIDC token for short-lived GCP credentials
3. Credentials are scoped to a specific service account with least-privilege IAM roles

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

# 4. Grant IAM roles to the service account
gcloud projects add-iam-policy-binding "repcheck-PLACEHOLDER_ENV" \
  --member="serviceAccount:repcheck-deployer@repcheck-PLACEHOLDER_ENV.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding "repcheck-PLACEHOLDER_ENV" \
  --member="serviceAccount:repcheck-deployer@repcheck-PLACEHOLDER_ENV.iam.gserviceaccount.com" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding "repcheck-PLACEHOLDER_ENV" \
  --member="serviceAccount:repcheck-deployer@repcheck-PLACEHOLDER_ENV.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# 5. Allow GitHub repo to impersonate the service account
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

---

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
- **Deployer** (CI/CD): `repcheck-deployer@repcheck-{env}.iam.gserviceaccount.com`
- **Pipeline runtime**: `repcheck-pipeline-sa@repcheck-{env}.iam.gserviceaccount.com`

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

---

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

---

## Cross-References

- **Dockerfile skeleton**: `docs/templates/skeletons/dockerfile-pipeline.txt`
- **Docker Compose for local dev**: `docs/templates/skeletons/docker-compose-local-dev.yml`
- **Cloud Run Job definition**: `docs/templates/skeletons/cloud-run-job.yaml`
- **CI/CD deploy workflow**: `docs/templates/skeletons/github-actions-deploy.yml`
- **Auto-bug filing**: `docs/templates/skeletons/github-actions-bug-on-failure.yml`
- **Testing infrastructure**: `docs/templates/annotated/testing-infrastructure.md`
- **System design**: `docs/architecture/SYSTEM_DESIGN.md`
