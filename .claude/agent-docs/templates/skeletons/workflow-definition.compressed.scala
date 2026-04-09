<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/workflow-definition.scala -->

# RepCheck Skeleton: Workflow Definition (State Machine)

**Repo:** Published from each pipeline repo via GitHub Actions CI  
**Purpose:** Defines pipeline state machines as JSON, published to GCS on release. Orchestrator reads these to determine execution order.

## Key Decisions
- Workflow definitions version-controlled in repo
- Published to GCS via GitHub Action on merge/release
- Semver independent from repo release version
- Developers own version bump decision
- All GCS objects versioned with semver in filenames
- Orchestrator reads workflow from GCS for execution order

## WorkflowStep

Maps to a Cloud Run Job execution.

```scala
final case class WorkflowStep(
    stepName: String,
    targetApp: String,
    dependencies: List[String] = List.empty,
    resources: ResourceRequirements = ResourceRequirements(),
    eventType: Option[String] = None,
    config: Map[String, String] = Map.empty
)

object WorkflowStep {
  given Encoder[WorkflowStep] = deriveEncoder
  given Decoder[WorkflowStep] = deriveDecoder
}
```

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| stepName | String | - | Human-readable step name (e.g., "create-snapshots") |
| targetApp | String | - | Cloud Run Job name to execute |
| dependencies | List[String] | [] | Steps that must complete before this one |
| resources | ResourceRequirements | {} | CPU/memory requirements |
| eventType | Option[String] | None | Pub/Sub event type produced on completion |
| config | Map[String, String] | {} | Step-specific configuration overrides |

## WorkflowDefinition

Complete pipeline workflow defining execution order.

```scala
final case class WorkflowDefinition(
    name: String,
    version: String,
    description: String,
    steps: List[WorkflowStep]
) {
  def readySteps(completedSteps: Set[String]): List[WorkflowStep] =
    steps.filter { step =>
      step.dependencies.forall(completedSteps.contains)
    }

  def isComplete(completedSteps: Set[String]): Boolean =
    steps.forall(s => completedSteps.contains(s.stepName))
}

object WorkflowDefinition {
  given Encoder[WorkflowDefinition] = deriveEncoder
  given Decoder[WorkflowDefinition] = deriveDecoder
}
```

| Method | Purpose |
|--------|---------|
| readySteps(completedSteps) | Get steps with no unmet dependencies (ready to run) |
| isComplete(completedSteps) | Check if all steps complete |

## Example Workflow: Bill Analysis Pipeline

GCS path: `workflows/bill-analysis-pipeline-v1.0.0.json`

```json
{
  "name": "bill-analysis-pipeline",
  "version": "v1.0.0",
  "description": "Full pipeline: snapshot → ingest → analyze → score",
  "steps": [
    {
      "stepName": "create-snapshots",
      "targetApp": "repcheck-snapshot-service",
      "dependencies": [],
      "resources": { "maxCpu": "1", "maxMemory": "512Mi" },
      "eventType": "snapshot.created"
    },
    {
      "stepName": "ingest-bills",
      "targetApp": "repcheck-bill-ingestion",
      "dependencies": ["create-snapshots"],
      "resources": { "maxCpu": "2", "maxMemory": "1Gi" },
      "eventType": "bill.text.available"
    },
    {
      "stepName": "ingest-votes",
      "targetApp": "repcheck-vote-ingestion",
      "dependencies": ["create-snapshots"],
      "resources": { "maxCpu": "1", "maxMemory": "512Mi" },
      "eventType": "vote.recorded"
    },
    {
      "stepName": "ingest-members",
      "targetApp": "repcheck-member-ingestion",
      "dependencies": ["create-snapshots"],
      "resources": { "maxCpu": "1", "maxMemory": "512Mi" }
    },
    {
      "stepName": "analyze-bills",
      "targetApp": "repcheck-llm-analysis",
      "dependencies": ["ingest-bills"],
      "resources": { "maxCpu": "4", "maxMemory": "4Gi" },
      "eventType": "analysis.completed"
    },
    {
      "stepName": "score-alignment",
      "targetApp": "repcheck-scoring-engine",
      "dependencies": ["analyze-bills", "ingest-votes"],
      "resources": { "maxCpu": "4", "maxMemory": "4Gi" }
    }
  ]
}
```

## WorkflowLoader

Reads workflow definitions from GCS.

```scala
trait WorkflowLoader[F[_]] {
  def load(name: String, version: String): F[WorkflowDefinition]
  def loadLatest(name: String): F[WorkflowDefinition]
}

object WorkflowLoader {
  final case class WorkflowLoaderConfig(
      bucket: String = "repcheck-workflows"
  )

  def make[F[_]: Sync](
      config: WorkflowLoaderConfig,
      gcsClient: GcsClient[F]
  ): WorkflowLoader[F] =
    new WorkflowLoader[F] {
      def load(name: String, version: String): F[WorkflowDefinition] =
        gcsClient.readJson[WorkflowDefinition](
          config.bucket,
          s"$name-$version.json"
        )

      def loadLatest(name: String): F[WorkflowDefinition] = {
        import cats.syntax.all.*
        for {
          paths <- gcsClient.listAll(config.bucket, s"$name-")
          // TODO: Use proper semver parsing for correct ordering (currently lexicographic)
          latestPath <- paths
            .sorted
            .lastOption
            .fold(
              Sync[F].raiseError[String](
                new RuntimeException(s"No workflow found for $name")
              )
            )(Sync[F].pure)
          workflow <- gcsClient.readJson[WorkflowDefinition](
            config.bucket,
            latestPath
          )
        } yield workflow
      }
    }
}
```

## GitHub Actions CI Publishing

`.github/workflows/publish-workflow.yml`

```yaml
name: Publish Workflow Definition
on:
  push:
    branches: [main]
    paths: ['workflows/**']

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}
      - uses: google-github-actions/setup-gcloud@v2
      - name: Upload workflow definitions to GCS
        run: |
          gsutil cp workflows/*.json gs://repcheck-workflows/
```