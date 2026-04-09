// =============================================================================
// RepCheck Skeleton: Workflow Definition (State Machine)
// Repo: Published from each pipeline repo via GitHub Actions CI
// =============================================================================
//
// PURPOSE: Defines pipeline state machines as JSON, published from GitHub
// to GCS on release. The orchestrator reads these to know what steps to
// execute and in what order.
//
// KEY DECISIONS (from Q&A):
// - Workflow definitions are version-controlled in the repo
// - Published to GCS via GitHub Action on merge/release
// - Semver independent from repo release version
// - Developers own the decision of when to bump workflow version
// - All GCS objects versioned with semver in filenames
// - Orchestrator reads workflow from GCS to determine execution order
// =============================================================================

package repcheck.pipeline.workflow

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*

import repcheck.pipeline.models.pubsub.ResourceRequirements

// ---------------------------------------------------------------------------
// Workflow Step — a single stage in the pipeline state machine
// ---------------------------------------------------------------------------

/** A step in the workflow that maps to a Cloud Run Job execution.
  *
  * @param stepName     Human-readable name (e.g., "create-snapshots", "ingest-bills")
  * @param targetApp    Cloud Run Job name to execute
  * @param dependencies Steps that must complete before this one runs
  * @param resources    CPU/memory requirements for this step
  * @param eventType    The Pub/Sub event type this step produces on completion
  * @param config       Step-specific configuration overrides
  */
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

// ---------------------------------------------------------------------------
// Workflow Definition — the complete state machine
// ---------------------------------------------------------------------------

/** A complete pipeline workflow defining the execution order.
  *
  * @param name    Workflow name (e.g., "bill-analysis-pipeline")
  * @param version Semver, independent from repo version
  * @param steps   Ordered list of steps (topologically sorted by dependencies)
  */
final case class WorkflowDefinition(
    name: String,
    version: String,
    description: String,
    steps: List[WorkflowStep]
) {
  /** Get steps that have no unmet dependencies (ready to run). */
  def readySteps(completedSteps: Set[String]): List[WorkflowStep] =
    steps.filter { step =>
      step.dependencies.forall(completedSteps.contains)
    }

  /** Check if the workflow is complete. */
  def isComplete(completedSteps: Set[String]): Boolean =
    steps.forall(s => completedSteps.contains(s.stepName))
}

object WorkflowDefinition {
  given Encoder[WorkflowDefinition] = deriveEncoder
  given Decoder[WorkflowDefinition] = deriveDecoder
}

// ---------------------------------------------------------------------------
// Example Workflow: Full Bill Analysis Pipeline
// ---------------------------------------------------------------------------
//
// GCS path: workflows/bill-analysis-pipeline-v1.0.0.json
//
// {
//   "name": "bill-analysis-pipeline",
//   "version": "v1.0.0",
//   "description": "Full pipeline: snapshot → ingest → analyze → score",
//   "steps": [
//     {
//       "stepName": "create-snapshots",
//       "targetApp": "repcheck-snapshot-service",
//       "dependencies": [],
//       "resources": { "maxCpu": "1", "maxMemory": "512Mi" },
//       "eventType": "snapshot.created"
//     },
//     {
//       "stepName": "ingest-bills",
//       "targetApp": "repcheck-bill-ingestion",
//       "dependencies": ["create-snapshots"],
//       "resources": { "maxCpu": "2", "maxMemory": "1Gi" },
//       "eventType": "bill.text.available"
//     },
//     {
//       "stepName": "ingest-votes",
//       "targetApp": "repcheck-vote-ingestion",
//       "dependencies": ["create-snapshots"],
//       "resources": { "maxCpu": "1", "maxMemory": "512Mi" },
//       "eventType": "vote.recorded"
//     },
//     {
//       "stepName": "ingest-members",
//       "targetApp": "repcheck-member-ingestion",
//       "dependencies": ["create-snapshots"],
//       "resources": { "maxCpu": "1", "maxMemory": "512Mi" }
//     },
//     {
//       "stepName": "analyze-bills",
//       "targetApp": "repcheck-llm-analysis",
//       "dependencies": ["ingest-bills"],
//       "resources": { "maxCpu": "4", "maxMemory": "4Gi" },
//       "eventType": "analysis.completed"
//     },
//     {
//       "stepName": "score-alignment",
//       "targetApp": "repcheck-scoring-engine",
//       "dependencies": ["analyze-bills", "ingest-votes"],
//       "resources": { "maxCpu": "4", "maxMemory": "4Gi" }
//     }
//   ]
// }

// ---------------------------------------------------------------------------
// Workflow Loader — reads workflow definitions from GCS
// ---------------------------------------------------------------------------

import cats.effect.Sync
import repcheck.pipeline.models.gcs.GcsClient

trait WorkflowLoader[F[_]] {
  /** Load a workflow definition by name and version from GCS. */
  def load(name: String, version: String): F[WorkflowDefinition]

  /** Load the latest version of a workflow by name. */
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
          // Sort by semver (simplified: lexicographic on version string)
          // TODO: Use proper semver parsing for correct ordering
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

// ---------------------------------------------------------------------------
// GitHub Actions CI Publishing (reference)
// ---------------------------------------------------------------------------
//
// .github/workflows/publish-workflow.yml:
//
// name: Publish Workflow Definition
// on:
//   push:
//     branches: [main]
//     paths: ['workflows/**']
//
// jobs:
//   publish:
//     runs-on: ubuntu-latest
//     steps:
//       - uses: actions/checkout@v4
//       - uses: google-github-actions/auth@v2
//         with:
//           credentials_json: ${{ secrets.GCP_SA_KEY }}
//       - uses: google-github-actions/setup-gcloud@v2
//       - name: Upload workflow definitions to GCS
//         run: |
//           gsutil cp workflows/*.json gs://repcheck-workflows/
