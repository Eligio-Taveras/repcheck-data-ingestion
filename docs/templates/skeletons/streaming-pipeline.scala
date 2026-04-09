// =============================================================================
// RepCheck Skeleton: FS2 Streaming Pipeline with Fail-and-Continue
// Repo: Each pipeline application
// =============================================================================
//
// PURPOSE: Core streaming pattern used by all pipeline apps. Each item gets a
// correlationId, is processed via parEvalMap, and produces a ProcessingResult
// written to AlloyDB immediately then released from memory.
//
// KEY DECISIONS (from Q&A):
// - Always parEvalMap(config.parallelism) — sequential = parallelism 1
// - No accumulation in memory — each ProcessingResult persisted immediately
// - ProcessingResult is lightweight: entityId, correlationId, status, errorMessage
// - PipelineRunSummary aggregator runs after stream completes
// - Correlation ID (UUID) on every item, visible in all logs and serialization
// - Transient errors → log result + continue; Systemic errors → halt pipeline
// =============================================================================

package repcheck.pipeline.streaming

import cats.effect.{Async, Clock}
import cats.syntax.all.*

import fs2.Stream

import java.time.Instant
import java.util.UUID

import repcheck.pipeline.models.retry.{
  ErrorClassifier,
  ErrorSeverity,
  SystemicFailure,
  TransientFailure
}

// ---------------------------------------------------------------------------
// Pipeline Item — wraps every item entering the pipeline
// ---------------------------------------------------------------------------

/** Every item in the pipeline gets wrapped with tracing metadata.
  *
  * @param correlationId Unique per item per run — visible in all logs, results, events
  * @param runId         Ties to the pipeline run for grouping
  * @param payload       The actual data to process
  */
final case class PipelineItem[T](
    correlationId: UUID,
    runId: String,
    payload: T
)

object PipelineItem {
  /** Wrap a raw item with a new correlationId. */
  def wrap[T](runId: String, payload: T): PipelineItem[T] =
    PipelineItem(
      correlationId = UUID.randomUUID(),
      runId = runId,
      payload = payload
    )
}

// ---------------------------------------------------------------------------
// Processing Result — written per-item to AlloyDB immediately
// ---------------------------------------------------------------------------

enum ResultStatus {
  case Succeeded, Failed
}

/** Lightweight result record — NO heavy payload data.
  * Written to AlloyDB `processing_results` table immediately after
  * each item is processed, then the reference is dropped from memory.
  */
final case class ProcessingResult(
    correlationId: UUID,
    runId: String,
    entityId: String,
    status: ResultStatus,
    errorMessage: Option[String],
    timestamp: Instant
)

// ---------------------------------------------------------------------------
// Pipeline Run Summary — written once after stream completes
// ---------------------------------------------------------------------------

/** Aggregated summary of a pipeline run.
  * Written to AlloyDB `pipeline_runs` table after the stream finishes.
  * The aggregator reads ProcessingResults from AlloyDB (NOT from memory).
  */
final case class PipelineRunSummary(
    runId: String,
    pipelineName: String,
    totalProcessed: Int,
    succeeded: Int,
    failed: Int,
    startedAt: Instant,
    completedAt: Instant
)

// ---------------------------------------------------------------------------
// Pipeline Runner — the core streaming pattern
// ---------------------------------------------------------------------------

/** Generic pipeline runner that processes items with fail-and-continue semantics.
  *
  * @tparam F Effect type
  * @tparam T Input item type
  * @tparam R Output result type (what the process function produces on success)
  */
trait PipelineRunner[F[_]] {

  /** Run the pipeline: stream items → process → write results → summarize.
    *
    * @param runId       Unique ID for this pipeline run
    * @param pipelineName Name for logging and summary (e.g., "bill-ingestion")
    * @param parallelism Number of items to process concurrently
    * @param items       Source stream of raw items
    * @param entityIdOf  Extract an ID string from an item (for result tracking)
    * @param process     The business logic applied to each item
    * @param persistResult Write a ProcessingResult to AlloyDB
    * @return PipelineRunSummary
    */
  def run[T, R](
      runId: String,
      pipelineName: String,
      parallelism: Int,
      items: Stream[F, T],
      entityIdOf: T => String,
      process: PipelineItem[T] => F[R],
      persistResult: ProcessingResult => F[Unit]
  ): F[PipelineRunSummary]
}

object PipelineRunner {

  def make[F[_]: Async]: PipelineRunner[F] =
    new PipelineRunner[F] {
      def run[T, R](
          runId: String,
          pipelineName: String,
          parallelism: Int,
          items: Stream[F, T],
          entityIdOf: T => String,
          process: PipelineItem[T] => F[R],
          persistResult: ProcessingResult => F[Unit]
      ): F[PipelineRunSummary] =
        for {
          startedAt <- Clock[F].realTimeInstant

          // Process all items with fail-and-continue
          // parEvalMap handles parallelism; sequential = parallelism 1
          _ <- items
            .map(item => PipelineItem.wrap(runId, item))
            .parEvalMap(parallelism) { pipelineItem =>
              val entityId = entityIdOf(pipelineItem.payload)

              // TODO: Log INFO: s"Processing $entityId [${pipelineItem.correlationId}]"

              process(pipelineItem)
                .as(
                  ProcessingResult(
                    correlationId = pipelineItem.correlationId,
                    runId = runId,
                    entityId = entityId,
                    status = ResultStatus.Succeeded,
                    errorMessage = None,
                    timestamp = Instant.now()
                  )
                )
                .handleErrorWith {
                  case e: SystemicFailure =>
                    // Systemic error — DO NOT catch, let it propagate to halt the pipeline
                    Async[F].raiseError(e)

                  case e: TransientFailure =>
                    // Transient error — record failure, continue to next item
                    // TODO: Log WARN: s"Item $entityId failed [${pipelineItem.correlationId}]: ${e.getMessage}"
                    Async[F].pure(
                      ProcessingResult(
                        correlationId = pipelineItem.correlationId,
                        runId = runId,
                        entityId = entityId,
                        status = ResultStatus.Failed,
                        errorMessage = Some(e.getMessage),
                        timestamp = Instant.now()
                      )
                    )

                  case e =>
                    // Unknown error — treat as transient, log for investigation
                    // TODO: Log ERROR: s"Unexpected error on $entityId [${pipelineItem.correlationId}]: ${e.getMessage}"
                    Async[F].pure(
                      ProcessingResult(
                        correlationId = pipelineItem.correlationId,
                        runId = runId,
                        entityId = entityId,
                        status = ResultStatus.Failed,
                        errorMessage = Some(s"Unexpected: ${e.getMessage}"),
                        timestamp = Instant.now()
                      )
                    )
                }
            }
            // Write each result to AlloyDB immediately, then release from memory
            .evalMap(persistResult)
            .compile
            .drain

          // Aggregator: read results from AlloyDB (NOT memory) to build summary
          completedAt <- Clock[F].realTimeInstant
          summary <- buildSummary(runId, pipelineName, startedAt, completedAt)
        } yield summary

      /** Read persisted ProcessingResults from AlloyDB and aggregate.
        * This avoids keeping results in memory during the stream.
        */
      private def buildSummary(
          runId: String,
          pipelineName: String,
          startedAt: Instant,
          completedAt: Instant
      ): F[PipelineRunSummary] =
        // TODO: Query AlloyDB `processing_results` table
        //       WHERE run_id = $runId
        //       COUNT succeeded vs failed
        //       Return PipelineRunSummary
        Async[F].pure(
          PipelineRunSummary(
            runId = runId,
            pipelineName = pipelineName,
            totalProcessed = 0, // TODO: query count
            succeeded = 0,      // TODO: query count where status == Succeeded
            failed = 0,         // TODO: query count where status == Failed
            startedAt = startedAt,
            completedAt = completedAt
          )
        )
    }
}
