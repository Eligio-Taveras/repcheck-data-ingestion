<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/streaming-pipeline.scala -->

```markdown
# RepCheck Skeleton: FS2 Streaming Pipeline with Fail-and-Continue

**Purpose:** Core streaming pattern for all pipeline apps. Items wrapped with correlationId, processed via parEvalMap, results persisted immediately.

**Key Decisions:**
- Always parEvalMap(config.parallelism) — sequential = parallelism 1
- No memory accumulation — each ProcessingResult persisted immediately
- ProcessingResult: entityId, correlationId, status, errorMessage only
- PipelineRunSummary aggregator runs after stream completes, reading from AlloyDB
- Correlation ID (UUID) on every item, visible in all logs/serialization
- Transient errors → log + continue; Systemic errors → halt pipeline

## Data Models

```scala
final case class PipelineItem[T](
    correlationId: UUID,
    runId: String,
    payload: T
)

object PipelineItem {
  def wrap[T](runId: String, payload: T): PipelineItem[T] =
    PipelineItem(
      correlationId = UUID.randomUUID(),
      runId = runId,
      payload = payload
    )
}
```

Wraps raw item with tracing metadata — correlationId unique per item per run, visible in all logs/results/events.

```scala
enum ResultStatus {
  case Succeeded, Failed
}

final case class ProcessingResult(
    correlationId: UUID,
    runId: String,
    entityId: String,
    status: ResultStatus,
    errorMessage: Option[String],
    timestamp: Instant
)
```

Lightweight result record — NO heavy payload. Written to AlloyDB `processing_results` table immediately, then dropped from memory.

```scala
final case class PipelineRunSummary(
    runId: String,
    pipelineName: String,
    totalProcessed: Int,
    succeeded: Int,
    failed: Int,
    startedAt: Instant,
    completedAt: Instant
)
```

Aggregated summary written to AlloyDB `pipeline_runs` table after stream completes. Aggregator reads ProcessingResults from AlloyDB (NOT memory).

## Core Implementation

```scala
trait PipelineRunner[F[_]] {
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
            .evalMap(persistResult)
            .compile
            .drain

          completedAt <- Clock[F].realTimeInstant
          summary <- buildSummary(runId, pipelineName, startedAt, completedAt)
        } yield summary

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
```

**Stream Flow:**
1. Wrap each item with correlationId and runId
2. parEvalMap(parallelism): execute process function concurrently
3. On success: create Succeeded result
4. On SystemicFailure: re-raise to halt pipeline
5. On TransientFailure: create Failed result, continue
6. On unknown error: treat as transient (Failed result, continue)
7. evalMap(persistResult): write each result to AlloyDB immediately
8. After stream drains: buildSummary queries AlloyDB for aggregates

**Result Persistence:** Each ProcessingResult written immediately via evalMap, never accumulated in memory.

**Summary Aggregation:** Reads persisted results from AlloyDB to avoid memory overhead.
```