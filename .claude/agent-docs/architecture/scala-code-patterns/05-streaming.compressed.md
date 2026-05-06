<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/scala-code-patterns/05-streaming.md -->

# Scala Code Patterns — Compressed

## 5. Fail-and-Continue Streaming

**Pattern**: Process items as a stream. Each item processed independently — failures persisted immediately as `ProcessingResult` records, then released from memory. Aggregator summarizes results after stream completes.

### Core Types (in `repcheck-pipeline-models`)

```scala
import java.time.Instant

enum ResultStatus {
  case Succeeded, Failed
}

// Written to AlloyDB immediately per-item, then released from memory
case class ProcessingResult(
  entityId: String,
  status: ResultStatus,
  errorMessage: Option[String],
  timestamp: Instant
)

// Written once after the entire stream completes
case class PipelineRunSummary(
  runId: String,
  pipelineName: String,
  totalProcessed: Int,
  succeeded: Int,
  failed: Int,
  startedAt: Instant,
  completedAt: Instant
)
```

### Stream Pattern

```scala
import fs2.Stream

def processStream[F[_]: Async](
  items: Stream[F, LegislativeBillApiDTO],
  persist: LegislativeBillDO => F[Unit],
  writeResult: ProcessingResult => F[Unit]
): Stream[F, Unit] =
  items.evalMap { dto =>
    val entityId = s"${dto.`type`}-${dto.number}-${dto.congress}"
    processSingleItem(dto, persist)
      .as(ProcessingResult(entityId, ResultStatus.Succeeded, None, Instant.now()))
      .recover { case e: Throwable =>
        ProcessingResult(entityId, ResultStatus.Failed, Some(e.getMessage), Instant.now())
      }
      .flatMap(writeResult)
      // After writeResult completes, dto and result are eligible for GC
  }

private def processSingleItem[F[_]: Async](
  dto: LegislativeBillApiDTO,
  persist: LegislativeBillDO => F[Unit]
): F[Unit] = {
  val bill = dto.toDO()
  persist(bill)
}
```

### Aggregator

```scala
def summarizeRun[F[_]: Sync](
  runId: String,
  pipelineName: String,
  startedAt: Instant,
  readResults: String => F[List[ProcessingResult]],
  writeSummary: PipelineRunSummary => F[Unit]
): F[PipelineRunSummary] =
  for {
    results <- readResults(runId)
    summary = PipelineRunSummary(
      runId = runId,
      pipelineName = pipelineName,
      totalProcessed = results.size,
      succeeded = results.count(_.status == ResultStatus.Succeeded),
      failed = results.count(_.status == ResultStatus.Failed),
      startedAt = startedAt,
      completedAt = Instant.now()
    )
    _ <- writeSummary(summary)
  } yield summary
```

### Rules
- Never accumulate items or results in memory across stream elements
- Each `ProcessingResult` written to AlloyDB (`processing_results` table, keyed by `run_id`/`entity_id`) immediately
- After writing, item and result released — no references retained
- Aggregator reads persisted results after stream completion to build summary
- `PipelineRunSummary` written to AlloyDB `pipeline_runs` table
- `ProcessingResult` stores only lightweight metadata — never the full payload

---

## 15. FS2 Streaming

**Pattern**: FS2 streams for memory-efficient processing. Individual items processed and released — no accumulation.

### Stream Construction

```scala
import fs2.Stream

// From a paginated API call
def streamBills[F[_]: Async: Network](
  api: LegislativeBillsApi[F],
  fromDate: Option[ZonedDateTime]
): Stream[F, LegislativeBillApiDTO] =
  api.streamBatch(fromDateTime = fromDate)
    .flatMap(batch => Stream.emits(batch.bills))

// Recursive streaming for all pages
def streamAllPages[F[_]: Async: Network](
  api: LegislativeBillsApi[F],
  fromDate: Option[ZonedDateTime],
  offset: Int = 0,
  pageSize: Int = 250
): Stream[F, LegislativeBillApiDTO] =
  Stream.eval(api.getObjects(offset, fromDate)).flatMap { batch =>
    val items = Stream.emits(batch.bills)
    if (batch.lengthRetrieved >= pageSize)
      items ++ streamAllPages(api, fromDate, offset + pageSize, pageSize)
    else
      items
  }
```

### Processing Pattern

```scala
// Process each item independently — no accumulation
streamAllPages(api, fromDate)
  .evalMap { dto =>
    processAndPersist(dto)  // returns F[ProcessingResult]
  }
  .evalMap { result =>
    writeProcessingResult(result)  // persist to AlloyDB, then release
  }
  .compile
  .drain  // consume the stream, discard everything
```

### Rules
- Use `evalMap` for per-item effectful processing
- Use `Stream.emits` to flatten batches into individual items
- Use `.compile.drain` when you don't need to collect results (persisted per-item)
- Recursive streams for pagination — stop when `lengthRetrieved < pageSize`
- Never use `.compile.toList` on unbounded streams — always drain or fold with bounded accumulation