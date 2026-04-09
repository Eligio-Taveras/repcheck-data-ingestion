<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/pipeline-app.scala -->

```markdown
# RepCheck Pipeline Application Entry Point

**Purpose**: Cloud Run Job entry point pattern. Extends IOApp, loads snapshots from GCS, processes items via FS2 streaming with fail-and-continue, writes PipelineRunSummary, updates run status in DB.

**Key Decisions**:
- Cloud Run Jobs (not services) — run, process, exit
- Load snapshot at startup (read GCS, not live DBs)
- Only DB communication during run: pipeline run status tracking
- FS2 streaming with parEvalMap(config.parallelism)
- Fail-and-continue with ProcessingResult per item
- PipelineRunSummary aggregated from AlloyDB after stream completes
- Pub/Sub event publishing for downstream triggers

## Example: Bill Ingestion Pipeline App

```scala
// TODO: Uncomment and implement when ready

object BillIngestionApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val runId = UUID.randomUUID().toString

    for {
      // Step 1: Load configuration
      config <- ConfigLoader.load[BillIngestionAppConfig]("bill-ingestion")

      // Step 2: Initialize resources
      result <- (
        GcsClient.make[IO](config.gcs, GcsErrorClassifier),
        AlloyDbTransactor.make[IO](config.db.url, config.db.user, config.db.password),
        PubSubPublisher.make[IO](config.pubSub, PubSubErrorClassifier)
      ).tupled.use { case (gcsClient, xa, publisher) =>

        for {
          // Step 3: Read snapshot manifest (created by snapshot service)
          manifest <- gcsClient.readJson[SnapshotManifest](
            "repcheck-snapshots",
            s"snapshots/$runId/manifest-${config.versions.snapshotVersion}.json"
          )

          // Step 4: Load source data from snapshot (NOT live DB)
          // This is the list of items to process
          bills <- gcsClient.readJson[List[BillDTO]](
            "repcheck-snapshots",
            s"${manifest.basePath}/bills-${config.versions.snapshotVersion}.json"
          )

          // Step 5: Create the processing stream
          billStream = Stream.emits(bills)

          // Step 6: Define the per-item processing function
          processBill = (item: PipelineItem[BillDTO]) => {
            for {
              // Fetch bill text from Congress.gov
              text <- fetchBillText(item.payload, config.congressGov)

              // Convert DTO → DO
              billDO <- IO.fromEither(
                item.payload.toDO.leftMap(e =>
                  DecodeFailed(item.payload.billId, "", new RuntimeException(e))
                )
              )

              // Save to AlloyDB
              _ <- billDO.saveBill[IO](xa, logger)

              // Publish event for downstream (LLM analysis)
              _ <- publisher.publish(
                PipelineEvent.create(
                  eventType = "bill.text.available",
                  source = "bill-ingestion",
                  payload = BillTextAvailable(
                    billId = billDO.billId,
                    congress = billDO.congress,
                    textUrl = text.url
                  ),
                  resources = ResourceRequirements(maxCpu = "2", maxMemory = "1Gi")
                )
              )
            } yield ()
          }

          // Step 7: Run the pipeline with fail-and-continue
          runner = PipelineRunner.make[IO]
          summary <- runner.run(
            runId = runId,
            pipelineName = "bill-ingestion",
            parallelism = config.congressGov.parallelism,
            items = billStream,
            entityIdOf = (bill: BillDTO) => bill.billId,
            process = processBill,
            persistResult = (result) =>
              upsertProcessingResult(result).transact(xa)
              // upsertProcessingResult writes to Tables.ProcessingResults
          )

          // Step 8: Write pipeline run summary
          _ <- upsertPipelineRunSummary(summary).transact(xa)
          // upsertPipelineRunSummary writes to Tables.PipelineRuns

          // Step 9: Log summary
          _ <- IO.println(
            s"Pipeline complete: ${summary.succeeded}/${summary.totalProcessed} succeeded, " +
            s"${summary.failed} failed, duration: ${summary.completedAt.toEpochMilli - summary.startedAt.toEpochMilli}ms"
          )
        } yield summary
      }
    } yield
      if (result.failed > 0) { ExitCode(1) }  // Non-zero exit for partial failure
      else { ExitCode.Success }
  }
}
```

## Template: Generic Pipeline App Structure

Base pattern for all pipeline apps. Copy and implement:
1. AppConfig type
2. Input DTO type
3. Per-item processing function
4. Downstream PipelineEvent payload

```scala
trait PipelineApp {
  // Override these in your app:
  // type AppConfig
  // type InputItem
  // def configNamespace: String
  // def pipelineName: String
  // def loadItems(gcs: GcsClient[IO], manifest: SnapshotManifest, config: AppConfig): IO[List[InputItem]]
  // def entityIdOf(item: InputItem): String
  // def processItem(item: PipelineItem[InputItem])(using /* dependencies */): IO[Unit]
  ???
}
```
```