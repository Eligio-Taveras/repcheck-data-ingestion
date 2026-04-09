# Pattern: IOApp Entry Point

## Pattern Summary
The main application entry point using Cats Effect `IOApp`. Orchestrates configuration loading, resource initialization, and stream processing in a for-comprehension. Delegates all real work to library modules — the app is just glue code.

## When To Use This Pattern
- Every Cloud Run Job needs an IOApp entry point
- Each pipeline application (bill ingestion, vote ingestion, analysis, scoring)

## Source Files
- `bill-identifier/src/main/scala/BillIdentifierApp.scala` — the entry point
- `bill-identifier/src/main/scala/config/ConfigLoader.scala` — config loading

---

## The Entry Point

```scala
// File: bill-identifier/src/main/scala/BillIdentifierApp.scala

import java.time.{ZoneId, ZonedDateTime}

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all._

import doobie.Transactor
import doobie.hikari.HikariTransactor

import config.{BillIdentifierConfig, ConfigLoader}
import congress.gov.apis.LegislativeBillsApi
import org.slf4j.LoggerFactory

// ANNOTATION: IOApp is the Cats Effect entry point.
// It provides:
//   - def run(args: List[String]): IO[ExitCode]
//   - A runtime that manages thread pools and shutdown hooks
//   - Signal handling (Ctrl+C gracefully cancels the IO)
//
// The app itself should be THIN — it wires dependencies together
// and delegates to library code. No business logic lives here.
object BillIdentifierApp extends IOApp {

  private val logger = LoggerFactory.getLogger(getClass)

  override def run(args: List[String]): IO[ExitCode] = {
    // ANNOTATION: Step 1 — Load config.
    // Returns IO[Config] — the config is not loaded until this IO runs.
    // ConfigLoader handles validation and error messages.
    val loadedConfig: IO[BillIdentifierConfig] = ConfigLoader.LoadConfig(args)

    // ANNOTATION: Step 2 — Build transactor as a Resource.
    // AlloyDbTransactor.make returns Resource[IO, Transactor[IO]] (HikariCP pool).
    // .use keeps the pool open for the duration of the block.
    val x: IO[Unit] = AlloyDbTransactor.make[IO](
      sys.env.getOrElse("DATABASE_URL", "jdbc:postgresql://localhost:5432/repcheck"),
      sys.env.getOrElse("DATABASE_USER", "repcheck"),
      sys.env.getOrElse("DATABASE_PASSWORD", "")
    ).use { xa =>

      // ANNOTATION: Step 3 — The for-comprehension orchestration.
      // Each line is a sequential step:
      //   config <- loadedConfig         (parse CLI args)
      //   api <- LegislativeBillsApi...  (create API client)
      //   ...                            (compute date range)
      //   _ <- stream...drain            (run the pipeline)
      //
      // If ANY step fails, the whole chain short-circuits via IO's
      // MonadError behavior — no manual try/catch needed.
      for {
        config <- loadedConfig
        api    <- LegislativeBillsApi[IO](config.apiKey, config.pageSize)
        lookbackWindowStart <- IO {
          Option(
            ZonedDateTime
              .now(ZoneId.of("UTC"))
              .minusDays(config.billLookBackInDays)
          )
        }
        now <- IO {
          Option(ZonedDateTime.now(ZoneId.of("UTC")))
        }
        // ANNOTATION: The FS2 stream is compiled here.
        // .compile.drain runs the stream discarding intermediate values —
        // each bill is persisted to AlloyDB as the stream runs.
        _ <- streamAllToAlloyDb(
          xa,
          api,
          lookbackWindowStart,
          now,
          config.pageSize
        ).compile.drain
      } yield ()
    }
    x
  }.as(ExitCode.Success)
  // ANNOTATION: .as(ExitCode.Success) transforms the IO[Unit]
  // into IO[ExitCode]. If the IO fails, Cats Effect will print the error
  // and return a non-zero exit code automatically.

  // ANNOTATION: Streaming pipeline method.
  // Fetches pages from the API and persists each bill to AlloyDB via Doobie.
  // Uses FS2 Stream for lazy, memory-efficient processing.
  //
  // The recursion pattern: fetch a page → persist → if page was full,
  // recurse with increased offset. If page was partial, stop.
  private def streamAllToAlloyDb(
      xa: Transactor[IO],
      api: LegislativeBillsApi[IO],
      lookbackWindowStart: Option[ZonedDateTime],
      now: Option[ZonedDateTime],
      offset: Int = 0
  ): fs2.Stream[IO, Unit] = {
    for {
      _ <- fs2.Stream.eval(IO(logger.info(s"Offset: $offset")))
      billsStream <- api.streamBatch(
        fromDateTime = lookbackWindowStart,
        toDateTime = now,
        offset = offset
      )
      recurse = billsStream.lengthRetrieved == api.pageSize
      _ <- fs2.Stream.eval(
        IO(
          logger.info(
            s"Recurse: $recurse, Bills Retrieved: ${billsStream.lengthRetrieved}, " +
            s"Page Size: ${api.pageSize}, Offset: $offset"
          )
        )
      )
      // ANNOTATION: Inner stream processes individual bills.
      // fs2.Stream.emits creates a stream from the list of bills,
      // then .evalMap runs the DTO→DO→save pipeline for each one.
      //
      // IO.fromEither converts the Either[String, DO] from toDO
      // into an IO that fails on Left — this is where invalid bill
      // types would cause a failure for that specific bill.
      _ <- fs2.Stream.eval(
        fs2.Stream
          .emits(billsStream.bills)
          .covary[IO]
          .evalMap { bill =>
            IO.fromEither(bill.toDO.left.map(new IllegalArgumentException(_)))
              .flatMap(_.saveBill[IO](xa, logger))
          }
          .compile
          .drain
      )
      // ANNOTATION: Stream concatenation for recursion.
      // Appends the recursive call (next page) or an empty stream.
      // This is lazy — the next page isn't fetched until
      // the consumer pulls from the stream.
      result <- fs2.Stream.emit[IO, Unit](()) ++
        (if (recurse) {
           streamAllToAlloyDb(
             xa,
             api,
             lookbackWindowStart,
             now,
             offset + api.pageSize
           )
         } else {
           fs2.Stream.empty: fs2.Stream[IO, Unit]
         })
    } yield result
  }
}
```

## Key Patterns to Follow

1. **IOApp provides the runtime** — never create your own `IORuntime`
2. **For-comprehension for sequential steps** — each `<-` is a sequential step that can fail
3. **Transactor as Resource** — `AlloyDbTransactor.make(...).use { xa => ... }` keeps the HikariCP pool alive for the duration
4. **Config first, then resources, then work** — fail fast if config is invalid
4. **FS2 Stream for batch processing** — compile at the end, not in the middle
5. **ExitCode.Success via .as** — transform any result type to ExitCode
6. **Environment variables with defaults** — `sys.env.getOrElse` for GCP project IDs, etc.

## How to Create a New Pipeline Entry Point

```scala
object VoteIngestionApp extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    AlloyDbTransactor.make[IO](dbUrl, dbUser, dbPassword).use { xa =>
      for {
        config   <- ConfigLoader.LoadConfig(args)
        snapshot <- SnapshotService.loadSnapshot(config.snapshotPath)
        api      <- VotesApi[IO](config.apiKey, config.pageSize)
        _        <- VoteIngestionPipeline
                      .run(api, xa, snapshot)
                      .compile
                      .drain
      } yield ExitCode.Success
    }
  }
}
```
