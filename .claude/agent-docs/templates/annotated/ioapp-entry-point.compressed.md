<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/annotated/ioapp-entry-point.md -->

# Pattern: IOApp Entry Point

IOApp is the Cats Effect entry point orchestrating configuration, resources, and stream processing in a for-comprehension. The app is thin glue code delegating work to libraries.

**When To Use:** Every Cloud Run Job needs an IOApp entry point (bill ingestion, vote ingestion, analysis, scoring pipelines).

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

object BillIdentifierApp extends IOApp {

  private val logger = LoggerFactory.getLogger(getClass)

  override def run(args: List[String]): IO[ExitCode] = {
    // Step 1: Load config (returns IO[Config], validation happens at runtime)
    val loadedConfig: IO[BillIdentifierConfig] = ConfigLoader.LoadConfig(args)

    // Step 2: Build Transactor as Resource (HikariCP pool, auto-closed via .use)
    val x: IO[Unit] = AlloyDbTransactor.make[IO](
      sys.env.getOrElse("DATABASE_URL", "jdbc:postgresql://localhost:5432/repcheck"),
      sys.env.getOrElse("DATABASE_USER", "repcheck"),
      sys.env.getOrElse("DATABASE_PASSWORD", "")
    ).use { xa =>

      // Step 3: For-comprehension orchestration. Each line is sequential; any failure short-circuits.
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
        // Compile and drain FS2 stream (persists bills to AlloyDB as it runs)
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

  // Streaming pipeline: fetch pages from API, persist to AlloyDB. Recurses until page is partial.
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
      // Process individual bills: DTO→DO→save via evalMap
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
      // Lazy stream concatenation: append next page or empty stream
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

## Key Patterns

- **IOApp provides runtime** — never create custom IORuntime
- **For-comprehension for sequential steps** — each `<-` fails the whole chain on error
- **Transactor as Resource** — `.use { xa => ... }` keeps HikariCP pool open for duration
- **Config → resources → work** — fail fast on invalid config
- **FS2 Stream for batching** — compile/drain at the end, not mid-stream
- **ExitCode.Success via .as** — transforms result to ExitCode
- **Environment variables with defaults** — `sys.env.getOrElse(key, default)`

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