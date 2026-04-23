package repcheck.ingestion.votes.app

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.http4s.client.Client

import doobie.util.transactor.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.events.{EventPublisherConfig, PubSubEventPublisher}
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig, VotesPipelineConfig}
import repcheck.pipeline.models.errors.RetryConfig

/**
 * Unit spec for [[VotesPipelineResources]]. Exercises `build` end-to-end with mocked low-level factories so the whole
 * composition (transactor → rate-limited clients → Pub/Sub publisher) is covered without standing up real SDKs.
 */
class VotesPipelineResourcesSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private def makeConfig(): VotesPipeline.AppConfig =
    VotesPipeline.AppConfig(
      database = DatabaseConfig(
        host = "localhost",
        port = 5432,
        database = "repcheck_test",
        username = "test",
        password = "test",
        maxConnections = 3,
      ),
      congressApi = CongressGovClientConfig(
        baseUrl = "http://localhost:0",
        apiKey = "test-key",
        pageSize = 10,
        pageDelay = 1.millis,
        retry = RetryConfig(),
      ),
      pipeline = VotesPipelineConfig(
        house = HouseVotesConfig(congress = 118, session = 1, parallelism = 1, pageDelay = 1.millis, lookbackDays = 0),
        senate = SenateVoteXmlConfig(),
      ),
      eventPublisher =
        EventPublisherConfig(projectId = "repcheck-test", topicName = "vote-events", source = "votes-pipeline"),
    )

  "build" should "compose a Resources bundle with all four fields populated" in {
    val config          = makeConfig()
    val xa              = mock[Transactor[IO]]
    val rawClient       = mock[Client[IO]]
    val pubSubPublisher = mock[PubSubEventPublisher[IO]]

    val resources = VotesPipelineResources
      .build[IO](
        config = config,
        transactorFactory = (_: DatabaseConfig) => Resource.pure[IO, Transactor[IO]](xa),
        httpClientFactory = Resource.pure[IO, Client[IO]](rawClient),
        pubSubPublisherFactory =
          (_: EventPublisherConfig) => Resource.pure[IO, PubSubEventPublisher[IO]](pubSubPublisher),
      )
      .use(IO.pure)
      .unsafeRunSync()

    val _ = resources.xa shouldBe xa
    val _ = resources.eventPublisher.toString should not be empty
    val _ = resources.retryWrapper.toString should not be empty
    // The houseClient / senateClient are wrapped Client instances — they aren't the raw client by reference identity.
    val _ = resources.houseClient should not be rawClient
    resources.senateClient should not be rawClient
  }

  it should "call each low-level factory exactly once" in {
    val config                = makeConfig()
    val xa                    = mock[Transactor[IO]]
    val rawClient             = mock[Client[IO]]
    val pubSubPublisher       = mock[PubSubEventPublisher[IO]]
    val transactorFactoryHits = new AtomicInteger(0)
    val httpFactoryHits       = new AtomicInteger(0)
    val pubSubFactoryHits     = new AtomicInteger(0)

    val _ = VotesPipelineResources
      .build[IO](
        config = config,
        transactorFactory =
          (_: DatabaseConfig) => Resource.eval(IO { val _ = transactorFactoryHits.incrementAndGet(); xa }),
        httpClientFactory = Resource.eval(IO { val _ = httpFactoryHits.incrementAndGet(); rawClient }),
        pubSubPublisherFactory = (_: EventPublisherConfig) =>
          Resource.eval(IO { val _ = pubSubFactoryHits.incrementAndGet(); pubSubPublisher }),
      )
      .use(_ => IO.unit)
      .unsafeRunSync()

    val _ = transactorFactoryHits.get() shouldBe 1
    val _ = httpFactoryHits.get() shouldBe 1
    pubSubFactoryHits.get() shouldBe 1
  }

  it should "wrap house and senate clients as distinct rate-limited clients (different Semaphore instances)" in {
    val config          = makeConfig()
    val xa              = mock[Transactor[IO]]
    val rawClient       = mock[Client[IO]]
    val pubSubPublisher = mock[PubSubEventPublisher[IO]]

    val resources = VotesPipelineResources
      .build[IO](
        config = config,
        transactorFactory = (_: DatabaseConfig) => Resource.pure[IO, Transactor[IO]](xa),
        httpClientFactory = Resource.pure[IO, Client[IO]](rawClient),
        pubSubPublisherFactory =
          (_: EventPublisherConfig) => Resource.pure[IO, PubSubEventPublisher[IO]](pubSubPublisher),
      )
      .use(IO.pure)
      .unsafeRunSync()

    // Sanity: each chamber gets its own wrapped client — not the same instance.
    resources.houseClient should not be resources.senateClient
  }

  it should "thread the supplied AppConfig's database section into transactorFactory" in {
    val config                 = makeConfig()
    val observedDatabaseConfig = new java.util.concurrent.atomic.AtomicReference[Option[DatabaseConfig]](None)

    val _ = VotesPipelineResources
      .build[IO](
        config = config,
        transactorFactory = (db: DatabaseConfig) =>
          Resource.eval(IO {
            val _ = observedDatabaseConfig.set(Some(db))
            mock[Transactor[IO]]
          }),
        httpClientFactory = Resource.pure[IO, Client[IO]](mock[Client[IO]]),
        pubSubPublisherFactory =
          (_: EventPublisherConfig) => Resource.pure[IO, PubSubEventPublisher[IO]](mock[PubSubEventPublisher[IO]]),
      )
      .use(_ => IO.unit)
      .unsafeRunSync()

    observedDatabaseConfig.get() shouldBe Some(config.database)
  }

  it should "thread the supplied AppConfig's eventPublisher section into pubSubPublisherFactory" in {
    val config = makeConfig()
    val observedEventPublisherConfig =
      new java.util.concurrent.atomic.AtomicReference[Option[EventPublisherConfig]](None)

    val _ = VotesPipelineResources
      .build[IO](
        config = config,
        transactorFactory = (_: DatabaseConfig) => Resource.pure[IO, Transactor[IO]](mock[Transactor[IO]]),
        httpClientFactory = Resource.pure[IO, Client[IO]](mock[Client[IO]]),
        pubSubPublisherFactory = (ep: EventPublisherConfig) =>
          Resource.eval(IO {
            val _ = observedEventPublisherConfig.set(Some(ep))
            mock[PubSubEventPublisher[IO]]
          }),
      )
      .use(_ => IO.unit)
      .unsafeRunSync()

    observedEventPublisherConfig.get() shouldBe Some(config.eventPublisher)
  }

  // =====================================================================================
  // noOpRetryWrapper — ensures the retry-logging callback body (which returns IO.unit)
  // actually executes under test. Without this, the lambda body is uncovered because
  // `build` wires it into DefaultIngestionEventPublisher / HouseVotesApiClient /
  // SenateVoteXmlClient but none of those invoke it during the happy path.
  // =====================================================================================

  "noOpRetryWrapper" should "successfully retry a recoverable operation, firing the no-op retry logger on the way" in {
    import java.util.UUID
    import repcheck.pipeline.models.errors.{ErrorClass, ErrorClassifier, RetryConfig}

    val wrapper  = VotesPipelineResources.noOpRetryWrapper[IO]
    val attempts = new java.util.concurrent.atomic.AtomicInteger(0)
    val config   = RetryConfig(maxRetries = 2, initialBackoffMs = 1L, maxBackoffMs = 5L, backoffMultiplier = 2.0)
    val classifier = new ErrorClassifier {
      override def classify(t: Throwable): ErrorClass = ErrorClass.Transient
    }

    // Operation fails once, then succeeds. RetryWrapper calls its logging callback on the intermediate retry,
    // exercising the lambda body `(_, _, _, _, _, _) => Async[F].unit`.
    val operation: IO[String] = IO.delay(attempts.incrementAndGet()).flatMap { n =>
      if (n < 2) {
        IO.raiseError[String](
          new repcheck.ingestion.votes.errors.HouseVoteFetchFailed(
            119,
            1,
            Some(1),
            s"intentional-failure-${n.toString}",
            new repcheck.ingestion.votes.errors.HouseVoteApiHttpError(500, "simulated"),
          )
        )
      } else {
        IO.pure("ok")
      }
    }

    val outcome = wrapper
      .withRetry(
        operation = operation,
        config = config,
        classifier = classifier,
        errorFactory =
          (msg, cause) => new repcheck.ingestion.votes.errors.HouseVoteFetchFailed(119, 1, Some(1), msg, cause),
        correlationId = UUID.randomUUID(),
      )
      .unsafeRunSync()

    val _ = outcome shouldBe "ok"
    // `attempts == 2` means the wrapper retried once (fired the logger) and then succeeded.
    attempts.get() shouldBe 2
  }

}
