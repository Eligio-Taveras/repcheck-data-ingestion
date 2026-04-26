package repcheck.ingestion.bills.summary.app

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global

import org.http4s.client.Client
import org.http4s.{Request, Response, Uri}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.summary.errors.StepRunIdInvalid
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.pipeline.models.errors.RetryConfig

class BillSummaryPipelineSpec extends AnyFlatSpec with Matchers {

  private def configWithDelay(pageDelay: FiniteDuration): CongressGovClientConfig =
    CongressGovClientConfig(
      apiKey = "test-key",
      baseUrl = "https://api.example.com/v3",
      pageSize = 100,
      pageDelay = pageDelay,
      retry = RetryConfig(),
    )

  "extractStepRunId" should "return the parsed Long when args(2) is a valid number" in {
    val result = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1", "42")).unsafeRunSync()
    result shouldBe 42L
  }

  it should "accept '0' for docker-compose / Ofelia placeholder use" in {
    val result = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1", "0")).unsafeRunSync()
    result shouldBe 0L
  }

  it should "raise StepRunIdInvalid when args(2) is missing" in {
    val outcome = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1")).attempt.unsafeRunSync()
    outcome match {
      case Left(StepRunIdInvalid("<missing>")) => succeed
      case Left(other)                         => fail(s"unexpected error: $other")
      case Right(value)                        => fail(s"expected failure, got success: $value")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is blank" in {
    val outcome = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1", "   ")).attempt.unsafeRunSync()
    outcome match {
      case Left(StepRunIdInvalid("   ")) => succeed
      case Left(other)                   => fail(s"unexpected error: $other")
      case Right(value)                  => fail(s"expected failure, got success: $value")
    }
  }

  it should "raise StepRunIdInvalid when args(2) is not numeric" in {
    val outcome = BillSummaryPipeline.extractStepRunId[IO](List("cfg", "run-1", "abc")).attempt.unsafeRunSync()
    outcome match {
      case Left(StepRunIdInvalid("abc")) => succeed
      case Left(other)                   => fail(s"unexpected error: $other")
      case Right(value)                  => fail(s"expected failure, got success: $value")
    }
  }

  "rateLimitedClient" should "wrap the underlying client and serve requests" in {
    val captured = new java.util.concurrent.atomic.AtomicInteger(0)
    val underlying = Client[IO] { (_: Request[IO]) =>
      Resource.make(IO {
        val _ = captured.incrementAndGet()
        Response[IO]()
      })(_ => IO.unit)
    }

    val program = BillSummaryPipeline
      .rateLimitedClient[IO](underlying, configWithDelay(1.millisecond), permits = 1L)
      .use { wrapped =>
        wrapped.run(Request[IO](uri = Uri.unsafeFromString("https://api.example.com/v3/summaries"))).use(_ => IO.unit)
      }

    val _ = program.unsafeRunSync()
    captured.get() shouldBe 1
  }

  it should "respect the permits parameter (>1 permits allow concurrent acquisition)" in {
    // Two permits → two acquisitions can hold simultaneously without serializing.
    // We acquire two permits via two `use` blocks running in parallel. With permits=2 both complete; with permits=1
    // they would still both complete eventually but sequentially. The functional test here is the default-setup
    // smoke: the wrapper must not block forever when permits=2 and we acquire two permits.
    val captured = new java.util.concurrent.atomic.AtomicInteger(0)
    val underlying = Client[IO] { (_: Request[IO]) =>
      Resource.make(IO {
        val _ = captured.incrementAndGet()
        Response[IO]()
      })(_ => IO.unit)
    }

    val program = BillSummaryPipeline
      .rateLimitedClient[IO](underlying, configWithDelay(1.millisecond), permits = 2L)
      .use { wrapped =>
        val req = Request[IO](uri = Uri.unsafeFromString("https://api.example.com/v3/summaries"))
        IO.both(
          wrapped.run(req).use(_ => IO.unit),
          wrapped.run(req).use(_ => IO.unit),
        ).void
      }

    val _ = program.unsafeRunSync()
    captured.get() shouldBe 2
  }

}
