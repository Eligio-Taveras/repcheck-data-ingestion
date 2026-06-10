package repcheck.ingestion.amendments.textcheck.events

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.events.{EventPublishFailed, PubSubEventPublisher}
import repcheck.pipeline.models.events.{AmendmentTextAvailableEvent, EventTypes}
import repcheck.shared.models.congress.amendment.AmendmentType

import com.repcheck.utils.errors.{RetryConfig, RetryWrapper}

class DefaultAmendmentTextEventPublisherSpec extends AnyFlatSpec with Matchers {

  private val correlationId = UUID.fromString("11111111-2222-3333-4444-555555555555")

  private def sampleEvent(): AmendmentTextAvailableEvent =
    AmendmentTextAvailableEvent(
      amendmentId = 7L,
      naturalKey = "117-SAMDT-2137",
      congress = 117,
      amendmentType = AmendmentType.SAMDT,
      number = "2137",
      versionTypeCode = "SUB",
      formatType = "HTML",
      url = "https://www.congress.gov/sub.htm",
      publishedDate = Some(Instant.parse("2024-04-01T12:00:00Z")),
      correlationId = correlationId,
    )

  private val retryConfig =
    RetryConfig(maxRetries = 1, initialBackoffMs = 1L, maxBackoffMs = 5L, backoffMultiplier = 1.0)

  private val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)

  "publish" should "delegate to PubSubEventPublisher with the configured topic and event-type attribute" in {
    val capturedTopic   = new AtomicReference[String]("")
    val capturedAttrs   = new AtomicReference[Map[String, String]](Map.empty)
    val capturedPayload = new AtomicReference[String]("")

    val pubsub = new PubSubEventPublisher[IO] {
      override def publish(topic: String, data: String, attributes: Map[String, String]): IO[String] = IO {
        capturedTopic.set(topic)
        capturedPayload.set(data)
        capturedAttrs.set(attributes)
        "ok"
      }
    }

    val publisher = new DefaultAmendmentTextEventPublisher[IO](
      publisher = pubsub,
      topicName = "amendment.text.available",
      source = "test",
      retryWrapper = retryWrapper,
      retryConfig = retryConfig,
    )

    val msgId = publisher.publish(sampleEvent(), correlationId).unsafeRunSync()
    val _     = msgId shouldBe "ok"
    val _     = capturedTopic.get shouldBe "amendment.text.available"
    val _     = capturedAttrs.get shouldBe Map("eventType" -> EventTypes.AmendmentTextAvailable)
    val _     = capturedPayload.get should include("117-SAMDT-2137")
    capturedPayload.get should include("SUB")
  }

  it should "retry on transient IOException and eventually succeed" in {
    val attempts = new AtomicInteger(0)
    val pubsub = new PubSubEventPublisher[IO] {
      override def publish(topic: String, data: String, attributes: Map[String, String]): IO[String] =
        IO.delay(attempts.incrementAndGet()).flatMap { n =>
          if (n < 2) { IO.raiseError(new java.io.IOException("flaky network")) }
          else { IO.pure("ok-after-retry") }
        }
    }

    val publisher = new DefaultAmendmentTextEventPublisher[IO](
      publisher = pubsub,
      topicName = "topic",
      source = "test",
      retryWrapper = retryWrapper,
      retryConfig = RetryConfig(maxRetries = 3, initialBackoffMs = 1L, maxBackoffMs = 5L, backoffMultiplier = 1.0),
    )

    val result = publisher.publish(sampleEvent(), correlationId).unsafeRunSync()
    val _      = result shouldBe "ok-after-retry"
    attempts.get shouldBe 2
  }

  it should "wrap a Systemic publish failure as EventPublishFailed" in {
    val pubsub = new PubSubEventPublisher[IO] {
      override def publish(topic: String, data: String, attributes: Map[String, String]): IO[String] =
        IO.raiseError(new IllegalStateException("permanent failure"))
    }

    val publisher = new DefaultAmendmentTextEventPublisher[IO](
      publisher = pubsub,
      topicName = "topic",
      source = "test",
      retryWrapper = retryWrapper,
      retryConfig = RetryConfig(maxRetries = 0, initialBackoffMs = 1L, maxBackoffMs = 5L, backoffMultiplier = 1.0),
    )

    val ex = intercept[EventPublishFailed] {
      publisher.publish(sampleEvent(), correlationId).unsafeRunSync()
    }
    ex.getMessage should include("topic")
  }

}
