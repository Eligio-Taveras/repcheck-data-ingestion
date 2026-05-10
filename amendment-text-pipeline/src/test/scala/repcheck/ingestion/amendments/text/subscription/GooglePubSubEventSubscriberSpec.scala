package repcheck.ingestion.amendments.text.subscription

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{
  AcknowledgeRequest,
  ModifyAckDeadlineRequest,
  PubsubMessage,
  PullRequest,
  PullResponse,
  ReceivedMessage,
}

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{AmendmentTextAvailableEvent, PipelineEvent}
import repcheck.shared.models.congress.amendment.AmendmentType

/**
 * Specs for the amendment-side [[GooglePubSubEventSubscriber]]. Mirror of the bill-side spec; covers deserialization,
 * pull semantics, ack semantics, resource lifecycle.
 */
class GooglePubSubEventSubscriberSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private class StubPipelineLogger extends PipelineLogger[IO] {
    private val messagesRef = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)

    override def info(context: LogContext, message: String): IO[Unit] = IO {
      val _ = messagesRef.updateAndGet(msgs => msgs :+ s"INFO: $message")
    }

    override def warn(context: LogContext, message: String): IO[Unit] = IO {
      val _ = messagesRef.updateAndGet(msgs => msgs :+ s"WARN: $message")
    }

    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO {
      val _ = messagesRef.updateAndGet(msgs => msgs :+ s"ERROR: $message")
    }

    override def debug(context: LogContext, message: String): IO[Unit] = IO {
      val _ = messagesRef.updateAndGet(msgs => msgs :+ s"DEBUG: $message")
    }

    def messages: List[String] = messagesRef.get()
  }

  private def makeTestEvent(
    naturalKey: String = "117-SAMDT-2137",
    correlationId: UUID = UUID.randomUUID(),
  ): PipelineEvent[AmendmentTextAvailableEvent] =
    PipelineEvent(
      eventType = "amendment.text.available",
      payload = AmendmentTextAvailableEvent(
        amendmentId = 42L,
        naturalKey = naturalKey,
        congress = 117,
        amendmentType = AmendmentType.SAMDT,
        number = "2137",
        versionTypeCode = "SUB",
        formatType = "HTML",
        url = "https://www.congress.gov/117/crec/2021/08/01/167/136/CREC-2021-08-01-pt1-PgS5255.htm",
        publishedDate = Some(Instant.parse("2021-08-01T04:00:00Z")),
        correlationId = correlationId,
      ),
      timestamp = Instant.now(),
      eventId = UUID.randomUUID(),
      correlationId = correlationId,
      source = "test-checker",
    )

  private def makeSubscriber(logger: StubPipelineLogger): GooglePubSubEventSubscriber[IO] = {
    val stubStub = mock[SubscriberStub]
    new GooglePubSubEventSubscriber[IO](stubStub, "test-sub", logger)
  }

  "deserializeEvent" should "parse a valid PipelineEvent JSON" in {
    val logger     = new StubPipelineLogger
    val subscriber = makeSubscriber(logger)
    val event      = makeTestEvent()
    val json       = PipelineEvent.encoder[AmendmentTextAvailableEvent].apply(event).noSpaces

    val result = subscriber.deserializeEvent(json, "ack-1").unsafeRunSync()
    val _      = result.isDefined shouldBe true
    val _      = result.map(_.ackId) shouldBe Some("ack-1")
    result.map(_.event.payload.naturalKey) shouldBe Some("117-SAMDT-2137")
  }

  it should "return None for invalid JSON and log a warning" in {
    val logger     = new StubPipelineLogger
    val subscriber = makeSubscriber(logger)
    val result     = subscriber.deserializeEvent("not valid json", "ack-2").unsafeRunSync()
    val _          = result shouldBe None
    logger.messages.exists(_.contains("Failed to deserialize")) shouldBe true
  }

  it should "return None for JSON with the wrong schema" in {
    val logger     = new StubPipelineLogger
    val subscriber = makeSubscriber(logger)
    val result     = subscriber.deserializeEvent("""{"wrong": "schema"}""", "ack-3").unsafeRunSync()
    result shouldBe None
  }

  it should "preserve correlationId from the event envelope" in {
    val logger        = new StubPipelineLogger
    val subscriber    = makeSubscriber(logger)
    val correlationId = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    val event         = makeTestEvent(correlationId = correlationId)
    val json          = PipelineEvent.encoder[AmendmentTextAvailableEvent].apply(event).noSpaces
    val result        = subscriber.deserializeEvent(json, "ack-4").unsafeRunSync()
    result.map(_.event.correlationId) shouldBe Some(correlationId)
  }

  // --- pull/acknowledge tests ---

  private def makeStubSubscriber(logger: StubPipelineLogger, stub: SubscriberStub) =
    new GooglePubSubEventSubscriber[IO](stub, "projects/p/subscriptions/test-sub", logger)

  private def buildReceivedMessage(json: String, ackId: String): ReceivedMessage = {
    val pubsubMsg = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(json)).build()
    ReceivedMessage.newBuilder().setMessage(pubsubMsg).setAckId(ackId).build()
  }

  private def validJson(naturalKey: String): String = {
    val event = makeTestEvent(naturalKey = naturalKey)
    PipelineEvent.encoder[AmendmentTextAvailableEvent].apply(event).noSpaces
  }

  "pull" should "return deserialized events for valid messages" in {
    val logger       = new StubPipelineLogger
    val stubMock     = mock[SubscriberStub]
    val pullCallable = mock[UnaryCallable[PullRequest, PullResponse]]

    val msg1 = buildReceivedMessage(validJson("117-SAMDT-1"), "ack-100")
    val msg2 = buildReceivedMessage(validJson("117-HAMDT-5"), "ack-101")
    val pullResponse =
      PullResponse.newBuilder().addReceivedMessages(msg1).addReceivedMessages(msg2).build()

    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(pullResponse)

    val subscriber = makeStubSubscriber(logger, stubMock)
    val result     = subscriber.pull(10).unsafeRunSync()

    val _ = result should have size 2
    val _ = result.headOption.map(_.ackId) shouldBe Some("ack-100")
    result.lift(1).map(_.event.payload.naturalKey) shouldBe Some("117-HAMDT-5")
  }

  it should "return empty list when no messages are available" in {
    val logger       = new StubPipelineLogger
    val stubMock     = mock[SubscriberStub]
    val pullCallable = mock[UnaryCallable[PullRequest, PullResponse]]
    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(PullResponse.newBuilder().build())

    val subscriber = makeStubSubscriber(logger, stubMock)
    val result     = subscriber.pull(5).unsafeRunSync()
    val _          = result shouldBe empty
    logger.messages.exists(_.contains("Pulled 0 messages")) shouldBe true
  }

  it should "skip invalid messages and return only valid ones" in {
    val logger       = new StubPipelineLogger
    val stubMock     = mock[SubscriberStub]
    val pullCallable = mock[UnaryCallable[PullRequest, PullResponse]]

    val invalid = buildReceivedMessage("not valid", "ack-bad")
    val valid   = buildReceivedMessage(validJson("117-SAMDT-99"), "ack-ok")
    val pullResponse =
      PullResponse.newBuilder().addReceivedMessages(invalid).addReceivedMessages(valid).build()

    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(pullResponse)

    val subscriber = makeStubSubscriber(logger, stubMock)
    val result     = subscriber.pull(10).unsafeRunSync()
    val _          = result should have size 1
    result.headOption.map(_.ackId) shouldBe Some("ack-ok")
  }

  // --- acknowledge ---

  "acknowledge" should "invoke the ack callable for non-empty ack IDs" in {
    val logger      = new StubPipelineLogger
    val stubMock    = mock[SubscriberStub]
    val ackCallable = mock[UnaryCallable[AcknowledgeRequest, com.google.protobuf.Empty]]

    when(stubMock.acknowledgeCallable()).thenReturn(ackCallable)
    when(ackCallable.call(any[AcknowledgeRequest]())).thenReturn(com.google.protobuf.Empty.getDefaultInstance)

    val subscriber = makeStubSubscriber(logger, stubMock)
    subscriber.acknowledge(List("ack-1", "ack-2")).unsafeRunSync()
    verify(ackCallable).call(any[AcknowledgeRequest]())
  }

  it should "not invoke the callable when the ack IDs list is empty" in {
    val logger      = new StubPipelineLogger
    val stubMock    = mock[SubscriberStub]
    val ackCallable = mock[UnaryCallable[AcknowledgeRequest, com.google.protobuf.Empty]]
    when(stubMock.acknowledgeCallable()).thenReturn(ackCallable)
    val subscriber = makeStubSubscriber(logger, stubMock)
    subscriber.acknowledge(List.empty).unsafeRunSync()
    verify(stubMock, never()).acknowledgeCallable()
  }

  // --- nack ---

  "nack" should "invoke modifyAckDeadlineCallable with deadlineSeconds=0 for non-empty ack IDs" in {
    val logger       = new StubPipelineLogger
    val stubMock     = mock[SubscriberStub]
    val nackCallable = mock[UnaryCallable[ModifyAckDeadlineRequest, com.google.protobuf.Empty]]

    when(stubMock.modifyAckDeadlineCallable()).thenReturn(nackCallable)
    when(nackCallable.call(any[ModifyAckDeadlineRequest]())).thenReturn(com.google.protobuf.Empty.getDefaultInstance)

    val subscriber = makeStubSubscriber(logger, stubMock)
    subscriber.nack(List("ack-1", "ack-2")).unsafeRunSync()

    val captor = org.mockito.ArgumentCaptor.forClass(classOf[ModifyAckDeadlineRequest])
    val _      = verify(nackCallable).call(captor.capture())
    val _      = captor.getValue.getAckDeadlineSeconds shouldBe 0
    captor.getValue.getAckIdsList.size shouldBe 2
  }

  it should "not invoke the callable when the ack IDs list is empty" in {
    val logger       = new StubPipelineLogger
    val stubMock     = mock[SubscriberStub]
    val nackCallable = mock[UnaryCallable[ModifyAckDeadlineRequest, com.google.protobuf.Empty]]
    when(stubMock.modifyAckDeadlineCallable()).thenReturn(nackCallable)
    val subscriber = makeStubSubscriber(logger, stubMock)
    subscriber.nack(List.empty).unsafeRunSync()
    verify(stubMock, never()).modifyAckDeadlineCallable()
  }

  // --- PubSubSubscriberResource ---

  "PubSubSubscriberResource.make" should "create + close a subscriber lifecycle" in {
    val logger   = new StubPipelineLogger
    val stubMock = mock[SubscriberStub]
    val config = EventSubscriberConfig(
      "test-project",
      "test-subscription",
      10,
      scala.concurrent.duration.DurationInt(30).seconds,
    )
    val pullCallable = mock[UnaryCallable[PullRequest, PullResponse]]
    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(PullResponse.newBuilder().build())

    val _ = PubSubSubscriberResource
      .make[IO](config, logger, () => stubMock)
      .use(_ => IO.unit)
      .unsafeRunSync()
    verify(stubMock).close()
  }

  it should "build a subscription name in projects/X/subscriptions/Y form" in {
    val logger   = new StubPipelineLogger
    val stubMock = mock[SubscriberStub]
    val config =
      EventSubscriberConfig("my-proj", "my-sub", 5, scala.concurrent.duration.DurationInt(30).seconds)

    val pullCallable = mock[UnaryCallable[PullRequest, PullResponse]]
    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(PullResponse.newBuilder().build())

    val _ = PubSubSubscriberResource
      .make[IO](config, logger, () => stubMock)
      .use(sub => sub.pull(1))
      .unsafeRunSync()

    val captor = org.mockito.ArgumentCaptor.forClass(classOf[PullRequest])
    verify(pullCallable).call(captor.capture())
    captor.getValue.getSubscription shouldBe "projects/my-proj/subscriptions/my-sub"
  }

}
