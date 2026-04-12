package com.repcheck.bills.text.subscription

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import com.google.api.gax.rpc.UnaryCallable
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{AcknowledgeRequest, PubsubMessage, PullRequest, PullResponse, ReceivedMessage}

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.{BillTextAvailableEvent, PipelineEvent}

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
    naturalKey: String = "118-HR-1",
    congress: Int = 118,
    correlationId: UUID = UUID.randomUUID(),
  ): PipelineEvent[BillTextAvailableEvent] =
    PipelineEvent(
      eventType = "bill.text.available",
      payload = BillTextAvailableEvent(
        naturalKey = naturalKey,
        congress = congress,
        textUrl = "https://api.congress.gov/text",
        textFormat = "Formatted Text",
        versionCode = "IH",
        previousVersionCode = None,
      ),
      timestamp = Instant.now(),
      eventId = UUID.randomUUID(),
      correlationId = correlationId,
      source = "test-checker",
    )

  private def makeSubscriber(logger: StubPipelineLogger): GooglePubSubEventSubscriber[IO] = {
    val stubStub = mock[com.google.cloud.pubsub.v1.stub.SubscriberStub]
    new GooglePubSubEventSubscriber[IO](stubStub, "test-sub", logger)
  }

  "deserializeEvent" should "parse a valid PipelineEvent JSON" in {
    val logger     = new StubPipelineLogger
    val subscriber = makeSubscriber(logger)
    val event      = makeTestEvent()
    val json       = PipelineEvent.encoder[BillTextAvailableEvent].apply(event).noSpaces

    val result = subscriber.deserializeEvent(json, "ack-1").unsafeRunSync()
    val _      = result.isDefined shouldBe true
    val _      = result.map(_.ackId) shouldBe Some("ack-1")
    result.map(_.event.payload.naturalKey) shouldBe Some("118-HR-1")
  }

  it should "return None for invalid JSON" in {
    val logger     = new StubPipelineLogger
    val subscriber = makeSubscriber(logger)

    val result = subscriber.deserializeEvent("not valid json", "ack-2").unsafeRunSync()
    val _      = result shouldBe None
    logger.messages.exists(_.contains("Failed to deserialize")) shouldBe true
  }

  it should "return None for JSON with wrong schema" in {
    val logger     = new StubPipelineLogger
    val subscriber = makeSubscriber(logger)

    val result = subscriber.deserializeEvent("""{"wrong": "schema"}""", "ack-3").unsafeRunSync()
    result shouldBe None
  }

  it should "preserve correlationId from the event envelope" in {
    val logger        = new StubPipelineLogger
    val subscriber    = makeSubscriber(logger)
    val correlationId = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    val event         = makeTestEvent(correlationId = correlationId)
    val json          = PipelineEvent.encoder[BillTextAvailableEvent].apply(event).noSpaces

    val result = subscriber.deserializeEvent(json, "ack-4").unsafeRunSync()
    result.map(_.event.correlationId) shouldBe Some(correlationId)
  }

  it should "preserve all event payload fields" in {
    val logger     = new StubPipelineLogger
    val subscriber = makeSubscriber(logger)
    val event      = makeTestEvent(naturalKey = "117-S-42", congress = 117)
    val json       = PipelineEvent.encoder[BillTextAvailableEvent].apply(event).noSpaces

    val result  = subscriber.deserializeEvent(json, "ack-5").unsafeRunSync()
    val payload = result.map(_.event.payload)
    val _       = payload.map(_.naturalKey) shouldBe Some("117-S-42")
    val _       = payload.map(_.congress) shouldBe Some(117)
    val _       = payload.map(_.versionCode) shouldBe Some("IH")
    payload.map(_.textFormat) shouldBe Some("Formatted Text")
  }

  "EventSubscriberConfig" should "hold projectId" in {
    val config = EventSubscriberConfig("proj", "sub", 100)
    config.projectId shouldBe "proj"
  }

  it should "hold subscriptionId" in {
    val config = EventSubscriberConfig("proj", "sub", 100)
    config.subscriptionId shouldBe "sub"
  }

  it should "hold maxMessages" in {
    val config = EventSubscriberConfig("proj", "sub", 50)
    config.maxMessages shouldBe 50
  }

  // --- helpers for pull/acknowledge tests ---

  private def makeStubSubscriber(
    logger: StubPipelineLogger,
    stub: SubscriberStub,
  ): GooglePubSubEventSubscriber[IO] =
    new GooglePubSubEventSubscriber[IO](stub, "projects/p/subscriptions/test-sub", logger)

  private def buildReceivedMessage(json: String, ackId: String): ReceivedMessage = {
    val pubsubMsg = PubsubMessage
      .newBuilder()
      .setData(ByteString.copyFromUtf8(json))
      .build()
    ReceivedMessage
      .newBuilder()
      .setMessage(pubsubMsg)
      .setAckId(ackId)
      .build()
  }

  private def validEventJson(naturalKey: String = "118-HR-1"): String = {
    val event = makeTestEvent(naturalKey = naturalKey)
    PipelineEvent.encoder[BillTextAvailableEvent].apply(event).noSpaces
  }

  // --- pull() tests ---

  "pull" should "return deserialized events for valid messages" in {
    val logger       = new StubPipelineLogger
    val stubMock     = mock[SubscriberStub]
    val pullCallable = mock[UnaryCallable[PullRequest, PullResponse]]

    val msg1 = buildReceivedMessage(validEventJson("118-HR-1"), "ack-100")
    val msg2 = buildReceivedMessage(validEventJson("118-S-5"), "ack-101")

    val pullResponse = PullResponse
      .newBuilder()
      .addReceivedMessages(msg1)
      .addReceivedMessages(msg2)
      .build()

    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(pullResponse)

    val subscriber = makeStubSubscriber(logger, stubMock)
    val result     = subscriber.pull(10).unsafeRunSync()

    val _ = result should have size 2
    val _ = result.headOption.map(_.ackId) shouldBe Some("ack-100")
    val _ = result.headOption.map(_.event.payload.naturalKey) shouldBe Some("118-HR-1")
    val _ = result.lift(1).map(_.ackId) shouldBe Some("ack-101")
    result.lift(1).map(_.event.payload.naturalKey) shouldBe Some("118-S-5")
  }

  it should "return empty list when no messages are available" in {
    val logger       = new StubPipelineLogger
    val stubMock     = mock[SubscriberStub]
    val pullCallable = mock[UnaryCallable[PullRequest, PullResponse]]

    val emptyResponse = PullResponse.newBuilder().build()

    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(emptyResponse)

    val subscriber = makeStubSubscriber(logger, stubMock)
    val result     = subscriber.pull(5).unsafeRunSync()

    val _ = result shouldBe empty
    logger.messages.exists(_.contains("Pulled 0 messages")) shouldBe true
  }

  it should "skip invalid messages and return only valid ones" in {
    val logger       = new StubPipelineLogger
    val stubMock     = mock[SubscriberStub]
    val pullCallable = mock[UnaryCallable[PullRequest, PullResponse]]

    val validMsg    = buildReceivedMessage(validEventJson("118-HR-99"), "ack-valid")
    val invalidMsg  = buildReceivedMessage("not valid json at all", "ack-invalid")
    val wrongSchema = buildReceivedMessage("""{"wrong": "schema"}""", "ack-wrong")

    val pullResponse = PullResponse
      .newBuilder()
      .addReceivedMessages(invalidMsg)
      .addReceivedMessages(validMsg)
      .addReceivedMessages(wrongSchema)
      .build()

    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(pullResponse)

    val subscriber = makeStubSubscriber(logger, stubMock)
    val result     = subscriber.pull(10).unsafeRunSync()

    val _ = result should have size 1
    val _ = result.headOption.map(_.ackId) shouldBe Some("ack-valid")
    val _ = result.headOption.map(_.event.payload.naturalKey) shouldBe Some("118-HR-99")
    logger.messages.count(_.contains("Failed to deserialize")) shouldBe 2
  }

  it should "log the number of pulled messages" in {
    val logger       = new StubPipelineLogger
    val stubMock     = mock[SubscriberStub]
    val pullCallable = mock[UnaryCallable[PullRequest, PullResponse]]

    val msg          = buildReceivedMessage(validEventJson(), "ack-log")
    val pullResponse = PullResponse.newBuilder().addReceivedMessages(msg).build()

    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(pullResponse)

    val subscriber = makeStubSubscriber(logger, stubMock)
    val _          = subscriber.pull(1).unsafeRunSync()

    logger.messages.exists(_.contains("Pulled 1 messages")) shouldBe true
  }

  // --- acknowledge() tests ---

  "acknowledge" should "call the acknowledge callable with non-empty ack IDs" in {
    val logger      = new StubPipelineLogger
    val stubMock    = mock[SubscriberStub]
    val ackCallable = mock[UnaryCallable[AcknowledgeRequest, com.google.protobuf.Empty]]

    when(stubMock.acknowledgeCallable()).thenReturn(ackCallable)
    when(ackCallable.call(any[AcknowledgeRequest]())).thenReturn(com.google.protobuf.Empty.getDefaultInstance)

    val subscriber = makeStubSubscriber(logger, stubMock)
    subscriber.acknowledge(List("ack-1", "ack-2")).unsafeRunSync()

    verify(ackCallable).call(any[AcknowledgeRequest]())
  }

  it should "not invoke the callable when ack IDs list is empty" in {
    val logger      = new StubPipelineLogger
    val stubMock    = mock[SubscriberStub]
    val ackCallable = mock[UnaryCallable[AcknowledgeRequest, com.google.protobuf.Empty]]

    when(stubMock.acknowledgeCallable()).thenReturn(ackCallable)

    val subscriber = makeStubSubscriber(logger, stubMock)
    subscriber.acknowledge(List.empty).unsafeRunSync()

    verify(stubMock, never()).acknowledgeCallable()
  }

  it should "complete without error for a single ack ID" in {
    val logger      = new StubPipelineLogger
    val stubMock    = mock[SubscriberStub]
    val ackCallable = mock[UnaryCallable[AcknowledgeRequest, com.google.protobuf.Empty]]

    when(stubMock.acknowledgeCallable()).thenReturn(ackCallable)
    when(ackCallable.call(any[AcknowledgeRequest]())).thenReturn(com.google.protobuf.Empty.getDefaultInstance)

    val subscriber = makeStubSubscriber(logger, stubMock)

    // Should not throw
    noException should be thrownBy {
      subscriber.acknowledge(List("single-ack")).unsafeRunSync()
    }
  }

  // --- PubSubSubscriberResource tests ---

  "PubSubSubscriberResource.make" should "create a PubSubEventSubscriber from a stub factory" in {
    val logger   = new StubPipelineLogger
    val stubMock = mock[SubscriberStub]
    val config   = EventSubscriberConfig("test-project", "test-subscription", 10)

    val pullCallable  = mock[UnaryCallable[PullRequest, PullResponse]]
    val emptyResponse = PullResponse.newBuilder().build()
    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(emptyResponse)

    val subscriber = PubSubSubscriberResource
      .make[IO](config, logger, () => stubMock)
      .use(sub => sub.pull(5).map(result => result shouldBe empty))
      .unsafeRunSync()

    val _ = subscriber
    verify(stubMock).pullCallable()
  }

  it should "close the stub on resource release" in {
    val logger   = new StubPipelineLogger
    val stubMock = mock[SubscriberStub]
    val config   = EventSubscriberConfig("test-project", "test-subscription", 10)

    val _ = PubSubSubscriberResource
      .make[IO](config, logger, () => stubMock)
      .use(_ => IO.unit)
      .unsafeRunSync()

    verify(stubMock).close()
  }

  it should "build correct subscription name from config" in {
    val logger   = new StubPipelineLogger
    val stubMock = mock[SubscriberStub]
    val config   = EventSubscriberConfig("my-gcp-project", "my-sub-id", 25)

    val pullCallable  = mock[UnaryCallable[PullRequest, PullResponse]]
    val emptyResponse = PullResponse.newBuilder().build()
    when(stubMock.pullCallable()).thenReturn(pullCallable)
    when(pullCallable.call(any[PullRequest]())).thenReturn(emptyResponse)

    val _ = PubSubSubscriberResource
      .make[IO](config, logger, () => stubMock)
      .use(sub => sub.pull(1))
      .unsafeRunSync()

    // The PullRequest should contain the correctly formatted subscription name
    val captor = org.mockito.ArgumentCaptor.forClass(classOf[PullRequest])
    verify(pullCallable).call(captor.capture())
    val capturedRequest = captor.getValue
    capturedRequest.getSubscription shouldBe "projects/my-gcp-project/subscriptions/my-sub-id"
  }

}
