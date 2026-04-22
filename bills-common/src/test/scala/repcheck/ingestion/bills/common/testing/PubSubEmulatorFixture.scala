package repcheck.ingestion.bills.common.testing

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.google.api.gax.core.{CredentialsProvider, NoCredentialsProvider}
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannelProvider}
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.cloud.pubsub.v1.{
  Publisher,
  SubscriptionAdminClient,
  SubscriptionAdminSettings,
  TopicAdminClient,
  TopicAdminSettings,
}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{AcknowledgeRequest, PubsubMessage, PullRequest, SubscriptionName, TopicName}

import io.grpc.ManagedChannelBuilder
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Provides Pub/Sub emulator infrastructure for integration tests. Automatically starts a Pub/Sub emulator Docker
 * container (shared across all suites via [[SharedDockerPubSub]]) and creates an ephemeral topic and subscription per
 * test suite, with automatic cleanup in `afterAll()`.
 */
trait PubSubEmulatorFixture extends BeforeAndAfterAll { self: Suite =>

  protected lazy val emulatorInfo: PubSubEmulatorInfo = SharedDockerPubSub.info
  protected lazy val emulatorHost: String             = emulatorInfo.endpoint

  protected val emulatorProjectId: String = "repcheck-test"

  private val testPrefix: String = s"test-${UUID.randomUUID().toString.take(8)}"

  protected val topicId: String        = s"$testPrefix-topic"
  protected val subscriptionId: String = s"$testPrefix-sub"

  protected lazy val topicName: TopicName               = TopicName.of(emulatorProjectId, topicId)
  protected lazy val subscriptionName: SubscriptionName = SubscriptionName.of(emulatorProjectId, subscriptionId)

  private lazy val channel = ManagedChannelBuilder
    .forTarget(emulatorHost)
    .usePlaintext()
    .build()

  private lazy val channelProvider: TransportChannelProvider =
    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))

  private lazy val credentialsProvider: CredentialsProvider =
    NoCredentialsProvider.create()

  private lazy val topicAdminClient: TopicAdminClient =
    TopicAdminClient.create(
      TopicAdminSettings
        .newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    )

  private lazy val subscriptionAdminClient: SubscriptionAdminClient =
    SubscriptionAdminClient.create(
      SubscriptionAdminSettings
        .newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    )

  // Shared subscriber stub — reused across pullMessages calls to avoid connection overhead
  private lazy val subscriberStub: GrpcSubscriberStub =
    GrpcSubscriberStub.create(
      SubscriberStubSettings
        .newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()
    )

  protected lazy val publisher: Publisher =
    Publisher
      .newBuilder(topicName)
      .setChannelProvider(channelProvider)
      .setCredentialsProvider(credentialsProvider)
      .build()

  /** Publishes a message to the test topic and returns the message ID. */
  protected def publishMessage(data: String, attributes: Map[String, String] = Map.empty): String = {
    val message = PubsubMessage
      .newBuilder()
      .setData(ByteString.copyFromUtf8(data))
      .putAllAttributes(attributes.asJava)
      .build()
    publisher.publish(message).get(10, TimeUnit.SECONDS)
  }

  /**
   * Drains all pending messages from the subscription. Call in beforeEach to isolate tests. Silently ignores errors so
   * it never breaks test setup.
   */
  protected def drainMessages(): Unit = {
    val _ = Try(pullMessages(100))
  }

  /**
   * Pulls messages from the test subscription. Uses a short RPC deadline (1 second) so negative- path assertions
   * (`pullMessages() shouldBe empty`) don't long-poll against gRPC's default 60- second pull deadline — that single
   * default was costing ~5 minutes of aggregate CI time across 5 integration tests.
   *
   * Positive-path assertions are unaffected: `publishMessage` blocks on `publisher.publish(...).get(10, SECONDS)` so by
   * the time `pullMessages()` runs the message is already in the subscription and returns immediately.
   *
   * The non-deprecated alternative to `PullRequest.setReturnImmediately(true)` (GCP now recommends StreamingPull for
   * production code) is a per-call deadline via `GrpcCallContext`; we catch `DeadlineExceededException` and return an
   * empty list since that's the "no messages arrived" signal we want anyway.
   */
  protected def pullMessages(maxMessages: Int = 10): List[PubsubMessage] = {
    val pullRequest = PullRequest
      .newBuilder()
      .setSubscription(subscriptionName.toString)
      .setMaxMessages(maxMessages)
      .build()

    val callContext = com.google.api.gax.grpc.GrpcCallContext
      .createDefault()
      .withTimeout(org.threeten.bp.Duration.ofSeconds(1))

    val messages: List[com.google.pubsub.v1.ReceivedMessage] =
      try {
        val response = subscriberStub.pullCallable().call(pullRequest, callContext)
        response.getReceivedMessagesList.asScala.toList
      } catch {
        case _: com.google.api.gax.rpc.DeadlineExceededException => List.empty
      }

    // Auto-ack all pulled messages
    if (messages.nonEmpty) {
      val ackIds = messages.map(_.getAckId).asJava
      val ackRequest = AcknowledgeRequest
        .newBuilder()
        .setSubscription(subscriptionName.toString)
        .addAllAckIds(ackIds)
        .build()
      val _ = subscriberStub.acknowledgeCallable().call(ackRequest)
    }

    messages.map(_.getMessage)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Force emulator container to start before creating topic/subscription
    val _ = emulatorHost
    val _ = topicAdminClient.createTopic(topicName)
    val _ = subscriptionAdminClient.createSubscription(
      subscriptionName,
      topicName,
      com.google.pubsub.v1.PushConfig.getDefaultInstance,
      10, // ack deadline seconds
    )
  }

  override def afterAll(): Unit = {
    try
      subscriptionAdminClient.deleteSubscription(subscriptionName)
    catch { case _: Exception => () }
    try
      topicAdminClient.deleteTopic(topicName)
    catch { case _: Exception => () }
    try publisher.shutdown()
    catch { case _: Exception => () }
    try subscriberStub.close()
    catch { case _: Exception => () }
    try topicAdminClient.close()
    catch { case _: Exception => () }
    try subscriptionAdminClient.close()
    catch { case _: Exception => () }
    try {
      channel.shutdownNow()
      val _ = channel.awaitTermination(5, TimeUnit.SECONDS)
    } catch { case _: Exception => () }
    super.afterAll()
  }

}
