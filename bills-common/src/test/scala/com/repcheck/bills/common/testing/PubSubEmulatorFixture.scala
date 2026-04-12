package com.repcheck.bills.common.testing

import java.util.UUID

import scala.jdk.CollectionConverters._

import com.google.api.gax.core.{CredentialsProvider, NoCredentialsProvider}
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannelProvider}
import com.google.cloud.pubsub.v1.{
  Publisher,
  SubscriptionAdminClient,
  SubscriptionAdminSettings,
  TopicAdminClient,
  TopicAdminSettings,
}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, PullRequest, SubscriptionName, TopicName}

import io.grpc.ManagedChannelBuilder
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Provides Pub/Sub emulator infrastructure for integration tests. Creates an ephemeral topic and subscription per test
 * suite, with automatic cleanup in `afterAll()`.
 *
 * Requires the Pub/Sub emulator to be running. Set `PUBSUB_EMULATOR_HOST` (e.g., `localhost:8085`).
 */
trait PubSubEmulatorFixture extends BeforeAndAfterAll { self: Suite =>

  protected val emulatorHost: String =
    sys.env.getOrElse("PUBSUB_EMULATOR_HOST", "localhost:8085")

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
    publisher.publish(message).get()
  }

  /** Pulls messages from the test subscription. */
  protected def pullMessages(maxMessages: Int = 10): List[PubsubMessage] = {
    import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}

    val stubSettings = SubscriberStubSettings
      .newBuilder()
      .setTransportChannelProvider(channelProvider)
      .setCredentialsProvider(credentialsProvider)
      .build()

    val stub = GrpcSubscriberStub.create(stubSettings)
    try {
      val pullRequest = PullRequest
        .newBuilder()
        .setSubscription(subscriptionName.toString)
        .setMaxMessages(maxMessages)
        .build()

      val response = stub.pullCallable().call(pullRequest)
      val messages = response.getReceivedMessagesList.asScala.toList

      // Auto-ack all pulled messages
      if (messages.nonEmpty) {
        val ackIds = messages.map(_.getAckId).asJava
        import com.google.pubsub.v1.AcknowledgeRequest
        val ackRequest = AcknowledgeRequest
          .newBuilder()
          .setSubscription(subscriptionName.toString)
          .addAllAckIds(ackIds)
          .build()
        val _ = stub.acknowledgeCallable().call(ackRequest)
      }

      messages.map(_.getMessage)
    } finally stub.close()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val _ = topicAdminClient.createTopic(topicName)
    val _ = subscriptionAdminClient.createSubscription(
      subscriptionName,
      topicName,
      com.google.pubsub.v1.PushConfig.getDefaultInstance,
      10, // ack deadline seconds
    )
  }

  override protected def afterAll(): Unit = {
    try
      subscriptionAdminClient.deleteSubscription(subscriptionName)
    catch { case _: Exception => () }
    try
      topicAdminClient.deleteTopic(topicName)
    catch { case _: Exception => () }
    try publisher.shutdown()
    catch { case _: Exception => () }
    try topicAdminClient.close()
    catch { case _: Exception => () }
    try subscriptionAdminClient.close()
    catch { case _: Exception => () }
    try channel.shutdown()
    catch { case _: Exception => () }
    super.afterAll()
  }

}
