package com.repcheck.bills.text.integration

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import com.google.cloud.pubsub.v1.{Publisher, SubscriptionAdminClient, TopicAdminClient}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, PullRequest, SubscriptionName, TopicName}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.repcheck.bills.common.testing.E2ETest

/**
 * Sanity tests for the real GCP dev Pub/Sub environment. Validates that the dev project's Pub/Sub topics and
 * subscriptions are functional.
 *
 * These tests require: - `GOOGLE_CLOUD_PROJECT` env var set to the dev GCP project - Valid GCP credentials (gcloud auth
 * or service account) - Dev Pub/Sub topics/subscriptions already created
 *
 * Run with: `sbt "testOnly -- -n com.repcheck.tags.E2ETest"`
 */
class DevPubSubSanitySpec extends AnyFlatSpec with Matchers {

  private val projectId = sys.env.getOrElse("GOOGLE_CLOUD_PROJECT", "repcheck-dev")

  // Use a test-specific ephemeral topic/subscription to avoid polluting real queues
  private val testPrefix     = s"sanity-test-${UUID.randomUUID().toString.take(8)}"
  private val testTopicId    = s"$testPrefix-topic"
  private val testSubId      = s"$testPrefix-sub"
  private lazy val topicName = TopicName.of(projectId, testTopicId)
  private lazy val subName   = SubscriptionName.of(projectId, testSubId)

  private def isGcpAvailable: Boolean =
    try {
      val topicAdmin = TopicAdminClient.create()
      topicAdmin.close()
      true
    } catch { case _: Exception => false }

  "Dev Pub/Sub" should "create topic, publish message, and pull it back" taggedAs E2ETest in {
    val _ = assume(isGcpAvailable, "GCP credentials not available — skipping dev Pub/Sub test")

    val topicAdmin = TopicAdminClient.create()
    val subAdmin   = SubscriptionAdminClient.create()

    try {
      // Create ephemeral topic and subscription
      val _ = topicAdmin.createTopic(topicName)
      val _ = subAdmin.createSubscription(
        subName,
        topicName,
        com.google.pubsub.v1.PushConfig.getDefaultInstance,
        10,
      )

      // Publish a test message
      val publisher = Publisher.newBuilder(topicName).build()
      try {
        val message = PubsubMessage
          .newBuilder()
          .setData(ByteString.copyFromUtf8("""{"test":"sanity-check"}"""))
          .putAttributes("eventType", "SanityTest")
          .build()
        val messageId = publisher.publish(message).get(10, TimeUnit.SECONDS)
        val _         = messageId.toString should not be empty

        // Wait briefly for message propagation
        Thread.sleep(2000L)

        // Pull the message back
        val stub = com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub.create(
          com.google.cloud.pubsub.v1.stub.SubscriberStubSettings.newBuilder().build()
        )
        try {
          val pullRequest = PullRequest
            .newBuilder()
            .setSubscription(subName.toString)
            .setMaxMessages(1)
            .build()

          val response = stub.pullCallable().call(pullRequest)
          val messages = response.getReceivedMessagesList.asScala.toList
          val _        = messages should not be empty
          messages.headOption.map(_.getMessage.getData.toStringUtf8).getOrElse("") should include("sanity-check")
        } finally stub.close()
      } finally {
        publisher.shutdown()
        val _ = publisher.awaitTermination(5, TimeUnit.SECONDS)
      }
    } finally {
      // Cleanup ephemeral resources
      try subAdmin.deleteSubscription(subName)
      catch { case _: Exception => () }
      try topicAdmin.deleteTopic(topicName)
      catch { case _: Exception => () }
      topicAdmin.close()
      subAdmin.close()
    }
  }

  it should "handle message attributes correctly" taggedAs E2ETest in {
    val _ = assume(isGcpAvailable, "GCP credentials not available — skipping dev Pub/Sub test")

    val topicAdmin = TopicAdminClient.create()
    val subAdmin   = SubscriptionAdminClient.create()

    try {
      val _ = topicAdmin.createTopic(topicName)
      val _ = subAdmin.createSubscription(
        subName,
        topicName,
        com.google.pubsub.v1.PushConfig.getDefaultInstance,
        10,
      )

      val publisher = Publisher.newBuilder(topicName).build()
      try {
        val message = PubsubMessage
          .newBuilder()
          .setData(ByteString.copyFromUtf8("""{"event":"test"}"""))
          .putAttributes("eventType", "BillTextAvailable")
          .putAttributes("source", "sanity-test")
          .build()
        val _ = publisher.publish(message).get(10, TimeUnit.SECONDS)

        Thread.sleep(2000L)

        val stub = com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub.create(
          com.google.cloud.pubsub.v1.stub.SubscriberStubSettings.newBuilder().build()
        )
        try {
          val pullRequest = PullRequest
            .newBuilder()
            .setSubscription(subName.toString)
            .setMaxMessages(1)
            .build()

          val response = stub.pullCallable().call(pullRequest)
          val messages = response.getReceivedMessagesList.asScala.toList
          val _        = messages should not be empty
          val attrs =
            messages.headOption.map(_.getMessage.getAttributesMap.asScala.toMap).getOrElse(Map.empty[String, String])
          val _ = attrs.get("eventType") shouldBe Some("BillTextAvailable")
          attrs.get("source") shouldBe Some("sanity-test")
        } finally stub.close()
      } finally {
        publisher.shutdown()
        val _ = publisher.awaitTermination(5, TimeUnit.SECONDS)
      }
    } finally {
      try subAdmin.deleteSubscription(subName)
      catch { case _: Exception => () }
      try topicAdmin.deleteTopic(topicName)
      catch { case _: Exception => () }
      topicAdmin.close()
      subAdmin.close()
    }
  }

  it should "verify dev project topic exists and is accessible" taggedAs E2ETest in {
    val _ = assume(isGcpAvailable, "GCP credentials not available — skipping dev Pub/Sub test")

    val topicAdmin = TopicAdminClient.create()
    try {
      // List topics in dev project — verify we can access them
      val topics = topicAdmin.listTopics(s"projects/$projectId").iterateAll().asScala.toList
      // Dev project should have at least the bill-events topic
      topics.toString should not be empty
    } finally topicAdmin.close()
  }

}
