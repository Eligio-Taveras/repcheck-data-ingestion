package com.repcheck.bills.text.integration

import java.sql.DriverManager

import scala.jdk.CollectionConverters._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.implicits._

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1.{PullRequest, SubscriptionName}

import io.grpc.ManagedChannelBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.repcheck.bills.common.testing.E2ETest

/**
 * Docker Compose E2E verification tests. Assumes `docker-compose -f docker-compose.e2e.yml up` is running with all
 * infrastructure services: AlloyDB Omni (port 5432), Pub/Sub emulator (port 8085), WireMock (port 8080).
 *
 * Run these tests with: `sbt "testOnly -- -n com.repcheck.tags.E2ETest"`
 *
 * These tests verify the full containerized pipeline flow: 1. Seed bills into the DB 2. Run the metadata pipeline
 * container (reads from WireMock) 3. Run the checker container (finds new text, publishes events) 4. Run the text
 * pipeline container (downloads text, stores in DB)
 *
 * The tests verify the end state in the database after all containers have run.
 */
class DockerE2ESpec extends AnyFlatSpec with Matchers {

  // Docker Compose infrastructure endpoints
  private val dbUrl      = sys.env.getOrElse("E2E_DB_URL", "jdbc:postgresql://localhost:5432/repcheck_bills_test")
  private val dbUser     = sys.env.getOrElse("E2E_DB_USER", "test")
  private val dbPassword = sys.env.getOrElse("E2E_DB_PASSWORD", "test")
  private val pubsubHost = sys.env.getOrElse("PUBSUB_EMULATOR_HOST", "localhost:8085")

  private lazy val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = dbUrl,
    user = dbUser,
    password = dbPassword,
    logHandler = None,
  )

  private def isInfraAvailable: Boolean = {
    val dbAvailable =
      try {
        val conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)
        conn.close()
        true
      } catch { case _: Exception => false }

    val pubsubAvailable =
      try {
        val channel = ManagedChannelBuilder.forTarget(pubsubHost).usePlaintext().build()
        channel.shutdown()
        true
      } catch { case _: Exception => false }

    dbAvailable && pubsubAvailable
  }

  "Docker E2E" should "have bill records in database after metadata pipeline runs" taggedAs E2ETest in {
    val _ = assume(isInfraAvailable, "Docker Compose infrastructure is not running")

    val billCount = sql"SELECT COUNT(*) FROM bills"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    // After metadata pipeline processes WireMock bill list, at least one bill should exist
    billCount should be > 0L
  }

  it should "have bill text versions stored after text pipeline runs" taggedAs E2ETest in {
    val _ = assume(isInfraAvailable, "Docker Compose infrastructure is not running")

    val versionCount = sql"SELECT COUNT(*) FROM bill_text_versions WHERE content IS NOT NULL"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    // After text pipeline downloads and stores content, at least one version should have content
    versionCount should be > 0L
  }

  it should "have events on the Pub/Sub topic after pipeline processing" taggedAs E2ETest in {
    val _ = assume(isInfraAvailable, "Docker Compose infrastructure is not running")

    val projectId      = "repcheck-test"
    val subscriptionId = "e2e-verification-sub"

    val channel = ManagedChannelBuilder
      .forTarget(pubsubHost)
      .usePlaintext()
      .build()

    val channelProvider     = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
    val credentialsProvider = NoCredentialsProvider.create()

    val stubSettings = SubscriberStubSettings
      .newBuilder()
      .setTransportChannelProvider(channelProvider)
      .setCredentialsProvider(credentialsProvider)
      .build()

    try {
      val stub = GrpcSubscriberStub.create(stubSettings)
      try {
        val pullRequest = PullRequest
          .newBuilder()
          .setSubscription(SubscriptionName.of(projectId, subscriptionId).toString)
          .setMaxMessages(10)
          .build()

        val response = stub.pullCallable().call(pullRequest)
        val messages = response.getReceivedMessagesList.asScala.toList

        // After full pipeline run, there should be events published
        messages should not be empty
      } finally stub.close()
    } finally {
      val _ = channel.shutdown()
    }
  }

}
