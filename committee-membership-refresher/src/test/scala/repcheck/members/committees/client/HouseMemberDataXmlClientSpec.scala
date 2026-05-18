package repcheck.members.committees.client

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.pipeline.models.errors.RetryConfig

class HouseMemberDataXmlClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration.options().bindAddress("127.0.0.1").dynamicPort()
  )

  implicit private val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private lazy val (httpClient, httpShutdown) = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build
    .allocated
    .unsafeRunSync()

  private val retryConfig: RetryConfig = RetryConfig(
    maxRetries = 1,
    initialBackoffMs = 10L,
    maxBackoffMs = 50L,
    backoffMultiplier = 2.0,
  )

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private def baseConfig: CommitteeMembershipConfig =
    CommitteeMembershipConfig(
      parallelism = 1,
      requestTimeout = 5.seconds,
      currentCongress = 119,
      pageSize = 250,
      houseMemberDataUrl = s"http://localhost:${wireMock.port().toString}/MemberData.xml",
      senateIdentityUrl = "http://localhost:8080/identity.xml",
      senateCommitteeBaseUrl = "http://localhost:8080/senate",
    )

  private def makeClient(config: CommitteeMembershipConfig): HouseMemberDataXmlClient[IO] = {
    val xmlFeed = XmlFeedClient.make[IO](httpClient, retryConfig)
    new HouseMemberDataXmlClient[IO](xmlFeed, config, noopLogger)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
  }

  override def afterAll(): Unit = {
    wireMock.stop()
    try httpShutdown.unsafeRunSync()
    catch { case _: Exception => () }
    super.afterAll()
  }

  override def afterEach(): Unit = {
    wireMock.resetAll()
    super.afterEach()
  }

  private def stubXml(path: String, body: String, status: Int = 200): Unit = {
    val _ = wireMock.stubFor(
      get(urlEqualTo(path))
        .willReturn(
          aResponse().withStatus(status).withHeader("Content-Type", "application/xml").withBody(body)
        )
    )
  }

  private def houseMember(
    bioguideId: String,
    firstName: String = "John",
    lastName: String = "Doe",
    party: String = "R",
    state: String = "Texas",
    district: String = "1",
    committees: String = "",
  ): String =
    s"""<member>
       |  <member-info>
       |    <bioguideID>$bioguideId</bioguideID>
       |    <firstname>$firstName</firstname>
       |    <lastname>$lastName</lastname>
       |    <party>$party</party>
       |    <state><state-fullname>$state</state-fullname></state>
       |    <district>$district</district>
       |  </member-info>
       |  <committee-assignments>$committees</committee-assignments>
       |</member>""".stripMargin

  private def document(members: Seq[String]): String =
    s"<MemberData>${members.mkString}</MemberData>"

  "fetchMembers" should "emit one DTO per parsed House member" in {
    val body = document(
      Seq(
        houseMember("H001", committees = """<committee comcode="RU00" name="Rules" rank="1" side="majority"/>"""),
        houseMember("H002", firstName = "Jane", lastName = "Smith"),
      )
    )
    stubXml("/MemberData.xml", body)

    val result = makeClient(baseConfig).fetchMembers(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 2
    result.map(_.bioguideId) should contain theSameElementsAs List("H001", "H002")
  }

  it should "parse committee assignments with code normalization" in {
    val body = document(
      Seq(
        houseMember(
          "H003",
          committees = """<committee comcode="ru00" name="Rules" rank="2" side="minority"/>
          |<committee comcode="AP00" name="Appropriations" rank="5" side="majority"/>""".stripMargin,
        )
      )
    )
    stubXml("/MemberData.xml", body)

    val result = makeClient(baseConfig).fetchMembers(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    val member = result.headOption
    val _      = member.isDefined shouldBe true
    member.foreach { m =>
      val _     = m.committees.size shouldBe 2
      val codes = m.committees.map(_.committeeCode)
      val _     = codes should contain("RU00")
      codes should contain("AP00")
    }
  }

  it should "skip members without bioguideID" in {
    val noBioguide =
      """<member>
        |  <member-info>
        |    <firstname>Ghost</firstname>
        |    <lastname>Member</lastname>
        |    <party>D</party>
        |    <state><state-fullname>Ohio</state-fullname></state>
        |  </member-info>
        |  <committee-assignments/>
        |</member>""".stripMargin
    val body = document(Seq(houseMember("H004"), noBioguide))
    stubXml("/MemberData.xml", body)

    val result = makeClient(baseConfig).fetchMembers(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    result.headOption.map(_.bioguideId) shouldBe Some("H004")
  }

  it should "handle empty document" in {
    stubXml("/MemberData.xml", "<MemberData></MemberData>")

    val result = makeClient(baseConfig).fetchMembers(1L).compile.toList.unsafeRunSync()
    result shouldBe empty
  }

  it should "parse leadership attributes and subcommittee assignments" in {
    val committees =
      """<committee comcode="WM00" name="Ways and Means" rank="1" side="majority"/>
        |<subcommittee subcomcode="WM01" subcomname="Tax Policy" rank="3" side="majority"/>""".stripMargin
    val body = document(Seq(houseMember("H005", committees = committees)))
    stubXml("/MemberData.xml", body)

    val result = makeClient(baseConfig).fetchMembers(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    result.headOption.foreach { m =>
      val _ = m.committees.size shouldBe 2
      m.committees.map(_.committeeCode) should contain theSameElementsAs List("WM00", "WM01")
    }
  }

}
