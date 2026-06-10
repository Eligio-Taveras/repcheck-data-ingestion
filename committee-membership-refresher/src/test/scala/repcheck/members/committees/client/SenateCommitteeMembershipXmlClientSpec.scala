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

import com.repcheck.utils.errors.RetryConfig

class SenateCommitteeMembershipXmlClientSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

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
      houseMemberDataUrl = "http://localhost:8080/house.xml",
      senateIdentityUrl = "http://localhost:8080/identity.xml",
      senateCommitteeBaseUrl = s"http://localhost:${wireMock.port().toString}/senate",
    )

  private def makeClient(config: CommitteeMembershipConfig): SenateCommitteeMembershipXmlClient[IO] = {
    val xmlFeed = XmlFeedClient.make[IO](httpClient, retryConfig)
    new SenateCommitteeMembershipXmlClient[IO](xmlFeed, config, noopLogger)
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

  // Real senate.gov format: <committee_membership><committees> root, names nested under <name>,
  // subcommittee code in a <committee_code> child element, no bioguide_id or rank fields.
  private val sampleCommitteeXml: String =
    """<committee_membership>
      |  <committees>
      |    <majority_party>R</majority_party>
      |    <committee_name>Committee on Finance</committee_name>
      |    <committee_code>SSFI00</committee_code>
      |    <members>
      |      <member>
      |        <name><first>Ron</first><last>Wyden</last></name>
      |        <state>OR</state>
      |        <party>D</party>
      |        <position>Chairman</position>
      |      </member>
      |      <member>
      |        <name><first>Mike</first><last>Crapo</last></name>
      |        <state>ID</state>
      |        <party>R</party>
      |        <position>Ranking</position>
      |      </member>
      |    </members>
      |    <subcommittee>
      |      <subcommittee_name>Subcommittee on Health Care</subcommittee_name>
      |      <committee_code>SSFI01</committee_code>
      |      <members>
      |        <member>
      |          <name><first>Debbie</first><last>Stabenow</last></name>
      |          <state>MI</state>
      |          <party>D</party>
      |          <position>Chairman</position>
      |        </member>
      |      </members>
      |    </subcommittee>
      |  </committees>
      |</committee_membership>""".stripMargin

  // Backward-compat fixture: flat structure with bioguide_id and rank, proving the parser's
  // fallback field extraction still works if senate.gov ever includes them.
  private val sampleWithBioguideAndRank: String =
    """<committee_membership>
      |  <committees>
      |    <committee_code>SSFI00</committee_code>
      |    <members>
      |      <member>
      |        <name><first>Ron</first><last>Wyden</last></name>
      |        <state>OR</state>
      |        <party>D</party>
      |        <position>Chairman</position>
      |        <rank>1</rank>
      |        <bioguide_id>B001</bioguide_id>
      |      </member>
      |    </members>
      |  </committees>
      |</committee_membership>""".stripMargin

  "fetchCommitteeMembers" should "parse parent committee members and subcommittee members" in {
    stubXml("/senate/committee_memberships_SSFI.xml", sampleCommitteeXml)

    val result = makeClient(baseConfig).fetchCommitteeMembers("SSFI00", 1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 3

    val parentMembers = result.filter(!_.isSubcommittee)
    val _             = parentMembers.size shouldBe 2

    val subMembers = result.filter(_.isSubcommittee)
    val _          = subMembers.size shouldBe 1
    subMembers.headOption.foreach { m =>
      val _ = m.committeeCode shouldBe "SSFI01"
      m.firstName shouldBe "Debbie"
    }
  }

  it should "extract bioguide_id and rank when present" in {
    stubXml("/senate/committee_memberships_SSFI.xml", sampleWithBioguideAndRank)

    val result = makeClient(baseConfig).fetchCommitteeMembers("SSFI00", 1L).compile.toList.unsafeRunSync()
    val member = result.headOption
    val _      = member.flatMap(_.bioguideId) shouldBe Some("B001")
    member.flatMap(_.rank) shouldBe Some(1)
  }

  it should "handle soft-404 HTML response gracefully" in {
    val htmlResponse = "<html><body>Page Not Found</body></html>"
    stubXml("/senate/committee_memberships_SSXX.xml", htmlResponse)

    val result = makeClient(baseConfig).fetchCommitteeMembers("SSXX00", 1L).compile.toList.unsafeRunSync()
    result shouldBe empty
  }

  it should "emit empty stream for empty committee roster" in {
    val emptyCommittee =
      """<committee_membership><committees><committee_code>SSFI00</committee_code><members/></committees></committee_membership>"""
    stubXml("/senate/committee_memberships_SSFI.xml", emptyCommittee)

    val result = makeClient(baseConfig).fetchCommitteeMembers("SSFI00", 1L).compile.toList.unsafeRunSync()
    result shouldBe empty
  }

  it should "parse position field" in {
    stubXml("/senate/committee_memberships_SSFI.xml", sampleCommitteeXml)

    val result   = makeClient(baseConfig).fetchCommitteeMembers("SSFI00", 1L).compile.toList.unsafeRunSync()
    val chairman = result.find(_.firstName == "Ron")
    chairman.foreach(m => m.position shouldBe Some("Chairman"))
  }

}
