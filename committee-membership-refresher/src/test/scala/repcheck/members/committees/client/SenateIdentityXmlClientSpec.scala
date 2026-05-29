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

class SenateIdentityXmlClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

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
      senateIdentityUrl = s"http://localhost:${wireMock.port().toString}/cvc_member_data.xml",
      senateCommitteeBaseUrl = "http://localhost:8080/senate",
    )

  private def makeClient(config: CommitteeMembershipConfig): SenateIdentityXmlClient[IO] = {
    val xmlFeed = XmlFeedClient.make[IO](httpClient, retryConfig)
    new SenateIdentityXmlClient[IO](xmlFeed, config, noopLogger)
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

  private def senator(
    bioguide: String,
    lis: String,
    firstName: String = "Jane",
    lastName: String = "Doe",
    party: String = "D",
    state: String = "NY",
  ): String =
    s"""<senator>
       |  <bioguide_id>$bioguide</bioguide_id>
       |  <lis_member_id>$lis</lis_member_id>
       |  <first_name>$firstName</first_name>
       |  <last_name>$lastName</last_name>
       |  <party>$party</party>
       |  <state>$state</state>
       |</senator>""".stripMargin

  private def document(senators: Seq[String]): String =
    s"<senators>${senators.mkString}</senators>"

  "fetchIdentities" should "emit one DTO per parsed senator" in {
    val body = document(
      Seq(
        senator("B001", "S100"),
        senator("B002", "S200", firstName = "Alex", lastName = "Kim"),
      )
    )
    stubXml("/cvc_member_data.xml", body)

    val result = makeClient(baseConfig).fetchIdentities(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 2
    result.map(_.bioguideId) should contain theSameElementsAs List("B001", "B002")
  }

  it should "skip senators missing bioguide_id" in {
    val noBioguide =
      """<senator>
        |  <lis_member_id>S300</lis_member_id>
        |  <first_name>Missing</first_name>
        |  <last_name>Bio</last_name>
        |  <party>R</party>
        |  <state>TX</state>
        |</senator>""".stripMargin
    val body = document(Seq(senator("B003", "S301"), noBioguide))
    stubXml("/cvc_member_data.xml", body)

    val result = makeClient(baseConfig).fetchIdentities(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    result.headOption.map(_.bioguideId) shouldBe Some("B003")
  }

  it should "handle empty document" in {
    stubXml("/cvc_member_data.xml", "<senators></senators>")
    val result = makeClient(baseConfig).fetchIdentities(1L).compile.toList.unsafeRunSync()
    result shouldBe empty
  }

  it should "parse all fields correctly" in {
    val body = document(Seq(senator("B004", "S400", "Tammy", "Baldwin", "D", "WI")))
    stubXml("/cvc_member_data.xml", body)

    val result = makeClient(baseConfig).fetchIdentities(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    result.headOption.foreach { s =>
      val _ = s.bioguideId shouldBe "B004"
      val _ = s.lisMemberId shouldBe "S400"
      val _ = s.firstName shouldBe "Tammy"
      val _ = s.lastName shouldBe "Baldwin"
      val _ = s.party shouldBe "D"
      s.state shouldBe "WI"
    }
  }

  // Real senate.gov format: lis_member_id is an attribute, names nested under <name>,
  // committee assignments embedded as <committees><committee code="...">.
  it should "parse real format with nested names, attribute lis id, and committee codes" in {
    val realSenator =
      """<senator lis_member_id="S428">
        |  <name><first>Angela D.</first><last>Alsobrooks</last></name>
        |  <party>D</party>
        |  <state>MD</state>
        |  <bioguideId>A000382</bioguideId>
        |  <committees>
        |    <committee code="SSEV00">Environment and Public Works</committee>
        |    <committee code="SSBK00">Banking</committee>
        |  </committees>
        |</senator>""".stripMargin
    stubXml("/cvc_member_data.xml", document(Seq(realSenator)))

    val result = makeClient(baseConfig).fetchIdentities(1L).compile.toList.unsafeRunSync()
    val _      = result.size shouldBe 1
    result.headOption.foreach { s =>
      val _ = s.bioguideId shouldBe "A000382"
      val _ = s.lisMemberId shouldBe "S428"
      val _ = s.firstName shouldBe "Angela D."
      val _ = s.lastName shouldBe "Alsobrooks"
      s.committeeCodes should contain theSameElementsAs List("SSEV00", "SSBK00")
    }
  }

}
