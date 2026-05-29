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
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.config.GovInfoConfig

class GovInfoCdirClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(WireMockConfiguration.options().bindAddress("127.0.0.1").dynamicPort())

  private lazy val (httpClient, httpShutdown) =
    EmberClientBuilder.default[IO].withTimeout(5.seconds).build.allocated.unsafeRunSync()

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private def config = GovInfoConfig(baseUrl = s"http://localhost:${wireMock.port().toString}", apiKey = "k")

  private def client = new GovInfoCdirClient[IO](httpClient, config, noopLogger)

  override def beforeAll(): Unit = { super.beforeAll(); wireMock.start() }

  override def afterAll(): Unit = {
    wireMock.stop()
    try httpShutdown.unsafeRunSync()
    catch { case _: Exception => () }
    super.afterAll()
  }

  override def afterEach(): Unit = { wireMock.resetAll(); super.afterEach() }

  private def stub(path: String, body: String): Unit = {
    val _ = wireMock.stubFor(
      get(urlPathEqualTo(path)).willReturn(aResponse().withStatus(200).withBody(body))
    )
  }

  "committeeListingTexts" should "select the congress's package and return its committee-listing texts" in {
    stub(
      "/collections/CDIR/2000-01-01T00:00:00Z",
      """{"count":2,"packages":[
        |{"packageId":"CDIR-2019-09-30","dateIssued":"2019-09-30"},
        |{"packageId":"CDIR-2022-10-26","dateIssued":"2022-10-26"}]}""".stripMargin,
    )
    stub(
      "/packages/CDIR-2022-10-26/granules",
      """{"granules":[
        |{"granuleId":"CDIR-2022-10-26-HOUSECOMMITTEES"},
        |{"granuleId":"CDIR-2022-10-26-FRONTMATTER"},
        |{"granuleId":"CDIR-2022-10-26-SENATECOMMITTEES"}]}""".stripMargin,
    )
    val txtUrl = s"http://localhost:${wireMock.port().toString}/content"
    stub(
      "/packages/CDIR-2022-10-26/granules/CDIR-2022-10-26-HOUSECOMMITTEES/summary",
      s"""{"download":{"txtLink":"$txtUrl/house"}}""",
    )
    stub(
      "/packages/CDIR-2022-10-26/granules/CDIR-2022-10-26-SENATECOMMITTEES/summary",
      s"""{"download":{"txtLink":"$txtUrl/senate"}}""",
    )
    stub("/content/house", "<pre>STANDING COMMITTEES OF THE HOUSE</pre>")
    stub("/content/senate", "STANDING COMMITTEES OF THE SENATE")

    val texts = client.committeeListingTexts(117, 1L).unsafeRunSync()

    val _ = texts.size shouldBe 2
    // HTML tags stripped
    val _ = texts.exists(_.contains("STANDING COMMITTEES OF THE HOUSE")) shouldBe true
    texts.exists(_.contains("<pre>")) shouldBe false
  }

  it should "return empty when no package falls in the congress's years" in {
    stub(
      "/collections/CDIR/2000-01-01T00:00:00Z",
      """{"count":1,"packages":[{"packageId":"CDIR-1999-01-01","dateIssued":"1999-01-01"}]}""",
    )
    client.committeeListingTexts(117, 1L).unsafeRunSync() shouldBe empty
  }

}
