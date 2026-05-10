package repcheck.e2e.fixtures

import io.circe.Json

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit coverage for [[AmendmentChainFixtures]] — the programmatic JSON helper that materializes the sample amendment
 * chain. No docker, no compose stack — pure Scala. Runs on every `sbt test` so drift between the helper's output shape
 * and the WireMock fixtures it documents is caught at unit-test time, not on `dockerComposeE2e/test` (which is gated on
 * `DockerRequired`).
 */
class AmendmentChainFixturesSpec extends AnyFlatSpec with Matchers {

  private val wmBase = "http://wiremock:8080"

  "AmendmentChainFixtures.listResponse" should "emit one item per amendment with a WireMock-loopback URL" in {
    val json     = AmendmentChainFixtures.listResponse(AmendmentChainFixtures.sampleChain, wmBase)
    val items    = json.hcursor.downField("amendments").as[List[Json]].toOption.getOrElse(Nil)
    val _        = items should have size 3
    val firstUrl = items.headOption.flatMap(_.hcursor.downField("url").as[String].toOption).getOrElse("")
    firstUrl should startWith(wmBase + "/v3/amendment/")
  }

  it should "report pagination.count = number of items" in {
    val json  = AmendmentChainFixtures.listResponse(AmendmentChainFixtures.sampleChain, wmBase)
    val count = json.hcursor.downField("pagination").downField("count").as[Int].toOption
    count shouldBe Some(3)
  }

  "AmendmentChainFixtures.detailResponse" should "carry amendedBill when parent is a Bill" in {
    val json   = AmendmentChainFixtures.detailResponse(AmendmentChainFixtures.sampleSamdt100, wmBase)
    val parent = json.hcursor.downField("amendment").downField("amendedBill").focus
    val _      = parent shouldBe defined
    val title  = json.hcursor.downField("amendment").downField("amendedBill").downField("title").as[String].toOption
    title shouldBe Some("Infrastructure Investment and Jobs Act")
  }

  it should "carry amendedAmendment when parent is another Amendment" in {
    val json   = AmendmentChainFixtures.detailResponse(AmendmentChainFixtures.sampleSuamdt200, wmBase)
    val parent = json.hcursor.downField("amendment").downField("amendedAmendment").focus
    val _      = parent shouldBe defined
    val pNumber =
      json.hcursor.downField("amendment").downField("amendedAmendment").downField("number").as[String].toOption
    pNumber shouldBe Some("100")
  }

  "AmendmentChainFixtures.textVersionsResponse" should "group multiple formats under one textVersions entry" in {
    val entries = List(
      AmendmentChainFixtures.TextVersionEntry("Submitted", "2021-08-01T12:00:00Z", "HTML", "u1"),
      AmendmentChainFixtures.TextVersionEntry("Submitted", "2021-08-01T12:00:00Z", "PDF", "u2"),
    )
    val json     = AmendmentChainFixtures.textVersionsResponse(entries)
    val versions = json.hcursor.downField("textVersions").as[List[Json]].toOption.getOrElse(Nil)
    val _        = versions should have size 1
    val formats  = versions.headOption.flatMap(_.hcursor.downField("formats").as[List[Json]].toOption).getOrElse(Nil)
    formats should have size 2
  }

  it should "emit an empty list for no entries" in {
    val json     = AmendmentChainFixtures.textVersionsResponse(Nil)
    val versions = json.hcursor.downField("textVersions").as[List[Json]].toOption.getOrElse(Nil)
    val _        = versions shouldBe empty
    json.hcursor.downField("pagination").downField("count").as[Int].toOption shouldBe Some(0)
  }

  "AmendmentChainFixtures.pretty" should "produce indented output" in {
    val json   = AmendmentChainFixtures.listResponse(AmendmentChainFixtures.sampleChain, wmBase)
    val pretty = AmendmentChainFixtures.pretty(json)
    pretty should include("\n")
  }

}
