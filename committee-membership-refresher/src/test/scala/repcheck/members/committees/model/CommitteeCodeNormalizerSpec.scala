package repcheck.members.committees.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CommitteeCodeNormalizerSpec extends AnyFlatSpec with Matchers {

  "fromCongressGov" should "strip hs prefix for House committees and uppercase" in {
    CommitteeCodeNormalizer.fromCongressGov("hsru00", "House") shouldBe "RU00"
  }

  it should "uppercase Senate codes without stripping prefix" in {
    CommitteeCodeNormalizer.fromCongressGov("ssra00", "Senate") shouldBe "SSRA00"
  }

  it should "uppercase Joint codes without stripping prefix" in {
    CommitteeCodeNormalizer.fromCongressGov("jjec00", "Joint") shouldBe "JJEC00"
  }

  it should "handle already uppercase input" in {
    CommitteeCodeNormalizer.fromCongressGov("HSRU00", "House") shouldBe "RU00"
  }

  it should "handle mixed case chamber name" in {
    CommitteeCodeNormalizer.fromCongressGov("hsap00", "house") shouldBe "AP00"
  }

  "fromHouseClerk" should "uppercase codes" in {
    CommitteeCodeNormalizer.fromHouseClerk("ru00") shouldBe "RU00"
  }

  it should "leave already uppercase codes unchanged" in {
    CommitteeCodeNormalizer.fromHouseClerk("AP00") shouldBe "AP00"
  }

  "fromSenateXml" should "uppercase codes" in {
    CommitteeCodeNormalizer.fromSenateXml("ssfi00") shouldBe "SSFI00"
  }

  it should "leave already uppercase codes unchanged" in {
    CommitteeCodeNormalizer.fromSenateXml("SSFI00") shouldBe "SSFI00"
  }

}
