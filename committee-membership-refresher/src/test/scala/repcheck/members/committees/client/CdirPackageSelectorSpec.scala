package repcheck.members.committees.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.committees.client.CdirPackageSelector.CdirPackageRef

class CdirPackageSelectorSpec extends AnyFlatSpec with Matchers {

  "congressYears" should "map a congress to its two calendar years" in {
    val _ = CdirPackageSelector.congressYears(117) shouldBe (2021, 2022)
    CdirPackageSelector.congressYears(1) shouldBe (1789, 1790)
  }

  "selectForCongress" should "pick the latest package issued within the congress's years" in {
    val packages = List(
      CdirPackageRef("CDIR-2021-07-13", "2021-07-13"),
      CdirPackageRef("CDIR-2022-10-26", "2022-10-26"),
      CdirPackageRef("CDIR-2019-09-30", "2019-09-30"),
    )
    CdirPackageSelector.selectForCongress(packages, 117) shouldBe Some("CDIR-2022-10-26")
  }

  it should "return None when no package falls in the congress's years" in {
    val packages = List(CdirPackageRef("CDIR-1999-01-01", "1999-01-01"))
    CdirPackageSelector.selectForCongress(packages, 117) shouldBe None
  }

}
