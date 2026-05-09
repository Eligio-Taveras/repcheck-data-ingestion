package repcheck.ingestion.amendments.textcheck.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DoobieAmendmentTextVersionLookupSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieAmendmentTextVersionLookup

  "findExistingVersions" should "produce a ConnectionIO" in {
    val cio = repo.findExistingVersions(42L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
