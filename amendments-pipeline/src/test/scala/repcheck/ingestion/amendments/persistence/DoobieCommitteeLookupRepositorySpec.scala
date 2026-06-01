package repcheck.ingestion.amendments.persistence

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.testing.TransactorFixture
import repcheck.members.common.testing.DockerRequired

/**
 * AlloyDB Omni-backed integration spec for [[DoobieCommitteeLookupRepository]]. Tagged `DockerRequired` so `sbt test`
 * skips it without Docker; CI runs it via the `dockerTest` alias. Exercises the POSIX `substring(... from ...)` match
 * that H2 can't run, so this is the only place the resolution SQL is actually validated against Postgres.
 *
 * Test rows use deliberately fake systemCodes (`hsru00zz`, `hszz01`, `ssnope00`) to avoid colliding with any real
 * committee rows the shared container may already hold, and each test deletes its own row in a `finally`.
 */
class DoobieCommitteeLookupRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieCommitteeLookupRepository

  private def insertCommittee(naturalKey: String, url: String): Long =
    sql"""INSERT INTO committees (natural_key, name, url)
          VALUES ($naturalKey, 'Test Committee', $url)
          ON CONFLICT (natural_key) DO UPDATE SET url = EXCLUDED.url
          RETURNING id""".query[Long].unique.transact(xa).unsafeRunSync()

  private def deleteCommittee(naturalKey: String): Unit = {
    val _ = sql"""DELETE FROM committees WHERE natural_key = $naturalKey""".update.run.transact(xa).unsafeRunSync()
  }

  "findIdBySystemCode" should "resolve the systemCode embedded in committees.url" taggedAs DockerRequired in {
    val id = insertCommittee("ZZLOOKUP1", "https://api.congress.gov/v3/committee/house/hsru00zz?format=json")
    try
      repo.findIdBySystemCode("hsru00zz").transact(xa).unsafeRunSync() shouldBe Some(id)
    finally deleteCommittee("ZZLOOKUP1")
  }

  it should "lowercase the input before matching" taggedAs DockerRequired in {
    val id = insertCommittee("ZZLOOKUP2", "https://api.congress.gov/v3/committee/senate/sszz01?format=json")
    try
      repo.findIdBySystemCode("SSZZ01").transact(xa).unsafeRunSync() shouldBe Some(id)
    finally deleteCommittee("ZZLOOKUP2")
  }

  it should "return None when no committee URL contains the systemCode" taggedAs DockerRequired in {
    repo.findIdBySystemCode("ssnope00").transact(xa).unsafeRunSync() shouldBe None
  }

}
