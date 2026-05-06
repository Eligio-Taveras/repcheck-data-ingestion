package repcheck.ingestion.amendments.persistence

import scala.concurrent.duration._

import cats.effect.IO

import doobie.Transactor
import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.errors.InvalidAmendmentNaturalKey
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.common.Chamber
import repcheck.shared.models.congress.dos.amendment.AmendmentDO

/**
 * Pure unit coverage for [[DoobieAmendmentRepository]]. Verifies every method produces a well-formed `ConnectionIO` (so
 * the Doobie interpolator compiles and assembles its fragments) and exercises the failure paths of the natural-key
 * parser without spinning up a real Postgres instance. Real-SQL coverage lives in [[DoobieAmendmentRepositorySpec]]
 * (`DockerRequired`).
 */
class DoobieAmendmentRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieAmendmentRepository

  private val sample = AmendmentDO(
    amendmentId = 0L,
    naturalKey = "117-SAMDT-2137",
    congress = 117,
    amendmentType = Some(AmendmentType.SAMDT),
    number = "2137",
    billId = None,
    chamber = Chamber.Senate,
    description = None,
    purpose = None,
    sponsorMemberId = None,
    submittedDate = None,
    proposedDate = None,
    latestActionDate = None,
    latestActionTime = None,
    latestActionText = None,
    updateDate = None,
    apiUrl = None,
    parentAmendmentId = None,
    lastTextCheckAt = None,
    createdAt = None,
    updatedAt = None,
  )

  "upsert" should "produce a ConnectionIO for the upsert" in {
    val cio = repo.upsert(sample)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "insertIfNotExists" should "produce a ConnectionIO" in {
    val cio = repo.insertIfNotExists(sample)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findById" should "produce a ConnectionIO for any id" in {
    val _ = repo.findById(0L) shouldBe a[doobie.ConnectionIO[?]]
    val _ = repo.findById(1L) shouldBe a[doobie.ConnectionIO[?]]
    repo.findById(-1L) shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByNaturalKey" should "produce a ConnectionIO for any string" in {
    val _ = repo.findByNaturalKey("117-SAMDT-2137") shouldBe a[doobie.ConnectionIO[?]]
    repo.findByNaturalKey("") shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByNaturalKeys" should "short-circuit to the empty map for an empty input list without touching the DB" in {
    import cats.effect.unsafe.implicits.global
    // Empty input must not issue SQL — assert by transacting via an in-memory H2 with no `amendments` table. If a
    // SELECT escaped, H2 would raise `Table AMENDMENTS not found` and the test would fail.
    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:amendments-empty;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )
    val out = repo.findByNaturalKeys(List.empty).transact(xa).unsafeRunSync()
    out shouldBe Map.empty[String, AmendmentDO]
  }

  it should "produce a ConnectionIO for a single-element list" in {
    repo.findByNaturalKeys(List("117-SAMDT-2137")) shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a multi-element list" in {
    repo.findByNaturalKeys(List("117-SAMDT-1", "118-HAMDT-2", "119-SUAMDT-3")) shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBillId / findByCongress / findByParentAmendmentId" should "all produce ConnectionIOs" in {
    val _ = repo.findByBillId(42L) shouldBe a[doobie.ConnectionIO[?]]
    val _ = repo.findByCongress(118) shouldBe a[doobie.ConnectionIO[?]]
    repo.findByParentAmendmentId(7L) shouldBe a[doobie.ConnectionIO[?]]
  }

  "findCandidatesForTextCheck" should "produce a ConnectionIO" in {
    val cio = repo.findCandidatesForTextCheck(minCongress = 117, staleAfter = 4.hours)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "updateLastTextCheckAt" should "produce a ConnectionIO" in {
    repo.updateLastTextCheckAt(42L) shouldBe a[doobie.ConnectionIO[?]]
  }

  "upsertPlaceholder" should "produce a ConnectionIO when the natural key is well-formed" in {
    repo.upsertPlaceholder("117-SAMDT-2137") shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "raise InvalidAmendmentNaturalKey inside the ConnectionIO when the natural key is malformed" in {
    import cats.effect.unsafe.implicits.global
    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:amendments-placeholder-raise;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )
    // Use a key with only one segment — guaranteed to trip the `parts.length != 3` guard rather than reaching the
    // per-segment parse step.
    val outcome = repo.upsertPlaceholder("bogus").transact(xa).attempt.unsafeRunSync()
    outcome match {
      case Left(e: InvalidAmendmentNaturalKey) =>
        val _ = e.naturalKey shouldBe "bogus"
        e.getMessage should include("3 '-' segments")
      case other => fail(s"expected Left(InvalidAmendmentNaturalKey), got ${other.toString}")
    }
  }

  "parsePlaceholderNaturalKey" should "accept the canonical '<congress>-<TYPE>-<number>' form" in {
    repo.parsePlaceholderNaturalKey("117-SAMDT-2137") shouldBe Right((117, AmendmentType.SAMDT, "2137"))
  }

  it should "resolve the amendment type case-insensitively" in {
    val _ = repo.parsePlaceholderNaturalKey("117-samdt-2137") shouldBe Right((117, AmendmentType.SAMDT, "2137"))
    repo.parsePlaceholderNaturalKey("117-Samdt-2137") shouldBe Right((117, AmendmentType.SAMDT, "2137"))
  }

  it should "accept every AmendmentType variant" in {
    val _ = repo.parsePlaceholderNaturalKey("118-HAMDT-1") shouldBe Right((118, AmendmentType.HAMDT, "1"))
    val _ = repo.parsePlaceholderNaturalKey("118-SAMDT-2") shouldBe Right((118, AmendmentType.SAMDT, "2"))
    repo.parsePlaceholderNaturalKey("118-SUAMDT-3") shouldBe Right((118, AmendmentType.SUAMDT, "3"))
  }

  it should "return Left(InvalidAmendmentNaturalKey) for a key with fewer than 3 segments" in {
    val outcome = repo.parsePlaceholderNaturalKey("117-SAMDT")
    outcome match {
      case Left(e: InvalidAmendmentNaturalKey) =>
        val _ = e.naturalKey shouldBe "117-SAMDT"
        e.getMessage should include("3 '-' segments")
      case other => fail(s"expected Left(InvalidAmendmentNaturalKey), got ${other.toString}")
    }
  }

  it should "return Left(InvalidAmendmentNaturalKey) when congress is not an int" in {
    val outcome = repo.parsePlaceholderNaturalKey("abc-SAMDT-2137")
    outcome match {
      case Left(e: InvalidAmendmentNaturalKey) =>
        val _ = e.naturalKey shouldBe "abc-SAMDT-2137"
        e.getMessage should include("congress segment 'abc' is not an int")
      case other => fail(s"expected Left(InvalidAmendmentNaturalKey), got ${other.toString}")
    }
  }

  it should "return Left(InvalidAmendmentNaturalKey) when amendment_type is unrecognized" in {
    val outcome = repo.parsePlaceholderNaturalKey("117-BOGUS-2137")
    outcome match {
      case Left(e: InvalidAmendmentNaturalKey) =>
        val _ = e.naturalKey shouldBe "117-BOGUS-2137"
        e.getMessage should include("Unrecognized AmendmentType")
      case other => fail(s"expected Left(InvalidAmendmentNaturalKey), got ${other.toString}")
    }
  }

  it should "return Left(InvalidAmendmentNaturalKey) when number is empty" in {
    val outcome = repo.parsePlaceholderNaturalKey("117-SAMDT-")
    outcome match {
      case Left(e: InvalidAmendmentNaturalKey) =>
        val _ = e.naturalKey shouldBe "117-SAMDT-"
        e.getMessage should include("number segment is empty")
      case other => fail(s"expected Left(InvalidAmendmentNaturalKey), got ${other.toString}")
    }
  }

  "chamberFromAmendmentType" should "map HAMDT to House" in {
    repo.chamberFromAmendmentType(AmendmentType.HAMDT) shouldBe Chamber.House
  }

  it should "map SAMDT to Senate" in {
    repo.chamberFromAmendmentType(AmendmentType.SAMDT) shouldBe Chamber.Senate
  }

  it should "map SUAMDT to Senate" in {
    repo.chamberFromAmendmentType(AmendmentType.SUAMDT) shouldBe Chamber.Senate
  }

  "DoobieAmendmentRepository" should "implement AmendmentRepository[ConnectionIO]" in {
    repo shouldBe a[AmendmentRepository[?]]
  }

}
