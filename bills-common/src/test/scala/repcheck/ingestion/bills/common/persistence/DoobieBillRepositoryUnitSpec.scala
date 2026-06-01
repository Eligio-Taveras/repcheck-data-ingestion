package repcheck.ingestion.bills.common.persistence

import java.time.Instant

import cats.effect.IO

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.errors.InvalidBillNaturalKey
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.dos.bill.BillDO

class DoobieBillRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieBillRepository

  private val sampleBill = BillDO(
    billId = 0L,
    naturalKey = "118-HR-100",
    congress = 118,
    billType = BillType.HR,
    number = "100",
    title = "Test Bill",
    originChamber = None,
    originChamberCode = None,
    introducedDate = None,
    policyArea = None,
    latestActionDate = None,
    latestActionText = None,
    constitutionalAuthorityText = None,
    sponsorMemberId = None,
    textVersionType = None,
    updateDate = Some(Instant.parse("2024-01-01T00:00:00Z")),
    updateDateIncludingText = None,
    legislationUrl = None,
    apiUrl = None,
    createdAt = None,
    updatedAt = None,
    latestTextVersionId = None,
  )

  "upsert" should "produce a ConnectionIO for the upsert" in {
    val cio = repo.upsert(sampleBill)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBillId" should "produce a ConnectionIO for the query" in {
    val cio = repo.findByBillId("118-HR-100")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBillIds" should "produce a ConnectionIO for a non-empty list" in {
    val cio = repo.findByBillIds(List("118-HR-100", "118-HR-200"))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for an empty list" in {
    val cio = repo.findByBillIds(List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a single-element list" in {
    val cio = repo.findByBillIds(List("118-HR-100"))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findBillsNeedingTextCheck" should "produce a ConnectionIO for the query" in {
    val cioAll      = repo.findBillsNeedingTextCheck(List.empty)
    val cioFiltered = repo.findBillsNeedingTextCheck(List(118, 119))
    val _           = cioAll shouldBe a[doobie.ConnectionIO[?]]
    cioFiltered shouldBe a[doobie.ConnectionIO[?]]
  }

  "updateTextFields" should "produce a ConnectionIO for the update" in {
    val cio = repo.updateTextFields("118-HR-100", "IH", 1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "parseNaturalKey" should "split a natural key into congress, type, and number" in {
    val (congress, billType, number) = repo.parseNaturalKey("118-HR-100")
    val _                            = congress shouldBe 118
    val _                            = billType shouldBe "hr"
    number shouldBe "100"
  }

  it should "handle senate bill types" in {
    val (congress, billType, number) = repo.parseNaturalKey("117-S-42")
    val _                            = congress shouldBe 117
    val _                            = billType shouldBe "s"
    number shouldBe "42"
  }

  it should "handle joint resolutions" in {
    val (congress, billType, number) = repo.parseNaturalKey("118-SJRES-5")
    val _                            = congress shouldBe 118
    val _                            = billType shouldBe "sjres"
    number shouldBe "5"
  }

  // ==========================================================================================
  // upsertPlaceholder + parsePlaceholderNaturalKey — added for votes-pipeline integration
  // ==========================================================================================

  "upsertPlaceholder" should "produce a ConnectionIO when the natural key is well-formed" in {
    val cio = repo.upsertPlaceholder("119-HR-30")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "raise InvalidBillNaturalKey inside the ConnectionIO when the natural key is malformed" in {
    import cats.effect.unsafe.implicits.global
    import doobie.implicits._
    import doobie.util.transactor.Transactor
    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:bill-placeholder-unit-raise;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )
    // Use a key with only one segment — guaranteed to trip the `parts.length != 3` guard rather than reaching the
    // per-segment parse step.
    val outcome = repo.upsertPlaceholder("bogus").transact(xa).attempt.unsafeRunSync()
    outcome match {
      case Left(e: InvalidBillNaturalKey) =>
        val _ = e.naturalKey shouldBe "bogus"
        e.getMessage should include("3 '-' segments")
      case other => fail(s"expected Left(InvalidBillNaturalKey), got $other")
    }
  }

  "parsePlaceholderNaturalKey" should "accept the canonical '<congress>-<TYPE>-<number>' form" in {
    repo.parsePlaceholderNaturalKey("119-HR-30") shouldBe Right((119, BillType.HR, 30))
  }

  it should "resolve the bill type case-insensitively" in {
    val _ = repo.parsePlaceholderNaturalKey("119-hr-30") shouldBe Right((119, BillType.HR, 30))
    repo.parsePlaceholderNaturalKey("119-Hr-30") shouldBe Right((119, BillType.HR, 30))
  }

  it should "accept every bill-like BillType variant" in {
    val _ = repo.parsePlaceholderNaturalKey("118-S-42") shouldBe Right((118, BillType.S, 42))
    val _ = repo.parsePlaceholderNaturalKey("119-HJRES-1") shouldBe Right((119, BillType.HJRES, 1))
    val _ = repo.parsePlaceholderNaturalKey("119-SJRES-5") shouldBe Right((119, BillType.SJRES, 5))
    val _ = repo.parsePlaceholderNaturalKey("119-HRES-100") shouldBe Right((119, BillType.HRES, 100))
    repo.parsePlaceholderNaturalKey("119-SRES-200") shouldBe Right((119, BillType.SRES, 200))
  }

  it should "return Left(InvalidBillNaturalKey) for a key with fewer than 3 segments" in {
    val outcome = repo.parsePlaceholderNaturalKey("119-HR")
    outcome match {
      case Left(e: InvalidBillNaturalKey) =>
        val _ = e.naturalKey shouldBe "119-HR"
        e.getMessage should include("3 '-' segments")
      case other => fail(s"expected Left(InvalidBillNaturalKey), got $other")
    }
  }

  it should "return Left(InvalidBillNaturalKey) when congress is not an int" in {
    val outcome = repo.parsePlaceholderNaturalKey("abc-HR-30")
    outcome match {
      case Left(e: InvalidBillNaturalKey) =>
        val _ = e.naturalKey shouldBe "abc-HR-30"
        e.getMessage should include("congress segment 'abc' is not an int")
      case other => fail(s"expected Left(InvalidBillNaturalKey), got $other")
    }
  }

  it should "return Left(InvalidBillNaturalKey) when bill_type is unrecognized" in {
    val outcome = repo.parsePlaceholderNaturalKey("119-BOGUS-30")
    outcome match {
      case Left(e: InvalidBillNaturalKey) =>
        val _ = e.naturalKey shouldBe "119-BOGUS-30"
        e.getMessage should include("Unrecognized BillType")
      case other => fail(s"expected Left(InvalidBillNaturalKey), got $other")
    }
  }

  it should "return Left(InvalidBillNaturalKey) when number is not an int" in {
    val outcome = repo.parsePlaceholderNaturalKey("119-HR-thirty")
    outcome match {
      case Left(e: InvalidBillNaturalKey) =>
        val _ = e.naturalKey shouldBe "119-HR-thirty"
        e.getMessage should include("number segment 'thirty' is not an int")
      case other => fail(s"expected Left(InvalidBillNaturalKey), got $other")
    }
  }

}
