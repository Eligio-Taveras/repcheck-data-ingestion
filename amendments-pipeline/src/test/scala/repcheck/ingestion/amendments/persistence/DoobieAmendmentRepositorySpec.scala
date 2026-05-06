package repcheck.ingestion.amendments.persistence

import java.time.{Instant, LocalDate}

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.errors.InvalidAmendmentNaturalKey
import repcheck.ingestion.amendments.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.common.Chamber
import repcheck.shared.models.congress.dos.amendment.AmendmentDO

/**
 * AlloyDB Omni-backed integration spec for [[DoobieAmendmentRepository]]. Every test is tagged `DockerRequired` so `sbt
 * test` skips the suite when Docker isn't present; CI runs them via the `dockerTest` alias.
 *
 * Coverage includes the change-detector pre-filter SQL, idempotency of `upsertPlaceholder`, the empty-list short
 * circuit on `findByNaturalKeys`, the FK constraint surface, and concurrent-1-row writes through `parTraverse`.
 */
class DoobieAmendmentRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieAmendmentRepository

  private def baseDO(
    naturalKey: String = "117-SAMDT-2137",
    congress: Int = 117,
    amendmentType: Option[AmendmentType] = Some(AmendmentType.SAMDT),
    number: String = "2137",
    chamber: Chamber = Chamber.Senate,
    billId: Option[Long] = None,
    sponsorMemberId: Option[Long] = None,
    parentAmendmentId: Option[Long] = None,
    description: Option[String] = Some("In the nature of a substitute."),
    purpose: Option[String] = Some("Strike all after the enacting clause and insert"),
    submittedDate: Option[LocalDate] = Some(LocalDate.parse("2021-08-01")),
    proposedDate: Option[LocalDate] = Some(LocalDate.parse("2021-08-02")),
    latestActionDate: Option[LocalDate] = Some(LocalDate.parse("2021-08-10")),
    latestActionTime: Option[String] = Some("14:30:00"),
    latestActionText: Option[String] = Some("Amendment SA 2137 agreed to in Senate by Voice Vote."),
    updateDate: Option[Instant] = Some(Instant.parse("2024-01-15T00:00:00Z")),
    apiUrl: Option[String] = Some("https://api.congress.gov/v3/amendment/117/samdt/2137"),
    lastTextCheckAt: Option[Instant] = None,
  ): AmendmentDO = AmendmentDO(
    amendmentId = 0L,
    naturalKey = naturalKey,
    congress = congress,
    amendmentType = amendmentType,
    number = number,
    billId = billId,
    chamber = chamber,
    description = description,
    purpose = purpose,
    sponsorMemberId = sponsorMemberId,
    submittedDate = submittedDate,
    proposedDate = proposedDate,
    latestActionDate = latestActionDate,
    latestActionTime = latestActionTime,
    latestActionText = latestActionText,
    updateDate = updateDate,
    apiUrl = apiUrl,
    parentAmendmentId = parentAmendmentId,
    lastTextCheckAt = lastTextCheckAt,
    createdAt = None,
    updatedAt = None,
  )

  // ============================================================================================
  // upsert — full DO round-trip + RETURNING * coverage
  // ============================================================================================

  "upsert" should "insert a fully populated row and return it with a generated id" taggedAs DockerRequired in {
    val stored = repo.upsert(baseDO()).transact(xa).unsafeRunSync()

    val _ = stored.amendmentId should be > 0L
    val _ = stored.naturalKey shouldBe "117-SAMDT-2137"
    val _ = stored.congress shouldBe 117
    val _ = stored.amendmentType shouldBe Some(AmendmentType.SAMDT)
    val _ = stored.number shouldBe "2137"
    val _ = stored.chamber shouldBe Chamber.Senate
    val _ = stored.description shouldBe Some("In the nature of a substitute.")
    val _ = stored.purpose shouldBe Some("Strike all after the enacting clause and insert")
    val _ = stored.submittedDate shouldBe Some(LocalDate.parse("2021-08-01"))
    val _ = stored.proposedDate shouldBe Some(LocalDate.parse("2021-08-02"))
    val _ = stored.latestActionDate shouldBe Some(LocalDate.parse("2021-08-10"))
    val _ = stored.latestActionTime shouldBe Some("14:30:00")
    val _ = stored.updateDate shouldBe Some(Instant.parse("2024-01-15T00:00:00Z"))
    val _ = stored.apiUrl shouldBe Some("https://api.congress.gov/v3/amendment/117/samdt/2137")
    val _ = stored.createdAt shouldBe defined
    stored.updatedAt shouldBe defined
  }

  it should "update an existing row on conflict (natural_key collision)" taggedAs DockerRequired in {
    val first  = repo.upsert(baseDO()).transact(xa).unsafeRunSync()
    val second = repo.upsert(baseDO(description = Some("Updated description"))).transact(xa).unsafeRunSync()

    val _ = second.amendmentId shouldBe first.amendmentId
    val _ = second.description shouldBe Some("Updated description")
    countAmendmentsByNaturalKey("117-SAMDT-2137") shouldBe 1
  }

  it should "set updated_at on every conflict-update branch" taggedAs DockerRequired in {
    val first = repo.upsert(baseDO()).transact(xa).unsafeRunSync()
    Thread.sleep(50)
    val second = repo.upsert(baseDO(description = Some("Changed"))).transact(xa).unsafeRunSync()
    second.updatedAt should not be first.updatedAt
  }

  it should "round-trip optional fields as None when the DO has them all None" taggedAs DockerRequired in {
    val skinny = baseDO(
      description = None,
      purpose = None,
      submittedDate = None,
      proposedDate = None,
      latestActionDate = None,
      latestActionTime = None,
      latestActionText = None,
      updateDate = None,
      apiUrl = None,
      lastTextCheckAt = None,
    )
    val stored = repo.upsert(skinny).transact(xa).unsafeRunSync()
    val _      = stored.description shouldBe None
    val _      = stored.purpose shouldBe None
    val _      = stored.submittedDate shouldBe None
    val _      = stored.proposedDate shouldBe None
    val _      = stored.latestActionDate shouldBe None
    val _      = stored.latestActionTime shouldBe None
    val _      = stored.latestActionText shouldBe None
    val _      = stored.updateDate shouldBe None
    val _      = stored.apiUrl shouldBe None
    stored.lastTextCheckAt shouldBe None
  }

  it should "persist bill_id and sponsor_member_id when present" taggedAs DockerRequired in {
    val billId   = insertBill(billNumber = 1)
    val memberId = insertMember("TEST-AMEND-001")
    val withLinks =
      baseDO(naturalKey = "117-SAMDT-2200", number = "2200", billId = Some(billId), sponsorMemberId = Some(memberId))
    val stored = repo.upsert(withLinks).transact(xa).unsafeRunSync()
    val _      = stored.billId shouldBe Some(billId)
    stored.sponsorMemberId shouldBe Some(memberId)
  }

  it should "fail with FK violation when bill_id references an unknown bill" taggedAs DockerRequired in {
    val orphan = baseDO(billId = Some(999_999_999L))
    val out    = repo.upsert(orphan).transact(xa).attempt.unsafeRunSync()
    out.isLeft shouldBe true
  }

  it should "fail with FK violation when sponsor_member_id references an unknown member" taggedAs DockerRequired in {
    val orphan = baseDO(sponsorMemberId = Some(999_999_999L))
    val out    = repo.upsert(orphan).transact(xa).attempt.unsafeRunSync()
    out.isLeft shouldBe true
  }

  it should "persist parent_amendment_id as a self-referential FK" taggedAs DockerRequired in {
    val parent = repo.upsert(baseDO(naturalKey = "117-SAMDT-1", number = "1")).transact(xa).unsafeRunSync()
    val child = baseDO(
      naturalKey = "117-SUAMDT-1",
      number = "1",
      amendmentType = Some(AmendmentType.SUAMDT),
      parentAmendmentId = Some(parent.amendmentId),
    )
    val storedChild = repo.upsert(child).transact(xa).unsafeRunSync()
    storedChild.parentAmendmentId shouldBe Some(parent.amendmentId)
  }

  // ============================================================================================
  // upsertPlaceholder — single-statement, idempotent, chamber-derived from amendment_type
  // ============================================================================================

  "upsertPlaceholder" should "insert a row with the parsed natural key and a derived chamber" taggedAs DockerRequired in {
    val _      = repo.upsertPlaceholder("117-SAMDT-9001").transact(xa).unsafeRunSync()
    val _      = countAmendmentsByNaturalKey("117-SAMDT-9001") shouldBe 1
    val stored = repo.findByNaturalKey("117-SAMDT-9001").transact(xa).unsafeRunSync()
    stored match {
      case Some(a) =>
        val _ = a.congress shouldBe 117
        val _ = a.amendmentType shouldBe Some(AmendmentType.SAMDT)
        val _ = a.number shouldBe "9001"
        val _ = a.chamber shouldBe Chamber.Senate
        val _ = a.description shouldBe None
        a.updateDate shouldBe None
      case None => fail("Expected placeholder amendment to be findable via findByNaturalKey")
    }
  }

  it should "derive Chamber.House for HAMDT placeholders" taggedAs DockerRequired in {
    val _      = repo.upsertPlaceholder("118-HAMDT-7").transact(xa).unsafeRunSync()
    val stored = repo.findByNaturalKey("118-HAMDT-7").transact(xa).unsafeRunSync()
    stored.map(_.chamber) shouldBe Some(Chamber.House)
  }

  it should "derive Chamber.Senate for SUAMDT placeholders" taggedAs DockerRequired in {
    val _      = repo.upsertPlaceholder("117-SUAMDT-3").transact(xa).unsafeRunSync()
    val stored = repo.findByNaturalKey("117-SUAMDT-3").transact(xa).unsafeRunSync()
    stored.map(_.chamber) shouldBe Some(Chamber.Senate)
  }

  it should "be idempotent when the same natural key is inserted twice (ON CONFLICT DO NOTHING)" taggedAs DockerRequired in {
    val _ = repo.upsertPlaceholder("117-SAMDT-9002").transact(xa).unsafeRunSync()
    val _ = repo.upsertPlaceholder("117-SAMDT-9002").transact(xa).unsafeRunSync()
    countAmendmentsByNaturalKey("117-SAMDT-9002") shouldBe 1
  }

  it should "NOT overwrite a real (non-placeholder) amendment if one already exists" taggedAs DockerRequired in {
    val _      = repo.upsert(baseDO(naturalKey = "117-SAMDT-9003", number = "9003")).transact(xa).unsafeRunSync()
    val _      = repo.upsertPlaceholder("117-SAMDT-9003").transact(xa).unsafeRunSync()
    val stored = repo.findByNaturalKey("117-SAMDT-9003").transact(xa).unsafeRunSync()
    // The placeholder is a no-op: original description survives.
    stored.flatMap(_.description) shouldBe Some("In the nature of a substitute.")
  }

  it should "raise InvalidAmendmentNaturalKey for a malformed key" taggedAs DockerRequired in {
    val out = repo.upsertPlaceholder("totally-bogus").transact(xa).attempt.unsafeRunSync()
    out match {
      case Left(_: InvalidAmendmentNaturalKey) => succeed
      case other                               => fail(s"expected InvalidAmendmentNaturalKey, got ${other.toString}")
    }
  }

  it should "produce exactly one row when the same natural key is concurrently inserted by 10 fibers (parTraverse)" taggedAs DockerRequired in {
    // Stress test for the ON CONFLICT DO NOTHING idempotency. Without the conflict clause this would race and emit
    // 10 row insertions — the unique constraint on natural_key enforces exactly one survivor.
    val key  = "117-SAMDT-9999"
    val work = (1 to 10).toList.parTraverse_(_ => repo.upsertPlaceholder(key).transact(xa))
    val _    = work.unsafeRunSync()
    countAmendmentsByNaturalKey(key) shouldBe 1
  }

  it should "leave update_date NULL on a placeholder row" taggedAs DockerRequired in {
    // Mirrors the BillRepository.upsertPlaceholder contract: the placeholder must NOT stamp NOW() on update_date or
    // a future change-detector compare against API updateDate would erroneously short-circuit to "unchanged".
    val _      = repo.upsertPlaceholder("117-SAMDT-9100").transact(xa).unsafeRunSync()
    val stored = repo.findByNaturalKey("117-SAMDT-9100").transact(xa).unsafeRunSync()
    stored.flatMap(_.updateDate) shouldBe None
  }

  // ============================================================================================
  // findById / findByNaturalKey / findByNaturalKeys
  // ============================================================================================

  "findById" should "return the stored amendment by surrogate id" taggedAs DockerRequired in {
    val stored = repo.upsert(baseDO()).transact(xa).unsafeRunSync()
    val found  = repo.findById(stored.amendmentId).transact(xa).unsafeRunSync()
    found.map(_.naturalKey) shouldBe Some("117-SAMDT-2137")
  }

  it should "return None for a missing id" taggedAs DockerRequired in {
    repo.findById(999_999_999L).transact(xa).unsafeRunSync() shouldBe None
  }

  "findByNaturalKey" should "return None for a missing natural key" taggedAs DockerRequired in {
    repo.findByNaturalKey("117-SAMDT-MISSING").transact(xa).unsafeRunSync() shouldBe None
  }

  "findByNaturalKeys" should "return the empty map for an empty input list without issuing SQL" taggedAs DockerRequired in {
    repo.findByNaturalKeys(List.empty).transact(xa).unsafeRunSync() shouldBe Map.empty[String, AmendmentDO]
  }

  it should "return a map keyed by natural key for a non-empty input list" taggedAs DockerRequired in {
    val _   = repo.upsert(baseDO(naturalKey = "117-SAMDT-1", number = "1")).transact(xa).unsafeRunSync()
    val _   = repo.upsert(baseDO(naturalKey = "117-SAMDT-2", number = "2")).transact(xa).unsafeRunSync()
    val _   = repo.upsert(baseDO(naturalKey = "117-SAMDT-3", number = "3")).transact(xa).unsafeRunSync()
    val out = repo.findByNaturalKeys(List("117-SAMDT-1", "117-SAMDT-2")).transact(xa).unsafeRunSync()
    val _   = out.keySet shouldBe Set("117-SAMDT-1", "117-SAMDT-2")
    out.get("117-SAMDT-1").map(_.number) shouldBe Some("1")
  }

  it should "ignore missing natural keys" taggedAs DockerRequired in {
    val _   = repo.upsert(baseDO(naturalKey = "117-SAMDT-1", number = "1")).transact(xa).unsafeRunSync()
    val out = repo.findByNaturalKeys(List("117-SAMDT-1", "117-SAMDT-MISSING")).transact(xa).unsafeRunSync()
    val _   = out.size shouldBe 1
    out.contains("117-SAMDT-1") shouldBe true
  }

  // ============================================================================================
  // findByBillId / findByCongress / findByParentAmendmentId
  // ============================================================================================

  "findByBillId" should "return amendments associated with a bill" taggedAs DockerRequired in {
    val billId = insertBill(billNumber = 100)
    val _ = repo
      .upsert(baseDO(naturalKey = "117-SAMDT-100A", number = "100A", billId = Some(billId)))
      .transact(xa)
      .unsafeRunSync()
    val _ = repo
      .upsert(baseDO(naturalKey = "117-SAMDT-100B", number = "100B", billId = Some(billId)))
      .transact(xa)
      .unsafeRunSync()
    val _ =
      repo.upsert(baseDO(naturalKey = "117-SAMDT-200", number = "200", billId = None)).transact(xa).unsafeRunSync()

    val out = repo.findByBillId(billId).transact(xa).unsafeRunSync()
    val _   = out.size shouldBe 2
    out.map(_.naturalKey).toSet shouldBe Set("117-SAMDT-100A", "117-SAMDT-100B")
  }

  it should "return empty list for an unknown bill id" taggedAs DockerRequired in {
    repo.findByBillId(999_999_999L).transact(xa).unsafeRunSync() shouldBe empty
  }

  "findByCongress" should "filter by congress" taggedAs DockerRequired in {
    val _ = repo.upsert(baseDO(naturalKey = "117-SAMDT-1", number = "1")).transact(xa).unsafeRunSync()
    val _ = repo.upsert(baseDO(naturalKey = "118-SAMDT-1", number = "1", congress = 118)).transact(xa).unsafeRunSync()
    val _ = repo.upsert(baseDO(naturalKey = "118-SAMDT-2", number = "2", congress = 118)).transact(xa).unsafeRunSync()

    val out117 = repo.findByCongress(117).transact(xa).unsafeRunSync()
    val out118 = repo.findByCongress(118).transact(xa).unsafeRunSync()
    val _      = out117.map(_.naturalKey) shouldBe List("117-SAMDT-1")
    out118.map(_.naturalKey).toSet shouldBe Set("118-SAMDT-1", "118-SAMDT-2")
  }

  "findByParentAmendmentId" should "return all sub-amendments of a parent" taggedAs DockerRequired in {
    val parent = repo.upsert(baseDO(naturalKey = "117-SAMDT-500", number = "500")).transact(xa).unsafeRunSync()
    val _ = repo
      .upsert(
        baseDO(
          naturalKey = "117-SUAMDT-501",
          number = "501",
          amendmentType = Some(AmendmentType.SUAMDT),
          parentAmendmentId = Some(parent.amendmentId),
        )
      )
      .transact(xa)
      .unsafeRunSync()
    val _ = repo
      .upsert(
        baseDO(
          naturalKey = "117-SUAMDT-502",
          number = "502",
          amendmentType = Some(AmendmentType.SUAMDT),
          parentAmendmentId = Some(parent.amendmentId),
        )
      )
      .transact(xa)
      .unsafeRunSync()

    val children = repo.findByParentAmendmentId(parent.amendmentId).transact(xa).unsafeRunSync()
    children.map(_.naturalKey).toSet shouldBe Set("117-SUAMDT-501", "117-SUAMDT-502")
  }

  // ============================================================================================
  // findCandidatesForTextCheck — change-detector pre-filter
  // ============================================================================================

  "findCandidatesForTextCheck" should "exclude rows below minCongress" taggedAs DockerRequired in {
    val _ = repo.upsert(baseDO(naturalKey = "115-SAMDT-1", number = "1", congress = 115)).transact(xa).unsafeRunSync()
    val _ = repo.upsert(baseDO(naturalKey = "117-SAMDT-1", number = "1", congress = 117)).transact(xa).unsafeRunSync()

    val out = repo.findCandidatesForTextCheck(minCongress = 117, staleAfter = 4.hours).transact(xa).unsafeRunSync()
    val _   = out.map(_.naturalKey) should not contain "115-SAMDT-1"
    out.map(_.naturalKey) should contain("117-SAMDT-1")
  }

  it should "exclude SUAMDT rows" taggedAs DockerRequired in {
    val _ = repo.upsert(baseDO(naturalKey = "117-SAMDT-1", number = "1")).transact(xa).unsafeRunSync()
    val _ = repo
      .upsert(baseDO(naturalKey = "117-SUAMDT-1", number = "1", amendmentType = Some(AmendmentType.SUAMDT)))
      .transact(xa)
      .unsafeRunSync()

    val out = repo.findCandidatesForTextCheck(minCongress = 117, staleAfter = 4.hours).transact(xa).unsafeRunSync()
    val _   = out.map(_.amendmentType).flatten should not contain AmendmentType.SUAMDT
    out.map(_.naturalKey) should contain("117-SAMDT-1")
  }

  it should "include rows whose last_text_check_at is NULL" taggedAs DockerRequired in {
    val _ = repo
      .upsert(baseDO(naturalKey = "117-SAMDT-NEVER", number = "9000", lastTextCheckAt = None))
      .transact(xa)
      .unsafeRunSync()
    val out = repo.findCandidatesForTextCheck(minCongress = 117, staleAfter = 4.hours).transact(xa).unsafeRunSync()
    out.map(_.naturalKey) should contain("117-SAMDT-NEVER")
  }

  it should "include rows whose last_text_check_at is older than staleAfter" taggedAs DockerRequired in {
    val checked = Instant.now().minusSeconds(8 * 3600) // 8 hours ago — well past 4h stale window
    val _ = repo
      .upsert(baseDO(naturalKey = "117-SAMDT-STALE", number = "9001", lastTextCheckAt = Some(checked)))
      .transact(xa)
      .unsafeRunSync()
    val out = repo.findCandidatesForTextCheck(minCongress = 117, staleAfter = 4.hours).transact(xa).unsafeRunSync()
    out.map(_.naturalKey) should contain("117-SAMDT-STALE")
  }

  it should "exclude rows whose last_text_check_at is within staleAfter" taggedAs DockerRequired in {
    val checked = Instant.now().minusSeconds(60) // a minute ago, well within 4h stale window
    val _ = repo
      .upsert(baseDO(naturalKey = "117-SAMDT-FRESH", number = "9002", lastTextCheckAt = Some(checked)))
      .transact(xa)
      .unsafeRunSync()
    val out = repo.findCandidatesForTextCheck(minCongress = 117, staleAfter = 4.hours).transact(xa).unsafeRunSync()
    out.map(_.naturalKey) should not contain "117-SAMDT-FRESH"
  }

  // ============================================================================================
  // updateLastTextCheckAt — only sets the column, leaves business fields untouched
  // ============================================================================================

  "updateLastTextCheckAt" should "stamp NOW() on the targeted row only" taggedAs DockerRequired in {
    val a = repo
      .upsert(baseDO(naturalKey = "117-SAMDT-A", number = "A", description = Some("untouched")))
      .transact(xa)
      .unsafeRunSync()
    val b    = repo.upsert(baseDO(naturalKey = "117-SAMDT-B", number = "B")).transact(xa).unsafeRunSync()
    val _    = repo.updateLastTextCheckAt(a.amendmentId).transact(xa).unsafeRunSync()
    val newA = repo.findById(a.amendmentId).transact(xa).unsafeRunSync()
    val newB = repo.findById(b.amendmentId).transact(xa).unsafeRunSync()
    val _    = newA.flatMap(_.lastTextCheckAt) shouldBe defined
    // Description on `a` must survive — we only updated last_text_check_at, not the business columns.
    val _ = newA.flatMap(_.description) shouldBe Some("untouched")
    // Sibling row `b` is untouched.
    newB.flatMap(_.lastTextCheckAt) shouldBe None
  }

  // ============================================================================================
  // insertIfNotExists — EntityRepository contract
  // ============================================================================================

  "insertIfNotExists" should "insert when the row does not exist" taggedAs DockerRequired in {
    val _ = repo.insertIfNotExists(baseDO(naturalKey = "117-SAMDT-IFE-1", number = "ife1")).transact(xa).unsafeRunSync()
    val stored = repo.findByNaturalKey("117-SAMDT-IFE-1").transact(xa).unsafeRunSync()
    stored.map(_.number) shouldBe Some("ife1")
  }

  it should "be a no-op when the row already exists (richer data preserved)" taggedAs DockerRequired in {
    val _ = repo
      .upsert(baseDO(naturalKey = "117-SAMDT-IFE-2", number = "ife2", description = Some("rich data")))
      .transact(xa)
      .unsafeRunSync()
    val _ = repo
      .insertIfNotExists(
        baseDO(naturalKey = "117-SAMDT-IFE-2", number = "ife2", description = Some("placeholder data"))
      )
      .transact(xa)
      .unsafeRunSync()
    val stored = repo.findByNaturalKey("117-SAMDT-IFE-2").transact(xa).unsafeRunSync()
    stored.flatMap(_.description) shouldBe Some("rich data")
  }

}
