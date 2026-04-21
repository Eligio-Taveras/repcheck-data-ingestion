package repcheck.ingestion.members.profile.integration

import java.time.Instant

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.persistence.{
  DoobieLisMappingRepository,
  DoobieMemberHistoryArchiver,
  DoobieMemberRepository,
  DoobieMemberTermRepository,
}
import repcheck.members.common.testing.{DockerRequired, TransactorFixture}
import repcheck.members.lismapping.repository.DoobieLisMemberRepository
import repcheck.shared.models.congress.common.{Chamber, UsState}
import repcheck.shared.models.congress.dos.member.{LisMemberDO, MemberLisMappingDO, MemberTermDO}
import repcheck.shared.models.congress.member.MemberType

/**
 * Cross-project integration tests for the placeholder fill lifecycle. Validates two inter-pipeline data flows:
 *
 * **Flow A — LIS refresher encounters placeholder, profile pipeline fills it later:**
 *   1. Bills pipeline creates a placeholder member row (only `natural_key` set; `update_date IS NULL`).
 *   1. LIS mapping refresher looks up the member by bioguide id, finds the placeholder, and creates a
 *      `member_lis_mapping` row pointing to the placeholder's `members.id`.
 *   1. Member profile pipeline later upserts the full member data; the placeholder row becomes a live record.
 *   1. The `member_lis_mapping` row is intact (FK to `members.id` unchanged — same row, updated fields).
 *
 * **Flow B — profile pipeline archives prior data before overwriting:**
 *   1. Member with existing terms is archived by the profile pipeline (snapshot into `member_history` +
 *      `member_term_history`).
 *   1. New terms replace the old ones via `replaceAll`.
 *   1. The live record reflects new data; history tables hold the snapshot.
 *
 * Infrastructure: shared [[repcheck.members.common.testing.SharedDockerPostgres]] AlloyDB Omni container.
 */
class PlaceholderFillIntegrationSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private val memberRepo    = new DoobieMemberRepository()
  private val termRepo      = new DoobieMemberTermRepository()
  private val archiver      = new DoobieMemberHistoryArchiver()
  private val lisMemberRepo = new DoobieLisMemberRepository()
  private val mappingRepo   = new DoobieLisMappingRepository()

  /**
   * Inserts a placeholder member (only `natural_key` set; all other columns default to NULL). Mirrors what
   * `DoobieEntityRepository.insertIfNotExists` does when `PlaceholderCreator.ensureExists` is called.
   */
  private def insertPlaceholder(bioguideId: String): Long =
    sql"""
      INSERT INTO members (natural_key)
      VALUES ($bioguideId)
      ON CONFLICT (natural_key) DO UPDATE SET updated_at = NOW()
      RETURNING id
    """.query[Long].unique.transact(xa).unsafeRunSync()

  private def buildLisMemberDO(lisId: String): LisMemberDO = LisMemberDO(
    id = 0L,
    naturalKey = lisId,
    firstName = Some("John"),
    lastName = Some("Placeholder"),
    party = Some("Republican"),
    state = Some("TX"),
    lastVerified = Some(Instant.parse("2024-06-15T00:00:00Z")),
    createdAt = None,
  )

  private def senatorTerm(memberId: Long): MemberTermDO = MemberTermDO(
    termId = 0L,
    memberId = memberId,
    chamber = Some(Chamber.Senate),
    congress = Some(118),
    startYear = Some(2023),
    endYear = None,
    memberType = Some(MemberType.Senator),
    stateCode = Some(UsState.Texas),
    stateName = Some("Texas"),
    district = None,
  )

  "PlaceholderFill" should "expose placeholder in findPlaceholders before the profile pipeline fills it" taggedAs DockerRequired in {
    val memberId     = insertPlaceholder("P000001")
    val placeholders = memberRepo.findPlaceholders().transact(xa).unsafeRunSync()

    placeholders.map(_.memberId) should contain(memberId)
  }

  it should "remove member from findPlaceholders after the profile pipeline upserts full data" taggedAs DockerRequired in {
    val _ = insertPlaceholder("P000002")
    // Profile pipeline fills the placeholder via upsert — insertMember uses ON CONFLICT DO UPDATE
    val memberId = insertMember("P000002", firstName = "John", lastName = "Filled")

    val placeholders = memberRepo.findPlaceholders().transact(xa).unsafeRunSync()
    placeholders.map(_.memberId) should not contain memberId
  }

  it should "preserve the LIS mapping after the profile pipeline fills a placeholder" taggedAs DockerRequired in {
    // Step 1: Bills pipeline creates placeholder
    val memberId = insertPlaceholder("P000003")

    // Step 2: LIS refresher finds placeholder and creates mapping
    val lisMemberId = lisMemberRepo.upsertByNaturalKey(buildLisMemberDO("S300")).transact(xa).unsafeRunSync()
    val _ = mappingRepo
      .upsert(MemberLisMappingDO(0L, memberId, lisMemberId, Instant.now()))
      .transact(xa)
      .unsafeRunSync()

    val hasMappingBeforeFill = memberRepo.existsWithLisMapping(memberId).transact(xa).unsafeRunSync()

    // Step 3: Profile pipeline fills the placeholder (upsert same natural_key → same members.id)
    //   archive is a no-op because placeholder has no terms
    archiver.archiveMember("P000003").transact(xa).unsafeRunSync()
    val filledId = insertMember("P000003", firstName = "John", lastName = "Filled")
    termRepo.replaceAll(filledId, List(senatorTerm(filledId))).transact(xa).unsafeRunSync()

    // Step 4: Mapping FK is still intact after the fill (same members.id row was updated in place)
    val hasMappingAfterFill = memberRepo.existsWithLisMapping(filledId).transact(xa).unsafeRunSync()
    val placeholders        = memberRepo.findPlaceholders().transact(xa).unsafeRunSync()

    val _ = hasMappingBeforeFill shouldBe true
    val _ = hasMappingAfterFill shouldBe true
    placeholders.map(_.memberId) should not contain filledId
  }

  it should "archive prior member data before the profile pipeline overwrites with updated data" taggedAs DockerRequired in {
    val memberId = insertMember("P000004")
    val initialTerm = MemberTermDO(
      termId = 0L,
      memberId = memberId,
      chamber = Some(Chamber.Senate),
      congress = Some(117),
      startYear = Some(2021),
      endYear = Some(2023),
      memberType = Some(MemberType.Senator),
      stateCode = Some(UsState.California),
      stateName = Some("California"),
      district = None,
    )
    termRepo.replaceAll(memberId, List(initialTerm)).transact(xa).unsafeRunSync()

    // Profile pipeline archives before upserting updated data
    archiver.archiveMember("P000004").transact(xa).unsafeRunSync()

    val historyCount = sql"SELECT COUNT(*) FROM member_history WHERE member_id = $memberId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    historyCount shouldBe 1L
  }

  it should "have term history populated after archiving a member with terms" taggedAs DockerRequired in {
    val memberId = insertMember("P000005")
    val terms = List(
      MemberTermDO(
        0L,
        memberId,
        Some(Chamber.Senate),
        Some(117),
        Some(2021),
        Some(2023),
        Some(MemberType.Senator),
        Some(UsState.NewYork),
        Some("New York"),
        None,
      ),
      MemberTermDO(
        0L,
        memberId,
        Some(Chamber.Senate),
        Some(118),
        Some(2023),
        None,
        Some(MemberType.Senator),
        Some(UsState.NewYork),
        Some("New York"),
        None,
      ),
    )
    termRepo.replaceAll(memberId, terms).transact(xa).unsafeRunSync()
    archiver.archiveMember("P000005").transact(xa).unsafeRunSync()

    val historyId = sql"SELECT id FROM member_history WHERE member_id = $memberId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    val termHistoryCount = sql"SELECT COUNT(*) FROM member_term_history WHERE history_id = $historyId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    termHistoryCount shouldBe 2L
  }

}
