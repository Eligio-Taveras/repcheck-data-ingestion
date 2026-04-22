package repcheck.ingestion.members.profile.integration

import java.time.Instant

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.persistence.{
  DoobieLisMappingRepository,
  DoobieLisMemberRepository,
  DoobieMemberRepository,
  UpsertResult,
}
import repcheck.members.common.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.dos.member.{LisMemberDO, MemberLisMappingDO}

/**
 * Cross-project integration tests for the senator LIS mapping lifecycle. Validates the data-layer sequence that spans
 * both pipelines:
 *
 *   1. Member profile pipeline processes a senator and checks for an existing LIS mapping. With none present, it skips
 *      emitting a `member-updated` event (represented here by [[DoobieMemberRepository.existsWithLisMapping]] returning
 *      `false`).
 *   1. LIS mapping refresher creates the `lis_members` row and the `member_lis_mapping` bridge row.
 *   1. On the next profile pipeline run, [[DoobieMemberRepository.existsWithLisMapping]] returns `true` — the member
 *      would now receive an event.
 *
 * These tests operate at the repository layer — they wire repositories directly rather than running through the full
 * processor/entry-point stack. This is intentional: each repository is already individually tested in its own suite;
 * here we verify the cross-table state transitions that only become visible when both pipelines interact.
 *
 * Infrastructure: shared [[repcheck.members.common.testing.SharedDockerPostgres]] AlloyDB Omni container (started once
 * per JVM, cleaned between tests via [[repcheck.members.common.testing.TransactorFixture.afterEach]]).
 */
class SenatorLifecycleIntegrationSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private val memberRepo    = new DoobieMemberRepository()
  private val lisMemberRepo = new DoobieLisMemberRepository()
  private val mappingRepo   = new DoobieLisMappingRepository()

  private def buildLisMemberDO(lisId: String): LisMemberDO = LisMemberDO(
    id = 0L,
    naturalKey = lisId,
    firstName = Some("Jane"),
    lastName = Some("Senator"),
    party = Some("Democrat"),
    state = Some("CA"),
    lastVerified = Some(Instant.parse("2024-06-15T00:00:00Z")),
    createdAt = None,
  )

  "SenatorLifecycle" should
    "show no LIS mapping for a senator before the LIS refresher has run" taggedAs DockerRequired in {
      val memberId = insertMember("A000001", firstName = "Jane", lastName = "Senator")

      val hasMapping = memberRepo.existsWithLisMapping(memberId).transact(xa).unsafeRunSync()
      hasMapping shouldBe false
    }

  it should "show LIS mapping after the LIS refresher creates lis_members and member_lis_mapping rows" taggedAs DockerRequired in {
    val memberId    = insertMember("A000002")
    val lisMemberId = lisMemberRepo.upsertByNaturalKey(buildLisMemberDO("S100")).transact(xa).unsafeRunSync()
    val _ = mappingRepo
      .upsert(MemberLisMappingDO(0L, memberId, lisMemberId, Instant.now()))
      .transact(xa)
      .unsafeRunSync()

    val hasMapping = memberRepo.existsWithLisMapping(memberId).transact(xa).unsafeRunSync()
    hasMapping shouldBe true
  }

  it should
    "flow correctly: no mapping → profile pipeline skips event → LIS refresher creates mapping → event now possible" taggedAs DockerRequired in {
      // Step 1: Profile pipeline processes senator, no mapping → would skip event emission
      val memberId     = insertMember("A000003")
      val noMappingYet = memberRepo.existsWithLisMapping(memberId).transact(xa).unsafeRunSync()

      // Step 2: LIS refresher runs, creates lis_member + mapping
      val lisMemberId = lisMemberRepo.upsertByNaturalKey(buildLisMemberDO("S200")).transact(xa).unsafeRunSync()
      val _ = mappingRepo
        .upsert(MemberLisMappingDO(0L, memberId, lisMemberId, Instant.now()))
        .transact(xa)
        .unsafeRunSync()

      // Step 3: Profile pipeline re-runs → finds mapping → would emit event
      val mappingExists = memberRepo.existsWithLisMapping(memberId).transact(xa).unsafeRunSync()

      val _ = noMappingYet shouldBe false
      mappingExists shouldBe true
    }

  it should "return Inserted on first mapping upsert and Updated on re-upsert (xmax-based detection)" taggedAs DockerRequired in {
    val memberId    = insertMember("A000004")
    val lisMemberId = lisMemberRepo.upsertByNaturalKey(buildLisMemberDO("S101")).transact(xa).unsafeRunSync()
    val mapping     = MemberLisMappingDO(0L, memberId, lisMemberId, Instant.now())

    val firstResult  = mappingRepo.upsert(mapping).transact(xa).unsafeRunSync()
    val secondResult = mappingRepo.upsert(mapping.copy(lastVerified = Instant.now())).transact(xa).unsafeRunSync()

    val _ = firstResult shouldBe UpsertResult.Inserted
    secondResult shouldBe UpsertResult.Updated
  }

  it should "support multiple senators each mapped to a distinct LIS member without FK conflicts" taggedAs DockerRequired in {
    val memberId1 = insertMember("A000005")
    val memberId2 = insertMember("A000006")

    val lisMemberId1 = lisMemberRepo.upsertByNaturalKey(buildLisMemberDO("S300")).transact(xa).unsafeRunSync()
    val lisMemberId2 = lisMemberRepo.upsertByNaturalKey(buildLisMemberDO("S301")).transact(xa).unsafeRunSync()

    val _ =
      mappingRepo.upsert(MemberLisMappingDO(0L, memberId1, lisMemberId1, Instant.now())).transact(xa).unsafeRunSync()
    val _ =
      mappingRepo.upsert(MemberLisMappingDO(0L, memberId2, lisMemberId2, Instant.now())).transact(xa).unsafeRunSync()

    val _ = memberRepo.existsWithLisMapping(memberId1).transact(xa).unsafeRunSync() shouldBe true
    memberRepo.existsWithLisMapping(memberId2).transact(xa).unsafeRunSync() shouldBe true
  }

}
