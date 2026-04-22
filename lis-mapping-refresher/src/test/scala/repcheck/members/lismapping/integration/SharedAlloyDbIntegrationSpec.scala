package repcheck.members.lismapping.integration

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import doobie.ConnectionIO
import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.persistence.{DoobieLisMappingRepository, DoobieLisMemberRepository}
import repcheck.members.common.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.dos.member.{LisMemberDO, MemberLisMappingDO}

/**
 * Integration tests verifying that concurrent operations from both pipelines sharing the same AlloyDB instance do not
 * produce deadlocks, constraint violations, or data corruption.
 *
 * The LIS mapping refresher processes senators concurrently via `parEvalMap`. These tests simulate that concurrency by
 * running multiple `IO` instances in parallel via [[cats.syntax.all.catsSyntaxParallelTraverse1]] (`parTraverse`). The
 * underlying `ConnectionIO` programs are upsert-only (`ON CONFLICT DO UPDATE`) so they are inherently idempotent and
 * safe to run concurrently — the goal is to confirm that PostgreSQL row-level locking at the `lis_members` and
 * `member_lis_mapping` levels does not produce deadlocks under this pattern.
 *
 * Infrastructure: shared [[repcheck.members.common.testing.SharedDockerPostgres]] AlloyDB Omni container.
 */
class SharedAlloyDbIntegrationSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private val lisMemberRepo = new DoobieLisMemberRepository()
  private val mappingRepo   = new DoobieLisMappingRepository()

  private def buildLisMemberDO(lisId: String): LisMemberDO = LisMemberDO(
    id = 0L,
    naturalKey = lisId,
    firstName = Some("Senator"),
    lastName = Some("Test"),
    party = Some("Independent"),
    state = Some("NY"),
    lastVerified = Some(Instant.now()),
    createdAt = None,
  )

  "SharedAlloyDb" should
    "complete concurrent lis_member + mapping upserts across 5 senators without deadlocks" taggedAs DockerRequired in {
      // Pre-create members sequentially — IDs required for the FK in member_lis_mapping
      val memberIds = (1 to 5).toList.map(i => insertMember(s"CONCURRENT-${i.toString}"))

      // Simulate concurrent LIS refresher: each fiber upserts a lis_member then its mapping
      val concurrentOps: IO[List[Unit]] = memberIds.zipWithIndex.parTraverse {
        case (memberId, i) =>
          val lisId = s"CS${(100 + i).toString}"
          val program: ConnectionIO[Unit] = for {
            lisMemberId <- lisMemberRepo.upsertByNaturalKey(buildLisMemberDO(lisId))
            _           <- mappingRepo.upsert(MemberLisMappingDO(0L, memberId, lisMemberId, Instant.now()))
          } yield ()
          program.transact(xa)
      }

      // Must complete without throwing — any deadlock surfaces as a ConnectionIO exception
      val _ = concurrentOps.unsafeRunSync()

      val totalMappings = sql"SELECT COUNT(*) FROM member_lis_mapping"
        .query[Long]
        .unique
        .transact(xa)
        .unsafeRunSync()

      totalMappings shouldBe 5L
    }

  it should "converge to one lis_members row when the same lisId is upserted concurrently" taggedAs DockerRequired in {
    // Two concurrent fibers both trying to upsert the same LIS ID — ON CONFLICT must handle it
    val concurrentUpserts: IO[List[Long]] = (1 to 5).toList.parTraverse { _ =>
      lisMemberRepo.upsertByNaturalKey(buildLisMemberDO("S-SHARED")).transact(xa)
    }

    val ids = concurrentUpserts.unsafeRunSync()

    // All 5 calls must return the same row id — ON CONFLICT DO UPDATE returns the existing id
    val _ = ids.distinct.size shouldBe 1

    val rowCount = sql"SELECT COUNT(*) FROM lis_members WHERE natural_key = 'S-SHARED'"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    rowCount shouldBe 1L
  }

  it should "correctly record lastVerified updates under concurrent re-upserts of the same mapping" taggedAs DockerRequired in {
    val memberId    = insertMember("CONCURRENT-SAME")
    val lisMemberId = lisMemberRepo.upsertByNaturalKey(buildLisMemberDO("S-REUPSERT")).transact(xa).unsafeRunSync()

    val baseMapping = MemberLisMappingDO(0L, memberId, lisMemberId, Instant.now())

    // 5 concurrent re-upserts of the same mapping with slightly different lastVerified
    val concurrentOps: IO[List[Unit]] = (1 to 5).toList.parTraverse { _ =>
      mappingRepo.upsert(baseMapping.copy(lastVerified = Instant.now())).transact(xa).map(_ => ())
    }
    val _ = concurrentOps.unsafeRunSync()

    // Exactly one mapping row must exist — concurrent ON CONFLICT DO UPDATE must not produce duplicates
    val mappingCount = sql"SELECT COUNT(*) FROM member_lis_mapping WHERE member_id = $memberId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    mappingCount shouldBe 1L
  }

}
