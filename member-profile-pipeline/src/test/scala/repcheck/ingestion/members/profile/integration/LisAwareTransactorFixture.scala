package repcheck.ingestion.members.profile.integration

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.Suite
import repcheck.members.common.testing.TransactorFixture

/**
 * Extends [[TransactorFixture]] from members-common with additional cleanup for the two LIS-scoped tables
 * (`member_lis_mapping`, `lis_members`) that lie outside members-common's normal cleanup scope.
 *
 * Cleanup order matters for FK integrity: `member_lis_mapping` must be deleted before `members` (handled by
 * `super.afterEach()`) because it holds a `member_id` FK to `members.id`. `lis_members` is cleaned here alongside it
 * for symmetry.
 *
 * The `xa` transactor, `insertMember` helper, and AlloyDB Omni container bootstrap are all inherited from
 * [[TransactorFixture]] (available via the `membersCommon % "test->test"` SBT dependency). This trait only adds the LIS
 * table cleanup layer, matching the pattern used in `lis-mapping-refresher`'s own
 * [[repcheck.members.lismapping.testing.TransactorFixture]].
 */
trait LisAwareTransactorFixture extends TransactorFixture { self: Suite =>

  override def afterEach(): Unit = {
    val _ = sql"""
      DELETE FROM member_lis_mapping;
      DELETE FROM lis_members;
    """.update.run.transact(xa).unsafeRunSync()
    super.afterEach()
  }

}
