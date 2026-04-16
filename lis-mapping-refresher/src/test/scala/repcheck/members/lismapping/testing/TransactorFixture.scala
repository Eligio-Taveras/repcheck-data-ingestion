package repcheck.members.lismapping.testing

import doobie.implicits._

import org.scalatest.Suite

/**
 * Lis-mapping-specific layer over [[repcheck.members.common.testing.TransactorFixture]] (pulled in via
 * `lisMappingRefresher.dependsOn(membersCommon % "test->test")`). All the Docker/container bootstrap (`DockerPostgres`,
 * `SharedDockerPostgres`, `PostgresContainerInfo`) and the `xa` + `insertMember` helpers are inherited from the
 * members-common fixture — this trait only adds cleanup of the two LIS-scoped tables (`member_lis_mapping`,
 * `lis_members`) that are foreign to members-common's cleanup list.
 *
 * Cleanup order matters: `member_lis_mapping` must be deleted before `members` (super.afterEach) because of the FK to
 * `members.id`, and `lis_members` is cleaned here alongside it for symmetry.
 */
trait TransactorFixture extends repcheck.members.common.testing.TransactorFixture { self: Suite =>

  import cats.effect.unsafe.implicits.global

  override def afterEach(): Unit = {
    val _ = sql"""
      DELETE FROM member_lis_mapping;
      DELETE FROM lis_members;
    """.update.run.transact(xa).unsafeRunSync()
    super.afterEach()
  }

}
