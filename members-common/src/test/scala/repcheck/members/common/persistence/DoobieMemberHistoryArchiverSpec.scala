package repcheck.members.common.persistence

import cats.effect.unsafe.implicits.global

import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.common.{Chamber, UsState}
import repcheck.shared.models.congress.dos.member.MemberTermDO
import repcheck.shared.models.congress.member.MemberType

class DoobieMemberHistoryArchiverSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val archiver = new DoobieMemberHistoryArchiver
  private lazy val termRepo = new DoobieMemberTermRepository

  private def seedMemberWithTerms(bioguideId: String): Long = {
    val memberId = insertMember(bioguideId)
    val terms = List(
      MemberTermDO(
        termId = 0L,
        memberId = memberId,
        chamber = Some(Chamber.House),
        congress = Some(117),
        startYear = Some(2021),
        endYear = Some(2023),
        memberType = Some(MemberType.Representative),
        stateCode = Some(UsState.NewYork),
        stateName = Some("New York"),
        district = Some(5),
      ),
      MemberTermDO(
        termId = 0L,
        memberId = memberId,
        chamber = Some(Chamber.House),
        congress = Some(118),
        startYear = Some(2023),
        endYear = None,
        memberType = Some(MemberType.Representative),
        stateCode = Some(UsState.NewYork),
        stateName = Some("New York"),
        district = Some(5),
      ),
    )
    termRepo.replaceAll(memberId, terms).transact(xa).unsafeRunSync()
    memberId
  }

  "archiveMember" should "copy the members row into member_history" taggedAs DockerRequired in {
    val memberId = seedMemberWithTerms("A000001")
    archiver.archiveMember("A000001").transact(xa).unsafeRunSync()

    val historyCount = sql"SELECT COUNT(*) FROM member_history WHERE member_id = $memberId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
    historyCount shouldBe 1L
  }

  it should "copy current term rows into member_term_history" taggedAs DockerRequired in {
    val memberId = seedMemberWithTerms("A000002")
    archiver.archiveMember("A000002").transact(xa).unsafeRunSync()

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

  it should "link member_history row and its term_history rows via history_id" taggedAs DockerRequired in {
    val memberId = seedMemberWithTerms("A000003")
    archiver.archiveMember("A000003").transact(xa).unsafeRunSync()

    val historyId = sql"SELECT id FROM member_history WHERE member_id = $memberId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

    val distinct = sql"SELECT DISTINCT history_id FROM member_term_history WHERE history_id = $historyId"
      .query[Long]
      .to[List]
      .transact(xa)
      .unsafeRunSync()
    distinct should contain only historyId
  }

  it should "archive a member with no terms (term_history is empty)" taggedAs DockerRequired in {
    val memberId = insertMember("A000004")
    archiver.archiveMember("A000004").transact(xa).unsafeRunSync()

    val historyCount = sql"SELECT COUNT(*) FROM member_history WHERE member_id = $memberId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
    val termHistoryCount = sql"""
      SELECT COUNT(*) FROM member_term_history WHERE history_id IN
      (SELECT id FROM member_history WHERE member_id = $memberId)
    """.query[Long].unique.transact(xa).unsafeRunSync()

    val _ = historyCount shouldBe 1L
    termHistoryCount shouldBe 0L
  }

  it should "produce distinct history_id values on repeated archives" taggedAs DockerRequired in {
    val _ = seedMemberWithTerms("A000005")
    archiver.archiveMember("A000005").transact(xa).unsafeRunSync()
    archiver.archiveMember("A000005").transact(xa).unsafeRunSync()

    val ids = sql"""SELECT id FROM member_history
                    WHERE member_id = (SELECT id FROM members WHERE natural_key = 'A000005')"""
      .query[Long]
      .to[List]
      .transact(xa)
      .unsafeRunSync()
    val _ = ids.size shouldBe 2
    ids.distinct.size shouldBe 2
  }

  it should "be a no-op for a non-existent bioguide id (no rows inserted)" taggedAs DockerRequired in {
    archiver.archiveMember("NOPE-9999").transact(xa).unsafeRunSync()

    val historyCount = sql"SELECT COUNT(*) FROM member_history"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
    historyCount shouldBe 0L
  }

  // Placeholder transitions get archived alongside real-state transitions — every member-state change
  // is a meaningful audit event, including the moment we first learn about a member via a sponsorship
  // reference and then enrich them. Earlier this archiver short-circuited placeholders via
  // `WHERE update_date IS NOT NULL` in the existence query; that hid the most interesting transition
  // in a member's lifecycle from the audit log. Mirrors `BillHistoryArchiver` post-migration-034.
  // Schema requirement: `member_history.{update_date, first_name, last_name}` must be nullable
  // (companion db-migrations PR — migration 035 — extends migration 034's pattern from bills to
  // members).
  it should "archive a placeholder member (writes one history row with all profile fields NULL)" taggedAs DockerRequired in {
    val _ = sql"""INSERT INTO members (natural_key) VALUES ('PLACEHOLDER-001')""".update.run
      .transact(xa)
      .unsafeRunSync()

    archiver.archiveMember("PLACEHOLDER-001").transact(xa).unsafeRunSync()

    val historyCount = sql"SELECT COUNT(*) FROM member_history"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
    val _ = historyCount shouldBe 1L

    // The archived row should mirror the placeholder's NULL-ness: every profile field is None,
    // including update_date. Locks the contract — if the writer accidentally starts coercing
    // NULLs into `''` or NOW() (the bug we just fixed in BillRepository.upsertPlaceholder), this
    // assertion catches it.
    val snapshot = sql"""SELECT mh.first_name, mh.last_name, mh.current_party, mh.update_date
                         FROM member_history mh
                         JOIN members m ON m.id = mh.member_id
                         WHERE m.natural_key = 'PLACEHOLDER-001'"""
      .query[(Option[String], Option[String], Option[String], Option[java.time.Instant])]
      .unique
      .transact(xa)
      .unsafeRunSync()
    snapshot shouldBe ((None, None, None, None))
  }

}
