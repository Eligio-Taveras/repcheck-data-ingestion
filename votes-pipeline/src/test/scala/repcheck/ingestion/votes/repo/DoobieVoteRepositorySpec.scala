package repcheck.ingestion.votes.repo

import java.time.{Instant, LocalDate}

import cats.effect.unsafe.implicits.global

import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.vote.VoteDO
import repcheck.shared.models.congress.vote.{VoteMethod, VoteType}

/**
 * Integration tests for [[DoobieVoteRepository]] against a real AlloyDB Omni container. Covers §6.3 AC rows 1-7: upsert
 * identity, upsert update (updated_at refreshed), ON CONFLICT DO UPDATE, findByNaturalKey present/missing, findByBillId
 * list, findByCongress filter. Also covers the VoteType PG ENUM round-trip (8 variants) plus bogus-value rejection to
 * prove we get DB-level integrity rather than only app-level validation.
 */
class DoobieVoteRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieVoteRepository

  private def makeVote(
    naturalKey: String,
    congress: Int = 118,
    chamber: Chamber = Chamber.House,
    rollNumber: Int,
    billId: Option[Long] = None,
    voteType: Option[VoteType] = Some(VoteType.Passage),
    question: Option[String] = Some("On Passage"),
    voteDate: Option[LocalDate] = Some(LocalDate.parse("2024-01-15")),
    updateDate: Option[Instant] = Some(Instant.parse("2024-01-15T00:00:00Z")),
  ): VoteDO =
    VoteDO(
      voteId = 0L,
      naturalKey = naturalKey,
      congress = congress,
      chamber = chamber,
      rollNumber = rollNumber,
      sessionNumber = Some(1),
      billId = billId,
      question = question,
      voteType = voteType,
      voteMethod = Some(VoteMethod.YeaAndNay),
      result = Some("Passed"),
      voteDate = voteDate,
      legislationNumber = Some("100"),
      legislationType = Some(BillType.HR),
      legislationUrl = Some("https://www.congress.gov/bill/118th-congress/house-bill/100"),
      sourceDataUrl = Some("https://clerk.house.gov/Votes/2024/roll1.xml"),
      updateDate = updateDate,
      createdAt = None,
      updatedAt = None,
    )

  // AC#1
  "upsert" should "insert a new vote and return the row with the auto-generated id" taggedAs DockerRequired in {
    val vote     = makeVote("house:118:1:17", rollNumber = 17)
    val inserted = repo.upsert(vote).transact(xa).unsafeRunSync()

    val _ = inserted.voteId should be > 0L
    val _ = inserted.naturalKey shouldBe vote.naturalKey
    val _ = inserted.question shouldBe vote.question
    inserted.voteType shouldBe Some(VoteType.Passage)
  }

  // AC#2
  it should "refresh updated_at when the same natural key is upserted again" taggedAs DockerRequired in {
    val vote  = makeVote("house:118:1:18", rollNumber = 18)
    val first = repo.upsert(vote).transact(xa).unsafeRunSync()
    Thread.sleep(10) // ensure NOW() moves forward
    val second = repo.upsert(vote.copy(question = Some("Different question"))).transact(xa).unsafeRunSync()

    val _         = second.voteId shouldBe first.voteId // same row
    val _         = second.question shouldBe Some("Different question")
    val firstTs   = first.updatedAt.getOrElse(Instant.EPOCH)
    val secondTs  = second.updatedAt.getOrElse(Instant.EPOCH)
    val _         = secondTs.isAfter(firstTs) shouldBe true
    val createdTs = second.createdAt.getOrElse(Instant.EPOCH)
    firstTs.isBefore(createdTs) shouldBe false // created_at unchanged across upserts
  }

  // AC#3
  it should "use ON CONFLICT DO UPDATE so later upserts overwrite earlier fields" taggedAs DockerRequired in {
    val first     = makeVote("house:118:1:19", rollNumber = 19, voteType = Some(VoteType.Passage))
    val insertedA = repo.upsert(first).transact(xa).unsafeRunSync()
    val conflictB = first.copy(voteType = Some(VoteType.Cloture), question = Some("On Cloture"))
    val insertedB = repo.upsert(conflictB).transact(xa).unsafeRunSync()

    val _ = insertedB.voteId shouldBe insertedA.voteId
    val _ = insertedB.voteType shouldBe Some(VoteType.Cloture)
    insertedB.question shouldBe Some("On Cloture")
  }

  // AC#4
  "findByNaturalKey" should "return the vote when it exists" taggedAs DockerRequired in {
    val vote     = makeVote("house:118:1:20", rollNumber = 20)
    val inserted = repo.upsert(vote).transact(xa).unsafeRunSync()
    val found    = repo.findByNaturalKey("house:118:1:20").transact(xa).unsafeRunSync()

    found match {
      case Some(row) =>
        val _ = row.voteId shouldBe inserted.voteId
        row.naturalKey shouldBe "house:118:1:20"
      case None => sys.error("Expected vote to be found")
    }
  }

  // AC#5
  it should "return None when no vote matches the natural key" taggedAs DockerRequired in {
    val found = repo.findByNaturalKey("senate:999:9:999").transact(xa).unsafeRunSync()
    found shouldBe None
  }

  // AC#6
  "findByBillId" should "return every vote tied to the same bill" taggedAs DockerRequired in {
    val billId = insertBill(billNumber = 200)
    val _ = repo.upsert(makeVote("house:118:1:21", rollNumber = 21, billId = Some(billId))).transact(xa).unsafeRunSync()
    val _ = repo.upsert(makeVote("house:118:1:22", rollNumber = 22, billId = Some(billId))).transact(xa).unsafeRunSync()
    val _ = repo.upsert(makeVote("house:118:1:23", rollNumber = 23, billId = Some(billId))).transact(xa).unsafeRunSync()

    val found = repo.findByBillId(billId).transact(xa).unsafeRunSync()
    val _     = found.size shouldBe 3
    found.map(_.rollNumber).sorted shouldBe List(21, 22, 23)
  }

  // AC#7
  "findByCongress" should "return votes filtered by congress number" taggedAs DockerRequired in {
    val _ = repo.upsert(makeVote("house:118:1:24", congress = 118, rollNumber = 24)).transact(xa).unsafeRunSync()
    val _ = repo
      .upsert(makeVote("senate:118:1:25", congress = 118, chamber = Chamber.Senate, rollNumber = 25))
      .transact(xa)
      .unsafeRunSync()
    val _ = repo.upsert(makeVote("house:117:1:26", congress = 117, rollNumber = 26)).transact(xa).unsafeRunSync()

    val found118 = repo.findByCongress(118).transact(xa).unsafeRunSync()
    val _        = found118.size shouldBe 2
    found118.map(_.rollNumber).sorted shouldBe List(24, 25)
  }

  // VoteType PG ENUM round-trip (all 8 variants) — depends on P0.1 / shared-models 0.1.28
  "VoteType PG ENUM round-trip" should "persist and restore every variant without loss" taggedAs DockerRequired in {
    val allVariants = VoteType.values.toList
    allVariants.zipWithIndex.foreach {
      case (vt, idx) =>
        val nk = s"house:118:1:${1000 + idx}"
        val inserted =
          repo.upsert(makeVote(nk, rollNumber = 1000 + idx, voteType = Some(vt))).transact(xa).unsafeRunSync()
        val _ = inserted.voteType shouldBe Some(vt)
    }
  }

  it should "reject a bogus vote_type_enum value at the database level" taggedAs DockerRequired in {
    val attempt = scala.util.Try {
      sql"""INSERT INTO votes (
              natural_key, congress, chamber, roll_number, vote_type, update_date
            ) VALUES (
              ${"house:118:1:9999"}, 118, ${Chamber.House.apiValue}::chamber_type, 9999, 'NotARealType'::vote_type_enum,
              ${Instant.parse("2024-01-15T00:00:00Z")}
            )""".update.run.transact(xa).unsafeRunSync()
    }
    attempt.isFailure shouldBe true
  }

}
