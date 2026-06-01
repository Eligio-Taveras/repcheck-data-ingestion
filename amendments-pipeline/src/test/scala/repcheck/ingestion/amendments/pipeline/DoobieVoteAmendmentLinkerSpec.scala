package repcheck.ingestion.amendments.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.testing.TransactorFixture
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.common.testing.DockerRequired

/**
 * AlloyDB Omni-backed integration spec for [[DoobieVoteAmendmentLinker]]. Tagged `DockerRequired`; the reconciliation
 * SQL uses POSIX `regexp_match` + a window function that H2 can't run, so this is the only place the link statement is
 * exercised against real Postgres. Test rows use out-of-the-way roll numbers and a fake congress to stay clear of any
 * seed data the shared container may hold; `afterEach` clears `votes` before the fixture clears amendments/bills.
 */
class DoobieVoteAmendmentLinkerSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private val silentLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private lazy val linker = new DoobieVoteAmendmentLinker[IO](xa, silentLogger)
  private val ctx         = LogContext("test", "vote-amendment-linker")

  private def insertAmendment(
    naturalKey: String,
    congress: Int,
    amendmentType: String,
    number: String,
    chamber: String,
    action: String,
  ): Long =
    sql"""INSERT INTO amendments (natural_key, congress, amendment_type, number, chamber, latest_action_text)
          VALUES ($naturalKey, $congress, $amendmentType::amendment_type_enum, $number, $chamber::chamber_type, $action)
          RETURNING id""".query[Long].unique.transact(xa).unsafeRunSync()

  private def insertVote(naturalKey: String, congress: Int, chamber: String, roll: Int): Long =
    sql"""INSERT INTO votes (natural_key, congress, chamber, roll_number, session_number)
          VALUES ($naturalKey, $congress, $chamber::chamber_type, $roll, 1)
          RETURNING id""".query[Long].unique.transact(xa).unsafeRunSync()

  private def voteAmendmentId(voteId: Long): Option[Long] =
    sql"SELECT amendment_id FROM votes WHERE id = $voteId".query[Option[Long]].unique.transact(xa).unsafeRunSync()

  private def voteLegType(voteId: Long): Option[String] =
    sql"SELECT legislation_type::text FROM votes WHERE id = $voteId"
      .query[Option[String]]
      .unique
      .transact(xa)
      .unsafeRunSync()

  override def afterEach(): Unit = {
    val _ = sql"DELETE FROM votes".update.run.transact(xa).unsafeRunSync()
    super.afterEach()
  }

  "linkAll" should "link a House vote to its amendment via 'Roll no. N'" taggedAs DockerRequired in {
    val aid = insertAmendment(
      "118-HAMDT-9001",
      118,
      "hamdt",
      "9001",
      "House",
      "On agreeing to the Test amendment (A001) Agreed to by recorded vote: 1 - 0 (Roll no. 901).",
    )
    val vid = insertVote("118-House-1-901", 118, "House", 901)

    val _ = linker.linkAll(ctx).unsafeRunSync()

    val _ = voteAmendmentId(vid) shouldBe Some(aid)
    voteLegType(vid) shouldBe Some("AMENDMENT")
  }

  it should "link a Senate vote via 'Record Vote Number: N'" taggedAs DockerRequired in {
    val aid = insertAmendment(
      "118-SAMDT-9002",
      118,
      "samdt",
      "9002",
      "Senate",
      "Amendment SA 9002 agreed to in Senate by Yea-Nay Vote. 1 - 0. Record Vote Number: 902.",
    )
    val vid = insertVote("118-Senate-1-902", 118, "Senate", 902)

    val _ = linker.linkAll(ctx).unsafeRunSync()

    voteAmendmentId(vid) shouldBe Some(aid)
  }

  it should "skip en-bloc votes (one roll, multiple amendments)" taggedAs DockerRequired in {
    val _ = insertAmendment(
      "118-HAMDT-9003",
      118,
      "hamdt",
      "9003",
      "House",
      "On agreeing to the en bloc amendments Agreed to by recorded vote (Roll no. 903).",
    )
    val _ = insertAmendment(
      "118-HAMDT-9004",
      118,
      "hamdt",
      "9004",
      "House",
      "On agreeing to the en bloc amendments Agreed to by recorded vote (Roll no. 903).",
    )
    val vid = insertVote("118-House-1-903", 118, "House", 903)

    val _ = linker.linkAll(ctx).unsafeRunSync()

    voteAmendmentId(vid) shouldBe None
  }

  it should "be idempotent (second pass updates nothing for an already-linked vote)" taggedAs DockerRequired in {
    val _ = insertAmendment(
      "118-HAMDT-9005",
      118,
      "hamdt",
      "9005",
      "House",
      "On agreeing to the Test amendment Agreed to by recorded vote (Roll no. 905).",
    )
    val _ = insertVote("118-House-1-905", 118, "House", 905)

    val first  = linker.linkAll(ctx).unsafeRunSync()
    val second = linker.linkAll(ctx).unsafeRunSync()

    val _ = first should be >= 1
    second shouldBe 0
  }

}
