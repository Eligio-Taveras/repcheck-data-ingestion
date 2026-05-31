package repcheck.ingestion.bills.common.persistence

import java.time.LocalDate

import cats.effect.unsafe.implicits.global

import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.bill.TextVersionCode

/**
 * AlloyDB Omni-backed integration spec for [[DoobieBillSummaryRepository]]. Tagged `DockerRequired`; the upsert
 * resolves `bill_id` via an `INSERT ... SELECT` subquery and binds the `text_version_code_type` enum + `date`, none of
 * which H2 runs. The fixture's `DELETE FROM bills` cascades to `bill_summaries` (FK ON DELETE CASCADE), so no extra
 * cleanup.
 */
class DoobieBillSummaryRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieBillSummaryRepository

  private def insertBill(number: Int): Unit = {
    val _ =
      sql"INSERT INTO bills (congress, bill_type, number) VALUES (118, 'hr'::bill_type_enum, $number) ON CONFLICT DO NOTHING".update.run
        .transact(xa)
        .unsafeRunSync()
  }

  private def readSummary(number: Int): Option[(String, Option[String], Option[LocalDate])] =
    sql"""SELECT bs.text, bs.action_desc, bs.action_date
          FROM bill_summaries bs JOIN bills b ON b.id = bs.bill_id
          WHERE b.congress = 118 AND b.bill_type::text = 'hr' AND b.number = $number"""
      .query[(String, Option[String], Option[LocalDate])]
      .option
      .transact(xa)
      .unsafeRunSync()

  "upsert" should "resolve bill_id and store the summary" taggedAs DockerRequired in {
    insertBill(9200)
    val _ = repo
      .upsert("118-HR-9200", TextVersionCode.IH, "body", Some("desc"), Some(LocalDate.parse("2024-01-15")))
      .transact(xa)
      .unsafeRunSync()
    readSummary(9200) shouldBe Some(("body", Some("desc"), Some(LocalDate.parse("2024-01-15"))))
  }

  it should "be idempotent on (bill_id, version_code), refreshing text" taggedAs DockerRequired in {
    insertBill(9201)
    val _ = repo.upsert("118-HR-9201", TextVersionCode.IH, "v1", None, None).transact(xa).unsafeRunSync()
    val _ = repo.upsert("118-HR-9201", TextVersionCode.IH, "v2", None, None).transact(xa).unsafeRunSync()

    val count = sql"SELECT count(*) FROM bill_summaries bs JOIN bills b ON b.id = bs.bill_id WHERE b.number = 9201"
      .query[Int]
      .unique
      .transact(xa)
      .unsafeRunSync()
    val _ = count shouldBe 1
    readSummary(9201).map(_._1) shouldBe Some("v2")
  }

  it should "no-op when the bill does not exist" taggedAs DockerRequired in {
    val _ = repo.upsert("118-HR-9299", TextVersionCode.IH, "body", None, None).transact(xa).unsafeRunSync()
    readSummary(9299) shouldBe None
  }

}
