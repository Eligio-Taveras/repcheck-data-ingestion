package repcheck.ingestion.votes.app

import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.testing.TransactorFixture
import repcheck.members.common.testing.DockerRequired
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Integration spec for [[DoobieBillPlaceholderRepository]] against DockerPostgres. Verifies the SQL shape (column list,
 * ON CONFLICT target) and the idempotency contract: repeated inserts for the same natural key must not produce a
 * duplicate row, and distinct keys must produce distinct rows.
 *
 * The parse-failure paths are fully covered by the pure unit spec in `DoobieBillPlaceholderRepositorySpec`; this file
 * focuses exclusively on the paths that need a real Postgres instance (ON CONFLICT, enum serialization, composite-key
 * uniqueness).
 */
class DoobieBillPlaceholderRepositoryIntegrationSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieBillPlaceholderRepository[cats.effect.IO](xa)

  private def countBillsByNaturalKey(congress: Int, billType: BillType, number: Int): Int =
    sql"""SELECT COUNT(*) FROM bills
          WHERE congress = $congress AND bill_type = $billType AND number = $number"""
      .query[Int]
      .unique
      .transact(xa)
      .unsafeRunSync()

  private def loadBill(congress: Int, billType: BillType, number: Int): (Int, BillType, Int, String) =
    sql"""SELECT congress, bill_type, number, title FROM bills
          WHERE congress = $congress AND bill_type = $billType AND number = $number"""
      .query[(Int, BillType, Int, String)]
      .unique
      .transact(xa)
      .unsafeRunSync()

  "insertIfNotExists" should "insert a new bills row with parsed composite key" taggedAs DockerRequired in {
    val entity = BillDO.hasPlaceholder.placeholder("119-HR-30")
    repo.insertIfNotExists(entity).unsafeRunSync()

    val _                                   = countBillsByNaturalKey(119, BillType.HR, 30) shouldBe 1
    val (congress, billType, number, title) = loadBill(119, BillType.HR, 30)
    val _                                   = congress shouldBe 119
    val _                                   = billType shouldBe BillType.HR
    val _                                   = number shouldBe 30
    // Placeholder title is intentionally an empty string per the class docstring — bills-pipeline overwrites it later.
    title shouldBe ""
  }

  it should "be idempotent when the same natural key is inserted twice" taggedAs DockerRequired in {
    val entity = BillDO.hasPlaceholder.placeholder("119-S-42")
    val _      = repo.insertIfNotExists(entity).unsafeRunSync()
    repo.insertIfNotExists(entity).unsafeRunSync()
    countBillsByNaturalKey(119, BillType.S, 42) shouldBe 1
  }

  it should "insert distinct rows for distinct natural keys" taggedAs DockerRequired in {
    val hr30 = BillDO.hasPlaceholder.placeholder("119-HR-30")
    val s42  = BillDO.hasPlaceholder.placeholder("119-S-42")
    val hr31 = BillDO.hasPlaceholder.placeholder("119-HR-31")

    val _ = repo.insertIfNotExists(hr30).unsafeRunSync()
    val _ = repo.insertIfNotExists(s42).unsafeRunSync()
    repo.insertIfNotExists(hr31).unsafeRunSync()

    val _ = countBillsByNaturalKey(119, BillType.HR, 30) shouldBe 1
    val _ = countBillsByNaturalKey(119, BillType.S, 42) shouldBe 1
    countBillsByNaturalKey(119, BillType.HR, 31) shouldBe 1
  }

  it should "lowercase the bill_type enum value when written to the database (matches findByBillId's comparison)" taggedAs DockerRequired in {
    val entity = BillDO.hasPlaceholder.placeholder("119-HJRES-5")
    repo.insertIfNotExists(entity).unsafeRunSync()

    // The Doobie Put[BillType] serializes BillType.HJRES to its apiValue "hjres" (lowercase) — that's what the
    // bill_type_enum PG type expects. DoobieBillRepository.findByBillId lowercases its parsed bill_type before
    // comparing, so writer + reader agree here.
    val rawBillType = sql"""SELECT bill_type::text FROM bills
                            WHERE congress = 119 AND number = 5"""
      .query[String]
      .unique
      .transact(xa)
      .unsafeRunSync()
    rawBillType shouldBe "hjres"
  }

}
