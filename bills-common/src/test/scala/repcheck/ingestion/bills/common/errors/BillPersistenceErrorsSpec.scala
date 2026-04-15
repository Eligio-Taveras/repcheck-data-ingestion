package repcheck.ingestion.bills.common.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillPersistenceErrorsSpec extends AnyFlatSpec with Matchers {

  private val cause = new RuntimeException("db error")

  "BillUpsertFailed" should "format message with billId and detail" in {
    val err = BillUpsertFailed("118-HR-1", "connection timeout", cause)
    err.getMessage shouldBe "Failed to upsert bill 118-HR-1: connection timeout"
  }

  it should "preserve the cause" in {
    BillUpsertFailed("118-HR-1", "connection timeout", cause).getCause shouldBe cause
  }

  it should "expose billId field" in {
    BillUpsertFailed("118-HR-1", "timeout", cause).billId shouldBe "118-HR-1"
  }

  it should "expose detail field" in {
    BillUpsertFailed("118-HR-1", "timeout", cause).detail shouldBe "timeout"
  }

  it should "be an Exception subclass" in {
    BillUpsertFailed("x", "y", cause) shouldBe a[Exception]
  }

  "BillArchiveFailed" should "format message with billId and detail" in {
    val err = BillArchiveFailed("118-HR-1", "FK violation", cause)
    err.getMessage shouldBe "Failed to archive bill history for 118-HR-1: FK violation"
  }

  it should "preserve the cause" in {
    BillArchiveFailed("118-HR-1", "FK violation", cause).getCause shouldBe cause
  }

  it should "expose billId field" in {
    BillArchiveFailed("118-HR-1", "FK violation", cause).billId shouldBe "118-HR-1"
  }

  it should "expose detail field" in {
    BillArchiveFailed("118-HR-1", "FK violation", cause).detail shouldBe "FK violation"
  }

  it should "be an Exception subclass" in {
    BillArchiveFailed("x", "y", cause) shouldBe a[Exception]
  }

  "BillCosponsorReplaceFailed" should "format message with billId and detail" in {
    val err = BillCosponsorReplaceFailed(42L, "batch insert failed", cause)
    err.getMessage shouldBe "Failed to replace cosponsors for bill 42: batch insert failed"
  }

  it should "preserve the cause" in {
    BillCosponsorReplaceFailed(42L, "batch insert failed", cause).getCause shouldBe cause
  }

  it should "expose billId field" in {
    BillCosponsorReplaceFailed(42L, "batch insert failed", cause).billId shouldBe 42L
  }

  it should "expose detail field" in {
    BillCosponsorReplaceFailed(42L, "batch insert failed", cause).detail shouldBe "batch insert failed"
  }

  it should "be an Exception subclass" in {
    BillCosponsorReplaceFailed(1L, "y", cause) shouldBe a[Exception]
  }

  "BillSubjectReplaceFailed" should "format message with billId and detail" in {
    val err = BillSubjectReplaceFailed(42L, "delete failed", cause)
    err.getMessage shouldBe "Failed to replace subjects for bill 42: delete failed"
  }

  it should "preserve the cause" in {
    BillSubjectReplaceFailed(42L, "delete failed", cause).getCause shouldBe cause
  }

  it should "expose billId field" in {
    BillSubjectReplaceFailed(42L, "delete failed", cause).billId shouldBe 42L
  }

  it should "expose detail field" in {
    BillSubjectReplaceFailed(42L, "delete failed", cause).detail shouldBe "delete failed"
  }

  it should "be an Exception subclass" in {
    BillSubjectReplaceFailed(1L, "y", cause) shouldBe a[Exception]
  }

  "BillTextVersionInsertFailed" should "format message with billId and detail" in {
    val err = BillTextVersionInsertFailed(42L, "unique constraint", cause)
    err.getMessage shouldBe "Failed to insert text version for bill 42: unique constraint"
  }

  it should "preserve the cause" in {
    BillTextVersionInsertFailed(42L, "unique constraint", cause).getCause shouldBe cause
  }

  it should "expose billId field" in {
    BillTextVersionInsertFailed(42L, "unique constraint", cause).billId shouldBe 42L
  }

  it should "expose detail field" in {
    BillTextVersionInsertFailed(42L, "unique constraint", cause).detail shouldBe "unique constraint"
  }

  it should "be an Exception subclass" in {
    BillTextVersionInsertFailed(1L, "y", cause) shouldBe a[Exception]
  }

}
