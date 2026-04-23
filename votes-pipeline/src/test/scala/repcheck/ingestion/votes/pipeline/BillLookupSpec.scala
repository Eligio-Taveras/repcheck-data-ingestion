package repcheck.ingestion.votes.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.BillResolutionFailed
import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Unit spec for [[BillLookup]]. Verifies the `upsertPlaceholder` + `findByBillId` composition and the defensive
 * [[BillResolutionFailed]] path when the lookup returns None immediately after an idempotent insert.
 *
 * The bills-common-owned placeholder path (`BillRepository.upsertPlaceholder`) is stubbed at the trait boundary so this
 * spec only exercises `BillLookup`'s sequencing. The real SQL — parse + composite-key ON CONFLICT — is covered by
 * `bills-common/.../DoobieBillRepositorySpec`.
 */
class BillLookupSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:billlookup;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private def mkLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    m
  }

  /**
   * Construct a `BillRepository` mock with `upsertPlaceholder` already stubbed to a no-op. Callers override
   * `findByBillId` per test case.
   */
  private def mkBillRepo(): BillRepository[ConnectionIO] = {
    val repo = mock[BillRepository[ConnectionIO]]
    when(repo.upsertPlaceholder(anyString())).thenReturn(connection.unit)
    repo
  }

  private def mkLookup(billRepo: BillRepository[ConnectionIO], logger: PipelineLogger[IO] = mkLogger): BillLookup[IO] =
    new BillLookup[IO](billRepo = billRepo, xa = testXa, logger = logger)

  private def billDOMock(id: Long): BillDO = {
    val b = mock[BillDO]
    when(b.billId).thenReturn(id)
    b
  }

  "forContext(logCtx).apply(nk)" should "resolve to Some(billId) after upsertPlaceholder + findByBillId succeed" in {
    val billRepo = mkBillRepo()
    // Finish the BillDO mock's stubbing BEFORE invoking when(billRepo.findByBillId) — Mockito cannot tolerate nested
    // in-progress stubbing (the `when(bill.billId).thenReturn(id)` inside billDOMock would clash with the outer `when`).
    val bill = billDOMock(404L)
    when(billRepo.findByBillId(eqTo("119-HR-1234"))).thenReturn(connection.pure(Some(bill)))

    val lookup = mkLookup(billRepo).forContext(LogContext("r", "s"))
    lookup("119-HR-1234").unsafeRunSync() shouldBe Some(404L)
  }

  it should "call upsertPlaceholder before findByBillId (ordering invariant)" in {
    val billRepo = mkBillRepo()
    val bill     = billDOMock(7L)
    when(billRepo.findByBillId(eqTo("119-HR-1234"))).thenReturn(connection.pure(Some(bill)))

    val lookup = mkLookup(billRepo).forContext(LogContext("r", "s"))
    val _      = lookup("119-HR-1234").unsafeRunSync()

    val inOrder = org.mockito.Mockito.inOrder(billRepo)
    val _       = inOrder.verify(billRepo).upsertPlaceholder(eqTo("119-HR-1234"))
    inOrder.verify(billRepo).findByBillId(eqTo("119-HR-1234"))
  }

  it should "raise BillResolutionFailed when findByBillId returns None after upsertPlaceholder" in {
    val billRepo = mkBillRepo()
    when(billRepo.findByBillId(eqTo("119-HR-1234"))).thenReturn(connection.pure(Option.empty[BillDO]))

    val lookup  = mkLookup(billRepo).forContext(LogContext("r", "s"))
    val outcome = lookup("119-HR-1234").attempt.unsafeRunSync()

    outcome match {
      case Left(e: BillResolutionFailed) =>
        e.billNaturalKey shouldBe "119-HR-1234"
      case other => fail(s"expected Left(BillResolutionFailed), got $other")
    }
  }

  it should "pass the exact natural key through to BillRepository.findByBillId" in {
    val billRepo = mkBillRepo()
    val bill     = billDOMock(7L)
    when(billRepo.findByBillId(eqTo("118-S-42"))).thenReturn(connection.pure(Some(bill)))

    val lookup = mkLookup(billRepo).forContext(LogContext("r", "s"))
    val _      = lookup("118-S-42").unsafeRunSync() shouldBe Some(7L)

    verify(billRepo, times(1)).findByBillId(eqTo("118-S-42"))
  }

  it should "log the error at error level when raising BillResolutionFailed" in {
    val billRepo = mkBillRepo()
    when(billRepo.findByBillId(eqTo("119-HR-9"))).thenReturn(connection.pure(Option.empty[BillDO]))
    val loggerMock = mkLogger

    val lookup = mkLookup(billRepo, loggerMock).forContext(LogContext("r", "s"))
    val _      = lookup("119-HR-9").attempt.unsafeRunSync()

    verify(loggerMock, times(1)).error(any[LogContext], anyString(), any[Option[Throwable]])
  }

}
