package repcheck.ingestion.votes.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.ingestion.votes.errors.BillResolutionFailed
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.placeholder.HasPlaceholder

/**
 * Unit spec for [[BillLookup]]. Verifies the placeholder + findByBillId composition and the defensive
 * [[BillResolutionFailed]] path when the lookup returns None immediately after an idempotent insert.
 */
class BillLookupSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:billlookup;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  // Mockito struggles with Scala 3's `using` arg list on PlaceholderCreator.ensureExists — hand-rolled stub.
  final private class StubPlaceholderCreator extends PlaceholderCreator[IO] {

    def ensureExists[T <: Product](
      naturalKey: String,
      repository: EntityRepository[IO, T],
    )(using HasPlaceholder[T]): IO[Unit] = IO.unit

  }

  private def mkLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    m
  }

  private def mkLookup(billRepo: BillRepository[ConnectionIO]): BillLookup[IO] =
    new BillLookup[IO](
      billRepo = billRepo,
      billEntityRepo = mock[EntityRepository[IO, BillDO]],
      placeholderCreator = new StubPlaceholderCreator,
      xa = testXa,
      logger = mkLogger,
    )

  private def billDOMock(id: Long): BillDO = {
    val b = mock[BillDO]
    when(b.billId).thenReturn(id)
    b
  }

  "forContext(logCtx).apply(nk)" should "resolve to Some(billId) after placeholder + findByBillId succeed" in {
    val billRepo = mock[BillRepository[ConnectionIO]]
    // Finish the BillDO mock's stubbing BEFORE invoking when(billRepo.findByBillId) — Mockito cannot tolerate nested
    // in-progress stubbing (the `when(bill.billId).thenReturn(id)` inside billDOMock would clash with the outer `when`).
    val bill = billDOMock(404L)
    when(billRepo.findByBillId(eqTo("119-HR-1234"))).thenReturn(connection.pure(Some(bill)))

    val lookup = mkLookup(billRepo).forContext(LogContext("r", "s"))
    lookup("119-HR-1234").unsafeRunSync() shouldBe Some(404L)
  }

  it should "raise BillResolutionFailed when findByBillId returns None after ensureExists" in {
    val billRepo = mock[BillRepository[ConnectionIO]]
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
    val billRepo = mock[BillRepository[ConnectionIO]]
    val bill     = billDOMock(7L)
    when(billRepo.findByBillId(eqTo("118-S-42"))).thenReturn(connection.pure(Some(bill)))

    val lookup = mkLookup(billRepo).forContext(LogContext("r", "s"))
    val _      = lookup("118-S-42").unsafeRunSync() shouldBe Some(7L)

    import org.mockito.Mockito.{times, verify}
    verify(billRepo, times(1)).findByBillId(eqTo("118-S-42"))
  }

  it should "log the error at error level when raising BillResolutionFailed" in {
    val billRepo = mock[BillRepository[ConnectionIO]]
    when(billRepo.findByBillId(eqTo("119-HR-9"))).thenReturn(connection.pure(Option.empty[BillDO]))
    val loggerMock = mkLogger

    val lookup = new BillLookup[IO](
      billRepo = billRepo,
      billEntityRepo = mock[EntityRepository[IO, BillDO]],
      placeholderCreator = new StubPlaceholderCreator,
      xa = testXa,
      logger = loggerMock,
    ).forContext(LogContext("r", "s"))

    val _ = lookup("119-HR-9").attempt.unsafeRunSync()

    import org.mockito.Mockito.{times, verify}
    verify(loggerMock, times(1)).error(any[LogContext], anyString(), any[Option[Throwable]])
  }

}
