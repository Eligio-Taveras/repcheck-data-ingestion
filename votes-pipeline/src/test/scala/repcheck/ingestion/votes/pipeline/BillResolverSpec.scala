package repcheck.ingestion.votes.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.ingestion.votes.errors.BillResolutionFailed
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.placeholder.HasPlaceholder

/**
 * Unit spec for [[BillResolver]]. Same pattern as [[MemberResolverSpec]]: lookup callback stubbed with Mockito-on-
 * function-type, placeholder creator stubbed with a hand-rolled class to sidestep Scala 3's `using`-arg-list
 * interaction with Mockito's matcher validation.
 */
class BillResolverSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val logCtx = LogContext(runId = "r", stepName = "test")

  private type FindBillIdByNaturalKey = String => IO[Option[Long]]

  final private class StubPlaceholderCreator extends PlaceholderCreator[IO] {
    private val callsRef = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)

    def ensureExists[T <: Product](
      naturalKey: String,
      repository: EntityRepository[IO, T],
    )(using HasPlaceholder[T]): IO[Unit] = IO {
      val _ = callsRef.updateAndGet(keys => keys :+ naturalKey)
    }

    def keys: List[String]                 = callsRef.get()
    def callCount: Int                     = keys.size
    def wasCalledWith(nk: String): Boolean = keys.contains(nk)
  }

  private def mkLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    m
  }

  private def mkFixture(lookup: Map[String, Option[Long]]): (
    BillResolver[IO],
    FindBillIdByNaturalKey,
    StubPlaceholderCreator,
    PipelineLogger[IO],
  ) = {
    val findFn = mock[FindBillIdByNaturalKey]
    lookup.foreach {
      case (nk, maybeId) =>
        when(findFn.apply(eqTo(nk))).thenReturn(IO.pure(maybeId))
    }

    val placeholderCreator = new StubPlaceholderCreator
    val entityRepo         = mock[EntityRepository[IO, BillDO]]
    val logger             = mkLogger

    val resolver = new BillResolver[IO](
      findBillIdByNaturalKey = findFn,
      placeholderCreator = placeholderCreator,
      billEntityRepo = entityRepo,
      logger = logger,
    )
    (resolver, findFn, placeholderCreator, logger)
  }

  // ------------------------------------------------------------------

  "resolve" should "ensure a placeholder before looking up and return the resolved id" in {
    val (resolver, findFn, placeholder, _) = mkFixture(Map("119-HR-1234" -> Some(42L)))

    val result = resolver.resolve("119-HR-1234", logCtx).unsafeRunSync()

    val _ = result shouldBe 42L
    val _ = placeholder.wasCalledWith("119-HR-1234") shouldBe true
    verify(findFn, times(1)).apply("119-HR-1234")
  }

  it should "raise BillResolutionFailed when the lookup returns None after ensureExists" in {
    val (resolver, _, _, logger) = mkFixture(Map("119-HR-9999" -> None))

    val outcome = resolver.resolve("119-HR-9999", logCtx).attempt.unsafeRunSync()

    val _ = outcome match {
      case Left(e: BillResolutionFailed) =>
        val _ = e.billNaturalKey shouldBe "119-HR-9999"
        e.getMessage should include("Failed to resolve bill 119-HR-9999")
      case other => fail(s"expected Left(BillResolutionFailed), got $other")
    }

    verify(logger, times(1)).error(any[LogContext], anyString(), any[Option[Throwable]])
  }

  "resolveOptional" should "return None without any side effects when passed None" in {
    val (resolver, findFn, placeholder, _) = mkFixture(Map.empty)

    val _ = resolver.resolveOptional(None, logCtx).unsafeRunSync() shouldBe Option.empty[Long]
    val _ = placeholder.callCount shouldBe 0
    verify(findFn, never()).apply(anyString())
  }

  it should "delegate to resolve when passed Some(nk)" in {
    val (resolver, findFn, placeholder, _) = mkFixture(Map("119-SRES-12" -> Some(7L)))

    val result = resolver.resolveOptional(Some("119-SRES-12"), logCtx).unsafeRunSync()

    val _ = result shouldBe Some(7L)
    val _ = placeholder.wasCalledWith("119-SRES-12") shouldBe true
    verify(findFn, times(1)).apply("119-SRES-12")
  }

}
