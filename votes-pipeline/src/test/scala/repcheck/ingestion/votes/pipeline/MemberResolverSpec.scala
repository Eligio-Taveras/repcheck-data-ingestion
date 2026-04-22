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
import repcheck.ingestion.votes.errors.MemberResolutionFailed
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.placeholder.HasPlaceholder

/**
 * Unit spec for [[MemberResolver]]. Every dependency is stubbed — the lookup callback via Mockito-on-function-type, the
 * placeholder creator via a hand-rolled `StubPlaceholderCreator` (same pattern bill-metadata-pipeline uses) to sidestep
 * Scala 3's `using` arg-list interaction with Mockito's matcher validation.
 */
class MemberResolverSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val logCtx = LogContext(runId = "r", stepName = "test")

  private type FindMemberIdByBioguide = String => IO[Option[Long]]

  /**
   * Hand-rolled trait implementation: records every `ensureExists` call's natural key so the test can assert call count
   * and order without passing Mockito matchers through a `using`-parameterized method.
   */
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
    MemberResolver[IO],
    FindMemberIdByBioguide,
    StubPlaceholderCreator,
    PipelineLogger[IO],
  ) = {
    val findFn = mock[FindMemberIdByBioguide]
    lookup.foreach {
      case (bid, maybeId) =>
        when(findFn.apply(eqTo(bid))).thenReturn(IO.pure(maybeId))
    }

    val placeholderCreator = new StubPlaceholderCreator
    val entityRepo         = mock[EntityRepository[IO, MemberDO]]
    val logger             = mkLogger

    val resolver = new MemberResolver[IO](
      findMemberIdByBioguide = findFn,
      placeholderCreator = placeholderCreator,
      memberEntityRepo = entityRepo,
      logger = logger,
    )
    (resolver, findFn, placeholderCreator, logger)
  }

  // ------------------------------------------------------------------

  "resolveBioguide" should "ensure a placeholder before looking up and return the resolved id" in {
    val (resolver, findFn, placeholder, _) = mkFixture(Map("A000055" -> Some(42L)))

    val result = resolver.resolveBioguide("A000055", logCtx).unsafeRunSync()

    val _ = result shouldBe 42L
    val _ = placeholder.wasCalledWith("A000055") shouldBe true
    verify(findFn, times(1)).apply("A000055")
  }

  it should "raise MemberResolutionFailed when the lookup returns None after ensureExists" in {
    val (resolver, _, _, logger) = mkFixture(Map("B000055" -> None))

    val outcome = resolver.resolveBioguide("B000055", logCtx).attempt.unsafeRunSync()

    val _ = outcome match {
      case Left(e: MemberResolutionFailed) =>
        val _ = e.bioguideId shouldBe "B000055"
        e.getMessage should include("Failed to resolve member B000055")
      case other => fail(s"expected Left(MemberResolutionFailed), got $other")
    }

    // Error is also logged at error level for operator visibility
    verify(logger, times(1)).error(any[LogContext], anyString(), any[Option[Throwable]])
  }

  "resolveBatch" should "short-circuit to an empty map when input is empty (never touches the placeholder creator or lookup)" in {
    val (resolver, findFn, placeholder, _) = mkFixture(Map.empty)

    val _ = resolver.resolveBatch(List.empty, logCtx).unsafeRunSync() shouldBe Map.empty[String, Long]
    val _ = placeholder.callCount shouldBe 0
    verify(findFn, never()).apply(anyString())
  }

  it should "dedupe its input before resolving (each distinct bioguide yields one ensureExists + one lookup)" in {
    val (resolver, findFn, placeholder, _) =
      mkFixture(Map("A000055" -> Some(1L), "B000055" -> Some(2L)))

    val result =
      resolver.resolveBatch(List("A000055", "B000055", "A000055", "B000055"), logCtx).unsafeRunSync()

    val _ = result shouldBe Map("A000055" -> 1L, "B000055" -> 2L)
    val _ = placeholder.callCount shouldBe 2
    val _ = placeholder.wasCalledWith("A000055") shouldBe true
    val _ = placeholder.wasCalledWith("B000055") shouldBe true
    val _ = verify(findFn, times(1)).apply("A000055")
    verify(findFn, times(1)).apply("B000055")
  }

  it should "propagate MemberResolutionFailed if any bioguide cannot be resolved after ensureExists" in {
    val (resolver, _, _, _) = mkFixture(Map("A000055" -> Some(1L), "B000055" -> None))

    val outcome = resolver.resolveBatch(List("A000055", "B000055"), logCtx).attempt.unsafeRunSync()

    outcome match {
      case Left(_: MemberResolutionFailed) => succeed
      case other                           => fail(s"expected Left(MemberResolutionFailed), got $other")
    }
  }

}
