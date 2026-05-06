package repcheck.ingestion.amendments.pipeline

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.amendments.api.AmendmentsApiClient
import repcheck.ingestion.amendments.config.AmendmentsConfig
import repcheck.ingestion.amendments.errors.{AmendmentProcessingFailed, AmendmentRecursionTooDeep}
import repcheck.ingestion.amendments.observability.AmendmentMetrics
import repcheck.ingestion.amendments.persistence.AmendmentRepository
import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.common.api.FetchParams
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.members.common.persistence.MemberRepository
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.amendment.AmendmentDO
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.amendment.{AmendmentDetailDTO, AmendmentListItemDTO}
import repcheck.shared.models.congress.dto.conversions.AmendmentConversions._

/**
 * End-to-end amendment processor. Streams amendments from `AmendmentsApiClient`, batches per-page stored-row reads, and
 * per-amendment fans out into the inline parent-recursion contract spelled out in §7.3. The recursion drains the full
 * ancestor chain to the bill before persisting any row, so by the time we upsert a child row its parent already has
 * `bill_id (resolved ancestor)` populated. No end-of-run sweep, no events emitted.
 *
 * ==Inline parent recursion contract (§7.3 / IMPLEMENTATION_PLAN Phase 2)==
 *
 * `processAmendment(naturalKey, listItemOpt, storedOpt, depth, correlationId)`:
 *   1. Depth guard — `depth > config.maxRecursionDepth` raises `AmendmentRecursionTooDeep` (no cycle guard, per S1). 2.
 *      Idempotency — `storedOpt.exists(_.updateDate.isDefined)` AND incoming list `updateDate <= stored.updateDate` →
 *      `Skipped("unchanged")`. 3. `apiClient.fetchDetail(naturalKey, correlationId)` — single API call per recursion
 *      frame; the SAME correlation id is threaded through every frame. 4. Resolve sponsor —
 *      `placeholderCreator.ensureExists[MemberDO](bioguide, memberEntityRepo)` followed by
 *      `memberRepo.findByBioguideId(bioguide).map(_.map(_.memberId))`. Mirrors `MemberResolver`. 5. Resolve bill —
 *      `billRepo.upsertPlaceholder(bnk).transact(xa)` followed by
 *      `billRepo.findByBillId(bnk).map(_.map(_.billId)).transact(xa)`. Mirrors `BillLookup.forContext`. 6. Resolve
 *      parent (recursive): when `detail.amendedAmendment` is defined, look up the parent. If it's already hydrated
 *      (`updateDate.isDefined`) use as-is; else recurse with `listItemOpt = None` and the SAME correlation id, then
 *      re-read. 7. Compute `effectiveBillId` inline as `resolvedBillId.orElse(parentEffectiveBillId)` — no
 *      `effective_bill_id` column lookup; `bill_id` carries the resolved-ancestor semantic. 8. DTO→DO via
 *      `detail.toDO(billId, sponsorMemberId, parentAmendmentId)` — `Left` returns `Failed`. 9. Upsert in a single SQL
 *      statement (no multi-table transaction needed).
 *
 * ==Streaming==
 *
 * `streamAll(runId)` iterates `config.congresses` sequentially. Each list page is grouped via `chunks` so that one
 * SELECT (`findByNaturalKeys`) covers up to `pageSize` items rather than `pageSize` SELECTs. Per-amendment work is
 * fanned out via `parEvalMap(config.parallelism)` and per-item failures are caught and emitted as
 * `ProcessingResult.Failed` so the stream survives a single bad amendment.
 */
class AmendmentProcessor[F[_]: Async](
  apiClient: AmendmentsApiClient[F],
  amendmentRepository: AmendmentRepository[doobie.ConnectionIO],
  placeholderCreator: PlaceholderCreator[F],
  memberRepository: MemberRepository,
  memberEntityRepo: EntityRepository[F, MemberDO],
  billRepository: BillRepository[doobie.ConnectionIO],
  xa: Transactor[F],
  config: AmendmentsConfig,
  logger: PipelineLogger[F],
  metrics: AmendmentMetrics,
) {

  private val StepName = "amendments-pipeline"

  /**
   * Top-level streaming entry point. Iterates `config.congresses` sequentially (per Q8 — mirrors votes-pipeline's
   * per-congress pattern), drains `apiClient.fetchAll` for each one, batch-reads stored rows per page, and fans out the
   * per-amendment work via `parEvalMap(config.parallelism)`. A single per-amendment failure is captured as
   * `ProcessingResult.Failed` and the stream continues — only stream-level errors propagate up.
   */
  def streamAll(runId: String): Stream[F, ProcessingResult] = {
    val fromDateTime = Instant.now().minus(config.lookbackDays.toLong, ChronoUnit.DAYS)
    val runCtx       = LogContext(runId, StepName)

    Stream.eval(
      logger.info(
        runCtx,
        s"streamAll.start lookbackDays=${config.lookbackDays.toString} " +
          s"parallelism=${config.parallelism.toString} maxRecursionDepth=${config.maxRecursionDepth.toString} " +
          s"congresses=${config.congresses.start.toString}..${config.congresses.end.toString}",
      )
    ) *> Stream
      .emits(config.congresses.toList)
      .flatMap(congress => streamCongress(congress, fromDateTime, runId, runCtx))
  }

  private[pipeline] def streamCongress(
    congress: Int,
    fromDateTime: Instant,
    runId: String,
    runCtx: LogContext,
  ): Stream[F, ProcessingResult] =
    apiClient
      .fetchAll(
        FetchParams(
          congress = Some(congress),
          fromDateTime = Some(fromDateTime),
          pageSize = config.pageSize,
        )
      )
      .chunks
      .evalMap { chunk =>
        // P2 batch optimization: one SELECT per page rather than one per amendment.
        val items = chunk.toList
        val keys  = items.map(AmendmentNaturalKeys.fromListItem)
        for {
          _ <- logger.debug(runCtx, s"page.batch.start congress=${congress.toString} size=${items.size.toString}")
          storedMap <- amendmentRepository.findByNaturalKeys(keys).transact(xa)
          _ <- logger.debug(
            runCtx,
            s"page.batch.done congress=${congress.toString} hits=${storedMap.size.toString}/${items.size.toString}",
          )
        } yield items.map(item => (item, storedMap.get(AmendmentNaturalKeys.fromListItem(item))))
      }
      .flatMap(Stream.emits)
      .parEvalMap(config.parallelism) {
        case (item, storedOpt) =>
          val naturalKey    = AmendmentNaturalKeys.fromListItem(item)
          val correlationId = UUID.randomUUID()
          val itemCtx       = LogContext(runId, StepName, Some(correlationId), Some(naturalKey))
          processAmendment(naturalKey, Some(item), storedOpt, depth = 0, correlationId)
            .handleErrorWith { e =>
              logger.error(itemCtx, s"Amendment $naturalKey processing raised: ${e.getMessage}", Some(e)) *>
                Async[F].pure[ProcessingResult](ProcessingResult.Failed(naturalKey, e.getMessage))
            }
      }

  /**
   * The single recursive primitive. See class-doc for the step-by-step contract. Public for testability — every spec
   * exercises behavior through this entry point rather than through `streamAll`.
   */
  def processAmendment(
    naturalKey: String,
    listItemOpt: Option[AmendmentListItemDTO],
    storedOpt: Option[AmendmentDO],
    depth: Int,
    correlationId: UUID,
  ): F[ProcessingResult] = {
    val ctx = LogContext("0", StepName, Some(correlationId), Some(naturalKey))

    if (depth > config.maxRecursionDepth) {
      val err = AmendmentRecursionTooDeep(depth, naturalKey)
      Async[F].delay(metrics.incrementRecursionDepthExceeded()) *>
        logger.warn(ctx, err.getMessage) *>
        Async[F].pure[ProcessingResult](ProcessingResult.Failed(naturalKey, err.getMessage))
    } else if (isUnchanged(listItemOpt, storedOpt)) {
      logger.debug(ctx, s"idempotent skip — list updateDate <= stored updateDate") *>
        Async[F].pure[ProcessingResult](ProcessingResult.Skipped(naturalKey, "unchanged"))
    } else {
      hydrateAndPersist(naturalKey, listItemOpt, depth, correlationId, ctx)
    }
  }

  /**
   * Encapsulates steps 3–9 of the recursion contract: detail fetch → sponsor / bill / parent resolve → DTO→DO → upsert.
   * Surfaces conversion failures (`Left`) as `ProcessingResult.Failed` and any unexpected throwable as the
   * `AmendmentProcessingFailed` chain that the caller's outer `handleErrorWith` converts to `Failed`.
   */
  private[pipeline] def hydrateAndPersist(
    naturalKey: String,
    listItemOpt: Option[AmendmentListItemDTO],
    depth: Int,
    correlationId: UUID,
    ctx: LogContext,
  ): F[ProcessingResult] = {
    val _ = listItemOpt // listItem fields are not used past idempotency; keep the param for signature symmetry.
    for {
      _      <- logger.debug(ctx, s"fetchDetail.start depth=${depth.toString}")
      _      <- Async[F].delay(metrics.incrementDetailFetches())
      detail <- apiClient.fetchDetail(naturalKey, correlationId)
      _      <- logger.debug(ctx, s"fetchDetail.done")

      sponsorMemberId  <- resolveSponsorMemberId(detail, ctx)
      directBillId     <- resolveBillId(detail, ctx)
      parentResolution <- resolveParent(detail, depth, correlationId, ctx)
      (parentAmendmentId, parentBillId) = parentResolution

      effectiveBillId = directBillId.orElse(parentBillId)
      _ <-
        if (effectiveBillId.isEmpty) {
          Async[F].delay(metrics.incrementOrphanResolved())
        } else {
          Async[F].unit
        }

      result <- detail.toDO(
        billId = effectiveBillId,
        sponsorMemberId = sponsorMemberId,
        parentAmendmentId = parentAmendmentId,
      ) match {
        case Left(reason) =>
          val msg = s"DTO-to-DO conversion failed: $reason"
          logger.warn(ctx, msg) *>
            Async[F].pure[ProcessingResult](ProcessingResult.Failed(naturalKey, msg))

        case Right(amendmentDO) =>
          for {
            _ <- amendmentRepository.upsert(amendmentDO).transact(xa).adaptError {
              case e => AmendmentProcessingFailed(naturalKey, e)
            }
            _ <- logger.info(
              ctx,
              s"Amendment $naturalKey upserted bill_id=${effectiveBillId.fold("none")(_.toString)} " +
                s"parent=${parentAmendmentId.fold("none")(_.toString)} " +
                s"sponsor=${sponsorMemberId.fold("none")(_.toString)}",
            )
          } yield ProcessingResult.Succeeded(naturalKey): ProcessingResult
      }
    } yield result
  }

  /**
   * Step 4 of the contract. Mirrors `bill-metadata-pipeline`'s `MemberResolver.ensureSponsorPlaceholder`: idempotent
   * placeholder + lookup. Yields `None` when the DTO has no sponsor.
   */
  private[pipeline] def resolveSponsorMemberId(
    detail: AmendmentDetailDTO,
    ctx: LogContext,
  ): F[Option[Long]] =
    detail.sponsors.flatMap(_.headOption).map(_.bioguideId) match {
      case Some(bioguideId) =>
        for {
          _        <- logger.debug(ctx, s"resolveSponsor.start bioguideId=$bioguideId")
          _        <- placeholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)
          memberId <- memberRepository.findByBioguideId(bioguideId).map(_.map(_.memberId)).transact(xa)
          _ <- logger.debug(
            ctx,
            s"resolveSponsor.done bioguideId=$bioguideId memberId=${memberId.fold("none")(_.toString)}",
          )
        } yield memberId

      case None =>
        logger.debug(ctx, s"resolveSponsor.skip (no sponsor)") *>
          Async[F].pure(Option.empty[Long])
    }

  /**
   * Step 5 of the contract. Mirrors votes-pipeline's `BillLookup.forContext`: reuses `BillRepository.upsertPlaceholder`
   * (idempotent `ON CONFLICT DO NOTHING`) followed by `findByBillId`. Yields `None` when the DTO doesn't reference a
   * bill (procedural / treaty / amendment-of-amendment).
   */
  private[pipeline] def resolveBillId(
    detail: AmendmentDetailDTO,
    ctx: LogContext,
  ): F[Option[Long]] =
    detail.amendedBill.flatMap(AmendmentNaturalKeys.amendedBillNaturalKey) match {
      case Some(bnk) =>
        for {
          _      <- logger.debug(ctx, s"resolveBill.start billNaturalKey=$bnk")
          _      <- billRepository.upsertPlaceholder(bnk).transact(xa)
          billId <- billRepository.findByBillId(bnk).map(_.map(_.billId)).transact(xa)
          _ <- logger.debug(
            ctx,
            s"resolveBill.done billNaturalKey=$bnk billId=${billId.fold("none")(_.toString)}",
          )
        } yield billId

      case None =>
        logger.debug(ctx, s"resolveBill.skip (no amendedBill)") *>
          Async[F].pure(Option.empty[Long])
    }

  /**
   * Step 6 of the contract — recursive parent resolution. When the detail DTO carries an `amendedAmendment`, the
   * parent's natural key is computed; if the parent is already hydrated in storage, its surrogate id and `bill_id` are
   * used as-is. Otherwise we recurse with `listItemOpt = None`, the SAME correlation id, and `depth + 1`, then re-read
   * the now-hydrated parent.
   *
   * Returns the tuple `(parentAmendmentId, parentBillId)` consumed by step 7's effective-bill computation. Both are
   * `None` when the DTO has no parent reference.
   *
   * Per L2 — a parallel sibling that also references this parent will still race a fresh detail fetch; that is the
   * documented wasted-call. The `metrics.incrementRecursionRedundant()` bump fires whenever we enter the recursive
   * branch (i.e., parent missing OR placeholder), giving operators a counter to watch during backfill.
   */
  private[pipeline] def resolveParent(
    detail: AmendmentDetailDTO,
    depth: Int,
    correlationId: UUID,
    ctx: LogContext,
  ): F[(Option[Long], Option[Long])] =
    detail.amendedAmendment.flatMap(AmendmentNaturalKeys.parentAmendmentNaturalKey) match {
      case None =>
        Async[F].pure((Option.empty[Long], Option.empty[Long]))

      case Some(parentNk) =>
        for {
          _              <- logger.debug(ctx, s"resolveParent.start parentNk=$parentNk depth=${depth.toString}")
          parentExisting <- amendmentRepository.findByNaturalKey(parentNk).transact(xa)
          tuple <- parentExisting match {
            case Some(p) if p.updateDate.isDefined =>
              // Parent already fully hydrated — use as-is. No recursion, no extra detail fetch.
              logger.debug(ctx, s"resolveParent.short-circuit parentNk=$parentNk") *>
                Async[F].pure((Some(p.amendmentId), p.billId))

            case otherwise =>
              // Parent missing OR placeholder — recurse with the SAME correlationId + listItemOpt = None.
              for {
                _ <- Async[F].delay(metrics.incrementRecursionRedundant())
                _ <- logger.debug(
                  ctx,
                  s"resolveParent.recurse parentNk=$parentNk parentExists=${otherwise.isDefined.toString}",
                )
                _ <- processAmendment(
                  naturalKey = parentNk,
                  listItemOpt = None,
                  storedOpt = otherwise,
                  depth = depth + 1,
                  correlationId = correlationId,
                )
                hydrated <- amendmentRepository.findByNaturalKey(parentNk).transact(xa)
              } yield (hydrated.map(_.amendmentId), hydrated.flatMap(_.billId))
          }
        } yield tuple
    }

  /**
   * Idempotency check for step 2 of the contract. Returns `true` only when both sides of the comparison have an
   * `updateDate` populated and the incoming list-page date is `<=` the stored row's date. A recursive caller
   * (`listItemOpt = None`) never satisfies the condition — every recursive frame either short-circuits at the
   * parent-already-hydrated branch in [[resolveParent]] or falls through to a real fetch + upsert.
   */
  private def isUnchanged(
    listItemOpt: Option[AmendmentListItemDTO],
    storedOpt: Option[AmendmentDO],
  ): Boolean =
    (listItemOpt, storedOpt.flatMap(_.updateDate)) match {
      case (Some(item), Some(storedDate)) =>
        parseInstant(item.updateDate).exists(incoming => !incoming.isAfter(storedDate))
      case _ => false
    }

  private def parseInstant(raw: Option[String]): Option[Instant] =
    raw.flatMap(s => scala.util.Try(Instant.parse(s)).toOption)

  /**
   * Roll a `List[ProcessingResult]` into the [[PipelineRunSummary]] surface §7.3 advertises in its acceptance criteria.
   * `eventsEmitted` is hard-coded to `0` because the amendments pipeline emits no events — downstream consumers
   * (text-availability checker §7.5) discover changes via cron-driven DB scan.
   */
  def summarize(results: List[ProcessingResult]): PipelineRunSummary =
    PipelineRunSummary(
      totalProcessed = results.size,
      succeeded = results.count(_.isSucceeded),
      skipped = results.count(_.isSkipped),
      failed = results.count(_.isFailed),
      eventsEmitted = 0,
      errors = results.collect { case f: ProcessingResult.Failed => f.reason },
    )

}

/**
 * Snapshot of a single pipeline run, returned by [[AmendmentProcessor.summarize]]. Lives next to the processor (rather
 * than in pipeline-models) because the shape is amendments-pipeline-specific — `eventsEmitted` is always 0 here, and
 * `errors` retains the raw reason strings rather than the grouped error counts pipeline-models surfaces in
 * `StepRunSummary`.
 */
final case class PipelineRunSummary(
  totalProcessed: Int,
  succeeded: Int,
  skipped: Int,
  failed: Int,
  eventsEmitted: Int,
  errors: List[String],
)
