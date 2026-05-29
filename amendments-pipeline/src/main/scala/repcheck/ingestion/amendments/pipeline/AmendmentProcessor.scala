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
import repcheck.shared.models.congress.dto.bill.SponsorDTO
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
 * `streamAll(runId)` issues a single global query against `/v3/amendment` (no `{congress}` in the path) with the
 * configured `fromDateTime` lookback. The Congress.gov API returns every amendment edited inside the window regardless
 * of congress, so iterating `config.congresses` per-call would only duplicate work and amplify the lookback overlap.
 * Items whose `congress` falls outside `[congressesMin, congressesMax]` are filtered in-process (defensive — protects
 * the 102-cutoff guard for `bills` / `amendments` schema CHECK constraints). Each list page is grouped via `chunks` so
 * that one SELECT (`findByNaturalKeys`) covers up to `pageSize` items rather than `pageSize` SELECTs. Per-amendment
 * work is fanned out via `parEvalMap(config.parallelism)` and per-item failures are caught and emitted as
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
   * Top-level streaming entry point. Issues a single global query against `/v3/amendment` (no `{congress}` in the path)
   * with `fromDateTime = now - lookbackDays`; the Congress.gov endpoint returns every amendment edited inside that
   * window across all congresses, so per-congress iteration would only duplicate work. Items outside `[congressesMin,
   * congressesMax]` are filtered in-process — the bound is retained as a defensive guard for the 102-cutoff CHECK
   * constraints on the `amendments` / `bills` tables. Each list page is then batch-read via one `findByNaturalKeys`
   * SELECT and per-amendment work is fanned out via `parEvalMap(config.parallelism)`. A single per-amendment failure is
   * captured as `ProcessingResult.Failed` and the stream continues — only stream-level errors propagate up.
   */
  def streamAll(runId: String): Stream[F, ProcessingResult] = {
    val fromDateTime = Instant.now().minus(config.lookbackDays.toLong, ChronoUnit.DAYS)
    val runCtx       = LogContext(runId, StepName)

    Stream.eval(
      logger.info(
        runCtx,
        s"streamAll.start lookbackDays=${config.lookbackDays.toString} " +
          s"parallelism=${config.parallelism.toString} maxRecursionDepth=${config.maxRecursionDepth.toString} " +
          s"congressFilter=${config.congressesMin.toString}..${config.congressesMax.toString}",
      )
    ) *> apiClient
      .fetchAll(
        FetchParams(
          congress = None,
          fromDateTime = Some(fromDateTime),
          pageSize = config.pageSize,
        )
      )
      .chunks
      .evalMap { chunk =>
        // P2 batch optimization: one SELECT per page rather than one per amendment.
        // The global feed returns rows from every congress edited in-window — apply the configured congress
        // filter before the DB round-trip so out-of-range rows neither query stored state nor reach the worker pool.
        val rawItems            = chunk.toList
        val (inRange, outRange) = rawItems.partition(item => isInCongressRange(item.congress))
        val keys                = inRange.map(AmendmentNaturalKeys.fromListItem)
        for {
          _ <- {
            if (outRange.nonEmpty) {
              Async[F].delay {
                outRange.foreach(_ => metrics.incrementCongressOutOfRange())
              } *>
                logger.debug(
                  runCtx,
                  s"page.filter congresses=${outRange.map(_.congress.toString).distinct.mkString(",")} " +
                    s"dropped=${outRange.size.toString}/${rawItems.size.toString}",
                )
            } else { Async[F].unit }
          }
          _ <- logger.debug(
            runCtx,
            s"page.batch.start size=${inRange.size.toString} (raw=${rawItems.size.toString})",
          )
          storedMap <- amendmentRepository.findByNaturalKeys(keys).transact(xa)
          _ <- logger.debug(
            runCtx,
            s"page.batch.done hits=${storedMap.size.toString}/${inRange.size.toString}",
          )
        } yield inRange.map(item => (item, storedMap.get(AmendmentNaturalKeys.fromListItem(item))))
      }
      .flatMap(Stream.emits)
      .parEvalMap(config.parallelism) {
        case (item, storedOpt) =>
          val naturalKey    = AmendmentNaturalKeys.fromListItem(item)
          val correlationId = UUID.randomUUID()
          val itemCtx       = LogContext(runId, StepName, Some(correlationId), Some(naturalKey))
          processAmendment(
            naturalKey = naturalKey,
            listItemOpt = Some(item),
            detailUrlOpt = item.url,
            storedOpt = storedOpt,
            depth = 0,
            correlationId = correlationId,
          ).handleErrorWith { e =>
            logger.error(itemCtx, s"Amendment $naturalKey processing raised: ${e.getMessage}", Some(e)) *>
              Async[F].pure[ProcessingResult](ProcessingResult.Failed(naturalKey, e.getMessage))
          }
      }
      // Per-item failures are already caught above. This guards the LIST-pagination path (fetchAll):
      // a list page that exhausts its retries on a 429 would otherwise propagate and abort the whole
      // backfill. Instead, log and end the stream gracefully so the run exits 0 with a partial result
      // and the next idempotent re-run resumes — rather than crashing every run mid-stream.
      .handleErrorWith { e =>
        Stream
          .eval(
            logger.error(
              runCtx,
              "streamAll terminated early (likely Congress.gov rate limit) — partial run; the next " +
                s"idempotent re-run will resume: ${e.getMessage}",
              Some(e),
            )
          )
          .drain
      }
  }

  /**
   * Inclusive `[congressesMin, congressesMax]` test for a list-page item. Surfaced separately so tests can pin the
   * defensive 102-cutoff filter behavior directly.
   */
  private[pipeline] def isInCongressRange(congress: Int): Boolean =
    congress >= config.congressesMin && congress <= config.congressesMax

  /**
   * The single recursive primitive. See class-doc for the step-by-step contract. Public for testability — every spec
   * exercises behavior through this entry point rather than through `streamAll`.
   *
   * `detailUrlOpt` carries the per-amendment detail URL handed over by the upstream list page (top-level call) or the
   * parent `amendedAmendment` DTO (recursive call). When neither source surfaces a URL we synthesize one from
   * `naturalKey` so the call still resolves against the configured `baseUrl` rather than blowing up with a malformed
   * request.
   */
  def processAmendment(
    naturalKey: String,
    listItemOpt: Option[AmendmentListItemDTO],
    detailUrlOpt: Option[String],
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
      hydrateAndPersist(naturalKey, listItemOpt, detailUrlOpt, depth, correlationId, ctx)
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
    detailUrlOpt: Option[String],
    depth: Int,
    correlationId: UUID,
    ctx: LogContext,
  ): F[ProcessingResult] = {
    val _         = listItemOpt // listItem fields are not used past idempotency; keep the param for signature symmetry.
    val detailUrl = detailUrlOpt.orElse(listItemOpt.flatMap(_.url)).getOrElse(naturalKey)
    for {
      _      <- logger.debug(ctx, s"fetchDetail.start depth=${depth.toString} url=$detailUrl")
      _      <- Async[F].delay(metrics.incrementDetailFetches())
      detail <- apiClient.fetchDetail(detailUrl, correlationId)
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
    detail.sponsors.flatMap(_.headOption).collect { case m: SponsorDTO.MemberSponsorDTO => m.bioguideId } match {
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
    detail.amendedAmendment match {
      case None =>
        Async[F].pure((Option.empty[Long], Option.empty[Long]))

      case Some(parentRef) =>
        AmendmentNaturalKeys.parentAmendmentNaturalKey(parentRef) match {
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
                  // Parent missing OR placeholder — recurse with the SAME correlationId, listItemOpt = None,
                  // and the parent's `url` from the DTO (when present) so the recursive `fetchDetail` still
                  // hits the right endpoint without us having to reconstruct the URL from the natural key.
                  for {
                    _ <- Async[F].delay(metrics.incrementRecursionRedundant())
                    _ <- logger.debug(
                      ctx,
                      s"resolveParent.recurse parentNk=$parentNk parentExists=${otherwise.isDefined.toString}",
                    )
                    _ <- processAmendment(
                      naturalKey = parentNk,
                      listItemOpt = None,
                      detailUrlOpt = parentRef.url,
                      storedOpt = otherwise,
                      depth = depth + 1,
                      correlationId = correlationId,
                    )
                    hydrated <- amendmentRepository.findByNaturalKey(parentNk).transact(xa)
                  } yield (hydrated.map(_.amendmentId), hydrated.flatMap(_.billId))
              }
            } yield tuple
        }
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
