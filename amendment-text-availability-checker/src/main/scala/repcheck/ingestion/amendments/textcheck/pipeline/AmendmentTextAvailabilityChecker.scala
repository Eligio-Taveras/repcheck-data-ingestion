package repcheck.ingestion.amendments.textcheck.pipeline

import java.time.Instant
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.amendments.persistence.AmendmentRepository
import repcheck.ingestion.amendments.textcheck.api.AmendmentTextApiClient
import repcheck.ingestion.amendments.textcheck.config.AmendmentTextCheckerConfig
import repcheck.ingestion.amendments.textcheck.errors.AmendmentTextCheckMissingAmendmentType
import repcheck.ingestion.amendments.textcheck.events.AmendmentTextEventPublisher
import repcheck.ingestion.amendments.textcheck.persistence.AmendmentTextVersionLookup
import repcheck.ingestion.amendments.textcheck.selection.AmendmentTextVersionSelector
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.events.AmendmentTextAvailableEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.dos.amendment.AmendmentDO
import repcheck.shared.models.congress.dto.amendment.{AmendmentFormatDTO, AmendmentTextItemDTO}

/**
 * Cron-driven amendment-text availability check. For each amendment the SQL pre-filter selects (per
 * `AmendmentRepository.findCandidatesForTextCheck`), this:
 *
 *   1. fetches the upstream `/amendment/.../text` payload, 2. compares its (versionTypeCode, formatType) tuples against
 *      `amendment_text_versions` via [[AmendmentTextVersionLookup]], 3. emits one `AmendmentTextAvailableEvent` per NEW
 *      tuple to `amendment.text.available`, 4. on success ONLY (per L1 of the §7.5 plan) stamps `last_text_check_at =
 *      NOW()`.
 *
 * Failure path leaves `last_text_check_at` unchanged so the next cron tick retries. `Skipped`/`Succeeded` results both
 * count as success for the timestamp update — the goal of the column is "we successfully reached upstream and processed
 * the diff", and both outcomes reach that bar.
 */
class AmendmentTextAvailabilityChecker[F[_]: Async](
  apiClient: AmendmentTextApiClient[F],
  amendmentRepo: AmendmentRepository[ConnectionIO],
  textVersionLookup: AmendmentTextVersionLookup[ConnectionIO],
  eventPublisher: AmendmentTextEventPublisher[F],
  xa: Transactor[F],
  config: AmendmentTextCheckerConfig,
  logger: PipelineLogger[F],
) {

  private val StepName = "amendment-text-availability-check"

  /**
   * Stream every candidate amendment's `ProcessingResult`. The candidate query runs once at the start of the stream;
   * results are produced lazily as each per-amendment check completes via `parEvalMap(parallelism)`.
   */
  def checkAll(runId: Long): Stream[F, ProcessingResult] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName)

    Stream
      .eval(
        logger.info(
          logCtx,
          s"Starting amendment text availability check (minCongress=${config.minCongress.toString}, " +
            s"staleAfter=${config.staleAfter.toString})",
        ) *>
          amendmentRepo
            .findCandidatesForTextCheck(config.minCongress, config.staleAfter)
            .transact(xa)
      )
      .flatMap { candidates =>
        Stream
          .eval(
            logger.info(
              logCtx,
              s"Found ${candidates.size.toString} amendments needing text check " +
                s"(parallelism=${config.parallelism.toString})",
            )
          )
          .drain ++
          Stream
            .emits(candidates)
            .parEvalMap(config.parallelism)(amendment => checkAmendment(amendment, UUID.randomUUID()))
      }
  }

  private[pipeline] def checkAmendment(amendment: AmendmentDO, correlationId: UUID): F[ProcessingResult] = {
    val naturalKey = amendment.naturalKey
    val logCtx = LogContext(
      runId = correlationId.toString,
      stepName = StepName,
      correlationId = Some(correlationId),
      entityId = Some(naturalKey),
    )

    val work: F[ProcessingResult] = for {
      _              <- logger.info(logCtx, s"Checking amendment $naturalKey")
      amendmentType  <- requireAmendmentType(amendment, naturalKey)
      upstream       <- apiClient.fetchTextVersions(amendment.congress, amendmentType, amendment.number, correlationId)
      _              <- logger.info(logCtx, s"Got ${upstream.size.toString} upstream text version(s) for $naturalKey")
      existingTuples <- textVersionLookup.findExistingVersions(amendment.amendmentId).transact(xa)
      newTuples = AmendmentTextVersionSelector.selectAllNewVersions(upstream, existingTuples)
      result <- emitAndStamp(amendment, naturalKey, newTuples, correlationId, logCtx)
    } yield result

    work.handleErrorWith(error => handleCheckError(naturalKey, error, logCtx))
  }

  private[pipeline] def emitAndStamp(
    amendment: AmendmentDO,
    naturalKey: String,
    newTuples: List[(AmendmentTextItemDTO, AmendmentFormatDTO)],
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    // `stampLastChecked` invokes a Doobie repository method on construction. Mockito records the call
    // even if the IO is never run — so we MUST defer the construction until after the inner effect
    // (publish or "skip" log) has completed successfully. Otherwise the mocked repository would
    // record a stamp call we never wanted to happen on a failure path.
    val deferredStamp: F[Unit] = Async[F].defer(stampLastChecked(amendment.amendmentId))

    if (newTuples.isEmpty) {
      logger
        .info(logCtx, s"No new text versions for $naturalKey — skipping")
        .flatMap(_ => deferredStamp)
        .as(ProcessingResult.Skipped(entityId = naturalKey, reason = "no-new-versions"))
    } else {
      newTuples
        .traverse_ { case (item, fmt) => publishOneEvent(amendment, naturalKey, item, fmt, correlationId, logCtx) }
        .flatMap(_ => deferredStamp)
        .as(ProcessingResult.Succeeded(entityId = naturalKey, eventEmitted = true))
    }
  }

  private[pipeline] def publishOneEvent(
    amendment: AmendmentDO,
    naturalKey: String,
    item: AmendmentTextItemDTO,
    fmt: AmendmentFormatDTO,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[Unit] = {
    val versionCode = AmendmentTextVersionSelector.versionTypeCode(item.`type`).getOrElse("")
    // amendmentType is a `Some` here (we passed `requireAmendmentType` upstream of any path that reaches this method);
    // unwrap with a fallback that won't be hit in practice — keeping this defensive instead of `.get` to avoid the
    // OptionPartial wart and have a deterministic fallback if the row's enum was somehow null.
    val amendmentType = amendment.amendmentType.getOrElse(AmendmentType.SAMDT)
    val event = AmendmentTextAvailableEvent(
      amendmentId = amendment.amendmentId,
      naturalKey = naturalKey,
      congress = amendment.congress,
      amendmentType = amendmentType,
      number = amendment.number,
      versionTypeCode = versionCode,
      formatType = fmt.`type`,
      url = fmt.url,
      publishedDate = item.date.flatMap(parsePublishedDate),
      correlationId = correlationId,
    )

    eventPublisher.publish(event, correlationId).flatMap { messageId =>
      logger.info(
        logCtx,
        s"Emitted AmendmentTextAvailableEvent for $naturalKey ($versionCode/${fmt.`type`}) — messageId=$messageId",
      )
    }
  }

  /**
   * Update `last_text_check_at = NOW()`. Per L1, only invoked on the success path (any failure short-circuits before
   * this method runs via `handleCheckError`).
   */
  private[pipeline] def stampLastChecked(amendmentId: Long): F[Unit] =
    amendmentRepo.updateLastTextCheckAt(amendmentId).transact(xa)

  private[pipeline] def requireAmendmentType(amendment: AmendmentDO, naturalKey: String): F[AmendmentType] =
    amendment.amendmentType match {
      case Some(t) => Async[F].pure(t)
      case None =>
        Async[F].raiseError(
          AmendmentTextCheckMissingAmendmentType(naturalKey, amendment.amendmentId)
        )
    }

  /**
   * Best-effort parse of the upstream `date` field (ISO-8601 datetime string or null) to an `Instant`. Anything that
   * doesn't parse becomes `None`; the event still emits, just without a `publishedDate`.
   */
  private[pipeline] def parsePublishedDate(raw: String): Option[Instant] =
    scala.util.Try(Instant.parse(raw)).toOption

  private[pipeline] def handleCheckError(
    naturalKey: String,
    error: Throwable,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    logger
      .error(
        logCtx,
        s"Failed to check amendment text for $naturalKey: ${error.getMessage}",
        Some(error),
      )
      .as(
        ProcessingResult.Failed(
          entityId = naturalKey,
          reason = s"Amendment text check failed: ${error.getMessage}",
          errorClass = "AmendmentTextCheckFailed",
        )
      )

}
