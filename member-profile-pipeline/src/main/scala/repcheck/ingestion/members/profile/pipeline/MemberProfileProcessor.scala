package repcheck.ingestion.members.profile.pipeline

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie._
import doobie.implicits._

import repcheck.ingestion.common.api.FetchParams
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.members.profile.api.MembersApiClient
import repcheck.ingestion.members.profile.config.MemberProfileConfig
import repcheck.ingestion.members.profile.errors.MemberProcessingFailed
import repcheck.members.common.diff.MemberDiffer
import repcheck.members.common.persistence.{
  MemberHistoryArchiver,
  MemberPartyHistoryRepository,
  MemberRepository,
  MemberTermRepository,
}
import repcheck.pipeline.models.events.MemberUpdatedEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.common.Chamber
import repcheck.shared.models.congress.dos.member.{MemberDO, MemberTermDO}
import repcheck.shared.models.congress.dos.results.MemberConversionResult
import repcheck.shared.models.congress.dto.conversions.MemberConversions.MemberDetailDTOOps
import repcheck.shared.models.congress.dto.member.MemberListItemDTO

/**
 * FS2 streaming pipeline that fetches member profiles from Congress.gov, detects changes, archives history, upserts to
 * AlloyDB, and emits `member.updated` events with chamber-aware logic.
 *
 * The archive -> upsert -> replace-terms -> append-party-history steps are composed into a single [[ConnectionIO]]
 * program and executed inside one database transaction via `.transact(xa)`; if any step fails, the entire transaction
 * rolls back. Event emission happens after the transaction commits.
 *
 * Chamber-aware emission: House members always receive `member.updated` after upsert. Senate members only receive the
 * event when an LIS mapping already exists — otherwise the senator is considered incomplete and the event is skipped
 * (the [[repcheck.members.common.persistence.MemberRepository.existsWithLisMapping]] query answers this).
 */
class MemberProfileProcessor[F[_]: Async](
  apiClient: MembersApiClient[F],
  memberRepo: MemberRepository,
  termRepo: MemberTermRepository,
  partyHistoryRepo: MemberPartyHistoryRepository,
  historyArchiver: MemberHistoryArchiver[ConnectionIO],
  eventPublisher: IngestionEventPublisher[F],
  xa: Transactor[F],
  config: MemberProfileConfig,
  logger: PipelineLogger[F],
) {

  private val stepName: String = "member-profile-processing"

  def streamAll(runId: Long): Stream[F, ProcessingResult] = {
    val params = FetchParams(congress = Some(config.congress))
    val logCtx = LogContext(runId.toString, stepName)

    apiClient
      .fetchAll(params)
      .handleErrorWith { e =>
        Stream.eval(
          logger.error(logCtx, s"Page fetch failed, completing with partial results: ${e.getMessage}", Some(e))
        ) *> Stream.empty
      }
      // NOTE: `parEvalMap(config.parallelism)` controls how many `processMember` fibers can run concurrently. It does NOT
      // throttle outbound HTTP calls — Congress.gov rate limits are tied to the API key (shared across bills/members/
      // votes/etc.), so the throttle has to live at the http4s `Client[F]` layer. The IOApp wiring (Phase 5A) wraps the
      // raw EmberClient with the centralized `RateLimitedHttpClient.make` (ingestion-common) before constructing
      // `MembersApiClient`, so the call sites here remain unchanged.
      .parEvalMap(config.parallelism) { listItem =>
        val correlationId = UUID.randomUUID()
        val itemCtx       = LogContext(runId.toString, stepName, Some(correlationId), Some(listItem.bioguideId))
        processMember(listItem, correlationId, runId).handleErrorWith { e =>
          logger.error(itemCtx, s"Failed to process ${listItem.bioguideId}: ${e.getMessage}", Some(e)) *>
            Async[F].pure(ProcessingResult.Failed(listItem.bioguideId, e.getMessage, e.getClass.getSimpleName))
        }
      }
  }

  def processMember(listItem: MemberListItemDTO, correlationId: UUID, runId: Long = 0L): F[ProcessingResult] = {
    val bioguideId = listItem.bioguideId
    val logCtx     = LogContext(runId.toString, stepName, Some(correlationId), Some(bioguideId))

    for {
      detail <- apiClient.fetchDetail(listItem.url.getOrElse(""))
      conversionResult <- Async[F].fromEither(
        detail.toDO.leftMap(reason => MemberProcessingFailed(bioguideId, s"DTO-to-DO conversion failed: $reason"))
      )
      stored <- findStoredMember(bioguideId)
      result <- evaluateAndProcess(bioguideId, conversionResult, stored, correlationId, logCtx)
    } yield result
  }

  private def findStoredMember(bioguideId: String): F[Option[MemberDO]] =
    memberRepo.findByBioguideId(bioguideId).transact(xa)

  private def evaluateAndProcess(
    bioguideId: String,
    conversionResult: MemberConversionResult,
    stored: Option[MemberDO],
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    stored match {
      case None =>
        logger.info(logCtx, s"New member detected: $bioguideId") *>
          persistAndEmit(bioguideId, conversionResult, isNew = true, correlationId, logCtx)

      case Some(existingMember) if isChanged(incoming = conversionResult.member, stored = existingMember) =>
        logger.info(logCtx, s"Updated member detected: $bioguideId") *>
          persistAndEmit(bioguideId, conversionResult, isNew = false, correlationId, logCtx)

      case Some(_) =>
        logger.debug(logCtx, s"Member unchanged: $bioguideId") *>
          Async[F].pure(ProcessingResult.Skipped(bioguideId, "unchanged"))
    }

  /**
   * A member is considered changed when the incoming `updateDate` strictly supersedes the stored one AND the incoming
   * differs from the stored in at least one non-identity field.
   *
   * The stale-first check matches the canonical [[repcheck.ingestion.common.changes.ChangeDetector.detect]] semantics:
   * if the incoming `updateDate` is not strictly after the stored one, we consider the member unchanged — we never
   * overwrite more-recent data. Placeholders written by the bills pipeline have `updateDate = None`: once the
   * member-profile API returns a real `updateDate`, that `None -> Some` transition falls through to the field-level
   * diff and is treated as a change.
   *
   * Only once the date check says the incoming wins do we compare fields via [[MemberDiffer.diffIgnoringIdentity]],
   * which excludes the DB-managed identity columns (`memberId`, `createdAt`, `updatedAt`) — see that method's docstring
   * for why we don't use difflicious's `.ignoreAt` directly.
   */
  private[pipeline] def isChanged(incoming: MemberDO, stored: MemberDO): Boolean =
    (incoming.updateDate, stored.updateDate) match {
      case (Some(i), Some(s)) if !i.isAfter(s) => false
      case (None, _)                           => false
      case _                                   => !MemberDiffer.diffIgnoringIdentity(incoming, stored).isOk
    }

  private def persistAndEmit(
    bioguideId: String,
    conversionResult: MemberConversionResult,
    isNew: Boolean,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    for {
      _        <- persistInTransaction(bioguideId, conversionResult, isNew, logCtx)
      memberId <- findMemberIdForBioguide(bioguideId)
      emitted  <- emitEventIfEligible(bioguideId, memberId, conversionResult.terms, correlationId, logCtx)
      _        <- logger.info(logCtx, s"Member $bioguideId upserted (eventEmitted=$emitted)")
    } yield ProcessingResult.Succeeded(bioguideId, emitted)

  /**
   * Composes archive + upsert + term replace + party history append into a single [[ConnectionIO]] program and executes
   * it inside one database transaction. Archive is a no-op for members not yet in `members` (the archiver itself
   * handles the missing-row case by returning `ConnectionIO.unit`).
   */
  private def persistInTransaction(
    bioguideId: String,
    conversionResult: MemberConversionResult,
    isNew: Boolean,
    logCtx: LogContext,
  ): F[Unit] = {
    val program: ConnectionIO[Unit] =
      for {
        _        <- historyArchiver.archiveMember(bioguideId)
        memberId <- memberRepo.upsert(conversionResult.member)
        _        <- termRepo.replaceAll(memberId, conversionResult.terms)
        _        <- partyHistoryRepo.appendNew(memberId, conversionResult.partyHistory)
      } yield ()

    val archiveNote = if (isNew) { "new member — archive will no-op" }
    else { "existing member — archive runs first" }
    logger.debug(logCtx, s"Persisting $bioguideId in a single transaction ($archiveNote)") *>
      program.transact(xa)
  }

  private def findMemberIdForBioguide(bioguideId: String): F[Long] =
    memberRepo.findByBioguideId(bioguideId).transact(xa).flatMap {
      case Some(m) => Async[F].pure(m.memberId)
      case None =>
        Async[F].raiseError(
          MemberProcessingFailed(
            bioguideId,
            "Member vanished between upsert and post-transaction re-read — expected a row to exist.",
          )
        )
    }

  /**
   * Decides whether to emit `member.updated` based on chamber derived from the member's most recent term (highest
   * `congress`). House members always get the event; Senate members only get it once an LIS mapping is recorded.
   * Members with no terms (or an unrecognizable chamber) default to House-style emission so we do not silently drop
   * events for otherwise valid members.
   */
  private def emitEventIfEligible(
    bioguideId: String,
    memberId: Long,
    terms: List[MemberTermDO],
    correlationId: UUID,
    logCtx: LogContext,
  ): F[Boolean] =
    chamberFromTerms(terms) match {
      case Some(Chamber.Senate) =>
        memberRepo.existsWithLisMapping(memberId).transact(xa).flatMap { hasMapping =>
          if (hasMapping) {
            publishMemberUpdated(bioguideId, correlationId).as(true)
          } else {
            logger
              .info(
                logCtx,
                s"Senate member $bioguideId has no LIS mapping yet; skipping member.updated event until mapping exists",
              )
              .as(false)
          }
        }
      case _ =>
        publishMemberUpdated(bioguideId, correlationId).as(true)
    }

  private def publishMemberUpdated(bioguideId: String, correlationId: UUID): F[Unit] =
    eventPublisher.memberUpdated(MemberUpdatedEvent(bioguideId), correlationId).void

  private[pipeline] def chamberFromTerms(terms: List[MemberTermDO]): Option[Chamber] = {
    val withCongress = terms.flatMap(t => t.congress.map(c => (c, t)))
    val mostRecent   = withCongress.sortBy { case (c, _) => -c }.headOption.map { case (_, term) => term }
    mostRecent.orElse(terms.headOption).flatMap(_.chamber)
  }

}
