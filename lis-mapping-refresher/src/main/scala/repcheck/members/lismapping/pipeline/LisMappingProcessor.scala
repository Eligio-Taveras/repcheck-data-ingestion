package repcheck.members.lismapping.pipeline

import java.time.Instant
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import doobie.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.common.persistence.{LisMappingRepository, LisMemberRepository, MemberRepository, UpsertResult}
import repcheck.members.lismapping.client.SenatorLookupXmlClient
import repcheck.members.lismapping.config.LisMappingConfig
import repcheck.members.lismapping.errors.LisMappingUpsertFailed
import repcheck.pipeline.models.events.MemberUpdatedEvent
import repcheck.shared.models.congress.dos.member.{LisMemberDO, MemberLisMappingDO}
import repcheck.shared.models.congress.dto.vote.SenatorLookupXmlDTO

/**
 * Orchestrates the daily LIS mapping refresh. Pulls every senator entry from the senate.gov `senators_cfm.xml` feed,
 * persists the latest `lis_members` and `member_lis_mapping` rows, and emits a [[MemberUpdatedEvent]] for each senator
 * whose mapping was newly inserted AND who already has a profile in the `members` table.
 *
 * Per acceptance criteria (05.6 §behavior): senators whose bioguide ids are not yet present in `members` are skipped —
 * no placeholder row is created here. The `member-profile-pipeline` will seed those rows during its own ingestion
 * cycle, and the next refresh will produce an `Inserted` mapping only if an existing one had been deleted.
 */
class LisMappingProcessor[F[_]: Async](
  xmlClient: SenatorLookupXmlClient[F],
  lisMemberRepo: LisMemberRepository,
  mappingRepo: LisMappingRepository,
  memberRepo: MemberRepository,
  eventPublisher: IngestionEventPublisher[F],
  xa: Transactor[F],
  config: LisMappingConfig,
  logger: PipelineLogger[F],
) {

  // Per-run identity flows in via `runId`; per-item correlation IDs are generated fresh in `processDto`.
  // StepName is a stable per-class label, matching the convention used by sibling clients.
  private val StepName: String = "lis-mapping-refresh"

  /**
   * Fetch-and-upsert the full senator lookup feed. Returns aggregated counts for the run.
   *
   * Failures in the XML fetch or in any single upsert propagate through this `F` — the run halts on the first error.
   * Event-publishing failures are non-fatal with respect to persistence (the mapping has already been committed) but
   * are propagated as errors so that the caller sees a failed refresh and can re-run.
   */
  def refreshAll(runId: Long): F[LisMappingRefreshResult] = {
    val logCtx = LogContext(
      runId = runId.toString,
      stepName = StepName,
    )

    val per: SenatorLookupXmlDTO => F[PerItemOutcome] = dto => processDto(dto, UUID.randomUUID(), logCtx)

    for {
      _ <- logger.info(logCtx, "Starting LIS mapping refresh")
      outcomes <- xmlClient
        .fetchMappings(runId)
        .parEvalMap(config.parallelism)(per)
        .compile
        .toList
      result = tally(outcomes)
      _ <- logger.info(
        logCtx,
        s"LIS mapping refresh complete: parsed=${result.totalParsed.toString} " +
          s"inserted=${result.inserted.toString} updated=${result.updated.toString} " +
          s"eventsEmitted=${result.eventsEmitted.toString}",
      )
    } yield result
  }

  /**
   * Processes a single DTO end-to-end:
   *   1. Upsert the `LisMemberDO` by natural key → returns the BIGSERIAL id. 2. Look up the corresponding `members` row
   *      by bioguide id. 3. If the member is known: upsert the mapping and, if it was `Inserted`, emit a
   *      `MemberUpdatedEvent`. 4. If the member is unknown: skip the mapping entirely — no placeholder is created.
   *
   * Steps 1–3a are executed within a single transaction so that the `lis_members` row and the `member_lis_mapping` row
   * are visible atomically to other readers.
   */
  private[pipeline] def processDto(
    dto: SenatorLookupXmlDTO,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[PerItemOutcome] = {
    val now     = Instant.now()
    val itemCtx = logCtx.copy(entityId = Some(dto.bioguideId))

    val lisMemberDO = buildLisMemberDO(dto, now)

    val program: ConnectionIO[MappingProgramResult] = for {
      lisMemberId <- lisMemberRepo.upsertByNaturalKey(lisMemberDO)
      maybeMember <- memberRepo.findByBioguideId(dto.bioguideId)
      outcome <- maybeMember match {
        case Some(m) =>
          mappingRepo
            .upsert(MemberLisMappingDO(id = 0L, memberId = m.memberId, lisMemberId = lisMemberId, lastVerified = now))
            .map(r => MappingProgramResult.Persisted(r))
        case None =>
          doobie.free.connection.pure[MappingProgramResult](MappingProgramResult.MemberMissing)
      }
    } yield outcome

    for {
      progResult <- program.transact(xa).handleErrorWith { error =>
        Async[F].raiseError[MappingProgramResult](
          LisMappingUpsertFailed(
            lisId = dto.lisId,
            detail = Option(error.getMessage).getOrElse(error.getClass.getSimpleName),
            cause = Some(error),
          )
        )
      }
      outcome <- progResult match {
        case MappingProgramResult.Persisted(UpsertResult.Inserted) =>
          emitEvent(dto, correlationId, itemCtx).as(PerItemOutcome.InsertedWithEvent)
        case MappingProgramResult.Persisted(UpsertResult.Updated) =>
          logger
            .debug(itemCtx, s"Refreshed existing mapping for bioguide=${dto.bioguideId} lisId=${dto.lisId}")
            .as(PerItemOutcome.UpdatedNoEvent)
        case MappingProgramResult.MemberMissing =>
          logger
            .info(
              itemCtx,
              s"Skipping mapping upsert for bioguide=${dto.bioguideId} (lisId=${dto.lisId}) — " +
                s"member row does not exist yet",
            )
            .as(PerItemOutcome.SkippedUnknownMember)
      }
    } yield outcome
  }

  /**
   * Converts a [[SenatorLookupXmlDTO]] into a [[LisMemberDO]] with `lastVerified = now`. `id` is `0L` because the
   * Doobie upsert ignores it and returns the database-assigned BIGSERIAL value.
   */
  private[pipeline] def buildLisMemberDO(dto: SenatorLookupXmlDTO, now: Instant): LisMemberDO =
    LisMemberDO(
      id = 0L,
      naturalKey = dto.lisId,
      firstName = Some(dto.firstName).filter(_.nonEmpty),
      lastName = Some(dto.lastName).filter(_.nonEmpty),
      party = Some(dto.party).filter(_.nonEmpty),
      state = Some(dto.state).filter(_.nonEmpty),
      lastVerified = Some(now),
      createdAt = None,
    )

  /**
   * Emits a [[MemberUpdatedEvent]] for a senator whose mapping was just inserted AND whose `members` row already
   * exists. The event payload carries only the bioguide id — consumers resolve further detail from AlloyDB.
   */
  private[pipeline] def emitEvent(
    dto: SenatorLookupXmlDTO,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[Unit] =
    eventPublisher.memberUpdated(MemberUpdatedEvent(memberId = dto.bioguideId), correlationId) *>
      logger.info(
        logCtx,
        s"Emitted MemberUpdatedEvent for bioguide=${dto.bioguideId} (lisId=${dto.lisId})",
      )

  /**
   * Aggregates the per-item outcomes into a single [[LisMappingRefreshResult]].
   */
  private[pipeline] def tally(outcomes: List[PerItemOutcome]): LisMappingRefreshResult = {
    val start = LisMappingRefreshResult.empty
    outcomes.foldLeft(start) { (acc, outcome) =>
      outcome match {
        case PerItemOutcome.InsertedWithEvent =>
          acc.copy(
            totalParsed = acc.totalParsed + 1,
            inserted = acc.inserted + 1,
            eventsEmitted = acc.eventsEmitted + 1,
          )
        case PerItemOutcome.UpdatedNoEvent =>
          acc.copy(totalParsed = acc.totalParsed + 1, updated = acc.updated + 1)
        case PerItemOutcome.SkippedUnknownMember =>
          acc.copy(totalParsed = acc.totalParsed + 1)
      }
    }
  }

}

/**
 * Outcome of a single senator entry after the full upsert-and-maybe-emit-event sequence has run.
 *
 *   - `InsertedWithEvent`: mapping was newly inserted AND a members row existed — event was emitted.
 *   - `UpdatedNoEvent`: mapping already existed; only `lastVerified` was refreshed.
 *   - `SkippedUnknownMember`: member row does not exist yet → mapping upsert was not attempted.
 */
sealed private[pipeline] trait PerItemOutcome

private[pipeline] object PerItemOutcome {
  case object InsertedWithEvent    extends PerItemOutcome
  case object UpdatedNoEvent       extends PerItemOutcome
  case object SkippedUnknownMember extends PerItemOutcome
}

/**
 * Result of the single per-DTO transaction: either the mapping upsert ran (and produced an [[UpsertResult]]) or the
 * bioguide id was unknown and the mapping was skipped.
 */
sealed private[pipeline] trait MappingProgramResult

private[pipeline] object MappingProgramResult {
  final case class Persisted(result: UpsertResult) extends MappingProgramResult
  case object MemberMissing                        extends MappingProgramResult
}
