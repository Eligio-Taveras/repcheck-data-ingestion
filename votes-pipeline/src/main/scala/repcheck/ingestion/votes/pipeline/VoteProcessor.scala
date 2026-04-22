package repcheck.ingestion.votes.pipeline

import java.time.ZoneOffset
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.api.HouseVotesApiClient
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig}
import repcheck.ingestion.votes.errors.VoteProcessingFailed
import repcheck.ingestion.votes.lis.LisResolver
import repcheck.ingestion.votes.repo.StanceMaterializationStatusRepository
import repcheck.ingestion.votes.xml.{SenateVoteIndexEntry, SenateVoteXmlClient}
import repcheck.pipeline.models.events.VoteRecordedEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.dto.conversions.VoteConversions
import repcheck.shared.models.congress.dto.vote.{VoteListItemDTO, VoteMembersDTO}

/**
 * End-to-end vote processor: fetches House + Senate roll-call data, converts it, reconciles members/bills, runs change
 * detection, persists the survivors, and emits `VoteRecordedEvent` for every vote with new scoring signal.
 *
 * ==Streaming shape==
 *   - `streamAll` merges [[processHouseVotes]] and [[processSenateVotes]] via `Stream.merge`. Both chambers run
 *     concurrently; a chamber-level fatal error (persistent 401, index decode failure, etc.) materializes as a single
 *     [[ProcessingResult.Failed]] for that chamber via `handleErrorWith` at the outer boundary, and the other chamber
 *     continues to completion.
 *   - Per-vote work inside each chamber runs under `parEvalMap(config.parallelism)`, with per-request pacing enforced
 *     upstream by the `rateLimitedClient` wrapper at app-level.
 *   - Each per-vote failure becomes a `ProcessingResult.Failed` via a second `handleErrorWith` inside the `parEvalMap`
 *     body; one bad vote does not abort the chamber stream.
 *
 * ==Decision matrix per vote==
 * After the converter produces `(VoteDO, List[VotePositionDO])`, the processor runs the detector against stored state
 * and dispatches on [[VoteChangeReport]]:
 *
 *   - [[VoteChangeReport.New]]: persist via `VotePersister.persistNew`, mark stance (iff bill-linked), emit
 *     `VoteRecordedEvent(isUpdate=false)`, return `Succeeded(..., eventEmitted=true)`.
 *   - [[VoteChangeReport.Updated]] with `positionsChanged = true`: persist via `VotePersister.persistUpdate` (archive +
 *     upsert + replaceAll), mark stance (iff bill-linked), emit `VoteRecordedEvent(isUpdate=true)`, return
 *     `Succeeded(..., eventEmitted=true)`.
 *   - [[VoteChangeReport.Updated]] with `positionsChanged = false`: persist via
 *     `VotePersister.persistMetadataOnlyUpdate` (archive + upsert, skip positions). SKIP event emission (positions are
 *     the scoring signal — no new signal exists to notify about) and SKIP stance mark (same reason). Return
 *     `Succeeded(..., eventEmitted=false)`.
 *   - [[VoteChangeReport.Unchanged]]: no-op, return `Skipped(reason="unchanged")`.
 *
 * ==Stance marking is schema-driven, not policy==
 * `StanceMaterializationStatusRepository.markHasVotes(billId)` fires only when `VoteDO.billId.isDefined`. This is NOT a
 * judgment that procedural votes are unimportant — the `stance_materialization_status` table is schema-keyed on
 * `bill_id` with a FK to `bills(id)`, so there is no row shape for a procedural motion. Procedural votes still emit
 * `VoteRecordedEvent` with `billNaturalKey = None`, and downstream consumers (scoring engine, UI, analytics) decide
 * whether to consume the event for their own purposes.
 *
 * ==Correlation IDs==
 * Each per-vote work unit generates its own `correlationId` at the top of `processHouseVote` / `processSenateVote` so a
 * single vote's trajectory through the pipeline (detector, resolvers, persister, event emitter) can be followed in the
 * logs without mixing with sibling votes. The run-level `runId` is the outer context; correlation IDs never collide
 * with `runId`.
 */
class VoteProcessor[F[_]: Async](
  houseClient: HouseVotesApiClient[F],
  senateClient: SenateVoteXmlClient[F],
  lisResolver: LisResolver[F],
  houseConverter: HouseVoteConverter[F],
  senateConverter: SenateVoteConverter[F],
  changeDetector: VoteChangeDetector[F],
  persister: VotePersister[F],
  stanceRepo: StanceMaterializationStatusRepository,
  eventPublisher: IngestionEventPublisher[F],
  findStoredVote: String => F[Option[VoteDO]],
  xa: Transactor[F],
  houseConfig: HouseVotesConfig,
  senateConfig: SenateVoteXmlConfig,
  congress: Int,
  session: Int,
  logger: PipelineLogger[F],
) {

  private val StepName: String = "votes-processing"

  /**
   * Top-level entry point. Merges the two chamber streams and lets fs2 interleave emitted `ProcessingResult` values
   * opportunistically. The caller (`VotesPipelineApp` / `PipelineExecutor`) folds the stream down into a
   * `PipelineRunSummary`.
   */
  def streamAll(runId: String): Stream[F, ProcessingResult] = {
    val houseCtx  = LogContext(runId, StepName + ":house")
    val senateCtx = LogContext(runId, StepName + ":senate")
    processHouseVotes(runId, houseCtx).merge(processSenateVotes(runId, senateCtx))
  }

  /**
   * Fetch the House list endpoint, flatMap into per-vote detail + processing, and wrap the whole chamber stream in
   * `handleErrorWith` so a list-endpoint failure emits a single `Failed` and does not propagate.
   */
  private[pipeline] def processHouseVotes(
    runId: String,
    logCtx: LogContext,
  ): Stream[F, ProcessingResult] =
    Stream
      .evalSeq(houseClient.fetchRecentVotes)
      .parEvalMap(houseConfig.parallelism) { listItem =>
        val naturalKey    = buildHouseNaturalKey(listItem)
        val correlationId = UUID.randomUUID()
        processHouseVote(listItem, correlationId, runId, naturalKey)
          .handleErrorWith { e =>
            logger.error(logCtx, s"House vote $naturalKey failed: ${e.getMessage}", Some(e)) *>
              Async[F].pure(ProcessingResult.Failed(naturalKey, e.getMessage))
          }
      }
      .handleErrorWith { e =>
        Stream.eval(
          logger.error(logCtx, s"House stream failed: ${e.getMessage}", Some(e))
        ) *> Stream.emit(ProcessingResult.Failed("house-chamber", e.getMessage))
      }

  /**
   * Fetch the Senate index, flatMap each entry into a per-vote fetch + LIS resolve + conversion + processing, and wrap
   * the whole chamber stream in `handleErrorWith` for index-level failures.
   */
  private[pipeline] def processSenateVotes(
    runId: String,
    logCtx: LogContext,
  ): Stream[F, ProcessingResult] =
    Stream
      .evalSeq(senateClient.fetchVoteIndex(congress, session))
      .parEvalMap(senateConfig.parallelism) { entry =>
        val naturalKey    = buildSenateNaturalKey(entry)
        val correlationId = UUID.randomUUID()
        processSenateVote(entry, correlationId, runId, naturalKey)
          .handleErrorWith { e =>
            logger.error(logCtx, s"Senate vote $naturalKey failed: ${e.getMessage}", Some(e)) *>
              Async[F].pure(ProcessingResult.Failed(naturalKey, e.getMessage))
          }
      }
      .handleErrorWith { e =>
        Stream.eval(
          logger.error(logCtx, s"Senate stream failed: ${e.getMessage}", Some(e))
        ) *> Stream.emit(ProcessingResult.Failed("senate-chamber", e.getMessage))
      }

  /**
   * Single-House-vote flow: fetch members, convert to DOs, run the common `processVote` pipeline.
   */
  private[pipeline] def processHouseVote(
    listItem: VoteListItemDTO,
    correlationId: UUID,
    runId: String,
    naturalKey: String,
  ): F[ProcessingResult] = {
    val logCtx = LogContext(
      runId = runId,
      stepName = StepName + ":house",
      correlationId = Some(correlationId),
      entityId = Some(naturalKey),
    )
    for {
      dto <- houseClient.fetchMembersVotePositions(
        congress = listItem.congress,
        session = listItem.sessionNumber.getOrElse(session),
        voteNumber = listItem.rollCallNumber,
      )
      (voteDo, ps) <- houseConverter.convert(dto, logCtx)
      billNk = buildHouseBillNaturalKey(dto)
      result <- processVote(voteDo, ps, billNk, correlationId, logCtx)
    } yield result
  }

  /**
   * Single-Senate-vote flow: fetch XML for the entry, resolve LIS members to `lis_members.id`, convert to DOs, run the
   * common `processVote` pipeline.
   */
  private[pipeline] def processSenateVote(
    entry: SenateVoteIndexEntry,
    correlationId: UUID,
    runId: String,
    naturalKey: String,
  ): F[ProcessingResult] = {
    val logCtx = LogContext(
      runId = runId,
      stepName = StepName + ":senate",
      correlationId = Some(correlationId),
      entityId = Some(naturalKey),
    )
    for {
      dto          <- senateClient.fetchVote(congress, session, entry.voteNumber)
      lisMap       <- lisResolver.resolve(dto)
      (voteDo, ps) <- senateConverter.convert(dto, lisMap, logCtx)
      // senate.gov XML does not carry bill-linkage metadata, so the bill natural key is always None for this path.
      // If a future enrichment stage adds billId to senate votes, plumb it through here.
      result <- processVote(voteDo, ps, billNaturalKey = None, correlationId, logCtx)
    } yield result
  }

  /**
   * Common tail of both chamber paths: look up stored state, run change detection, persist per the report, emit event
   * as appropriate, mark stance iff bill-linked, return the final [[ProcessingResult]].
   */
  private[pipeline] def processVote(
    voteDo: VoteDO,
    positions: List[VotePositionDO],
    billNaturalKey: Option[String],
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    for {
      stored <- findStoredVote(voteDo.naturalKey)
      report <- changeDetector.detect(voteDo, positions, correlationId)
      result <- dispatchOnReport(voteDo, positions, billNaturalKey, stored, report, correlationId, logCtx)
    } yield result

  private def dispatchOnReport(
    voteDo: VoteDO,
    positions: List[VotePositionDO],
    billNaturalKey: Option[String],
    stored: Option[VoteDO],
    report: VoteChangeReport,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    report match {
      case VoteChangeReport.New =>
        for {
          persisted <- persister.persistNew(voteDo, positions)
          _         <- maybeMarkStance(persisted.billId, logCtx)
          _         <- emitEvent(persisted, billNaturalKey, isUpdate = false, correlationId, logCtx)
        } yield ProcessingResult.Succeeded(voteDo.naturalKey, eventEmitted = true)

      case VoteChangeReport.Updated(true, _) =>
        // Stored must be Some here — the detector only returns Updated when a row was found.
        val storedVoteId = stored.map(_.voteId).getOrElse(0L)
        for {
          persisted <- persister.persistUpdate(voteDo, positions, storedVoteId)
          _         <- maybeMarkStance(persisted.billId, logCtx)
          _         <- emitEvent(persisted, billNaturalKey, isUpdate = true, correlationId, logCtx)
        } yield ProcessingResult.Succeeded(voteDo.naturalKey, eventEmitted = true)

      case VoteChangeReport.Updated(false, _) =>
        val storedVoteId = stored.map(_.voteId).getOrElse(0L)
        for {
          _ <- persister.persistMetadataOnlyUpdate(voteDo, storedVoteId)
          // Skip stance mark and event: positions are the scoring signal; unchanged positions means no new signal to
          // emit and no meaningful change to `has_votes`/`votes_updated_at` for the stance scan.
        } yield ProcessingResult.Succeeded(voteDo.naturalKey, eventEmitted = false)

      case VoteChangeReport.Unchanged =>
        Async[F].pure(ProcessingResult.Skipped(voteDo.naturalKey, "unchanged"))
    }

  private def maybeMarkStance(billId: Option[Long], logCtx: LogContext): F[Unit] =
    billId match {
      case None => Async[F].unit
      case Some(b) =>
        stanceRepo.markHasVotes(b).transact(xa).void *>
          logger.info(logCtx, s"Marked stance_materialization_status.has_votes=true for bill_id=${b.toString}")
    }

  private def emitEvent(
    persisted: VoteDO,
    billNaturalKey: Option[String],
    isUpdate: Boolean,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[Unit] = {
    val dateInstant = persisted.updateDate
      .orElse(persisted.voteDate.map(_.atStartOfDay().toInstant(ZoneOffset.UTC)))
      .getOrElse(java.time.Instant.EPOCH)
    val event = VoteRecordedEvent(
      voteNaturalKey = persisted.naturalKey,
      billNaturalKey = billNaturalKey,
      chamber = persisted.chamber.apiValue,
      date = dateInstant,
      congress = persisted.congress,
      isUpdate = isUpdate,
    )
    eventPublisher
      .voteRecorded(event, correlationId)
      .flatMap(msgId =>
        logger.info(
          logCtx,
          s"Published VoteRecordedEvent for ${persisted.naturalKey} (isUpdate=${isUpdate.toString}) as $msgId",
        )
      )
      .handleErrorWith { e =>
        // Persistence succeeded; failing to publish the event degrades downstream visibility but does not invalidate
        // the stored vote. Log loudly and propagate — the enclosing processor's per-vote handleErrorWith will surface
        // this as a ProcessingResult.Failed so operators can triage.
        logger.error(
          logCtx,
          s"Failed to publish VoteRecordedEvent for ${persisted.naturalKey}: ${e.getMessage}",
          Some(e),
        ) *>
          Async[F].raiseError[Unit](
            VoteProcessingFailed(persisted.naturalKey, s"event publish failed: ${e.getMessage}", Some(e))
          )
      }
  }

  /**
   * Construct the canonical vote natural key for a House list item. Uses the DTO's own `sessionNumber` when present
   * (covers the edge case where the DTO reports a session different from the pipeline's configured one) and falls back
   * to the configured `session` as a safety net.
   */
  private[pipeline] def buildHouseNaturalKey(listItem: VoteListItemDTO): String =
    VoteConversions.buildVoteNaturalKey(
      congress = listItem.congress,
      chamber = "House",
      session = listItem.sessionNumber.getOrElse(session),
      rollCallNumber = listItem.rollCallNumber,
    )

  private[pipeline] def buildSenateNaturalKey(entry: SenateVoteIndexEntry): String =
    VoteConversions.buildVoteNaturalKey(
      congress = congress,
      chamber = "Senate",
      session = session,
      rollCallNumber = entry.voteNumber,
    )

  /**
   * Derive a House vote's bill natural key from the DTO's legislation fields when they are populated. For procedural
   * votes (no legislation metadata), returns `None` — the caller skips `BillResolver` entirely, the vote persists with
   * `billId = None`, and the stance mark is skipped for schema reasons.
   */
  private[pipeline] def buildHouseBillNaturalKey(dto: VoteMembersDTO): Option[String] =
    for {
      t <- dto.legislationType
      n <- dto.legislationNumber
    } yield s"${dto.congress.toString}-${t.toUpperCase}-$n"

}
