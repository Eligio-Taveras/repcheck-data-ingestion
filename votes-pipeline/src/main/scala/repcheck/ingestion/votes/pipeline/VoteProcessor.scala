package repcheck.ingestion.votes.pipeline

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.amendments.persistence.AmendmentRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.api.HouseVotesApiClient
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig}
import repcheck.ingestion.votes.lis.LisResolver
import repcheck.ingestion.votes.metrics.AmendmentPlaceholderSkipCounter
import repcheck.ingestion.votes.xml.{SenateVoteIndexEntry, SenateVoteXmlClient}
import repcheck.pipeline.models.constants.Constants
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.results.VoteConversionResult
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.dto.vote.VoteMembersDTO

/**
 * End-to-end vote processor: fetches House + Senate roll-call data, converts it, reconciles members, detects what
 * changed, persists the survivors, and emits `VoteRecordedEvent` for every vote with new scoring signal. The processor
 * is a thin orchestrator — every sub-responsibility (bill resolution, member resolution, position materialization,
 * event emission, natural-key construction) is owned by a dedicated collaborator and injected via the constructor.
 *
 * ==Sequencing: save the vote first, then save its positions==
 *
 * Per the data model and the §6.4 decision matrix, the processor runs the operations in a strict order:
 *   1. Convert DTO → [[VoteConversionResult]] with inline bill lookup via [[BillLookup]]. `VoteDO.billId` is populated
 *      by the converter itself; the processor does NOT override it afterwards. 2. Resolve member identity for each
 *      incoming position — House bioguides via [[MemberLookup]], Senate LIS ids via [[LisResolver]]. 3. Look up the
 *      stored vote by natural key to drive change detection. 4. Run [[VoteChangeDetector]]. If `Unchanged` or
 *      metadata-only `Updated`, stop early. 5. Persist: [[VotePersister]] upserts the vote (archiving first on Updated
 *      branches), returns the DB-assigned `voteId`, then invokes the position factory lambda — built by
 *      [[VotePositionBuilders]] with the real `voteId` and real `member_id` / `lis_member_id` values. 6. For bill-
 *      linked new/updated votes, [[VoteEventEmitter]] marks `stance_materialization_status.has_votes = true` and emits
 *      `VoteRecordedEvent`.
 *
 * No [[VotePositionDO]] is ever carried through the pipeline with a fake `voteId = 0L`. The comparison view used by the
 * detector is materialized just-in-time keyed on the stored row's real `voteId` when a stored row exists; the persisted
 * rows are built by the factory lambda inside the persister's transaction.
 *
 * ==Procedural vs. bill-linked votes==
 *
 * A vote's `billId: Option[Long]` is nullable by design. Procedural motions (cloture, motion to recommit, motion to
 * suspend the rules, etc.) are first-class votes with no bill linkage; they still produce `VoteRecordedEvent` with
 * `billNaturalKey = None` and every position persists. The only thing gated on `billId.isDefined` is the
 * `stance_materialization_status.markHasVotes(billId)` call inside [[VoteEventEmitter]].
 *
 * ==Chamber-level failure isolation==
 *
 * `streamAll` merges the two chamber streams via `Stream.merge`. A chamber-level fatal error (persistent 401,
 * index-decode failure, etc.) is caught at the outer boundary and emitted as a single
 * `ProcessingResult.Failed("house-chamber", …)` or `"senate-chamber"`, and the other chamber continues. Per-vote
 * failures inside `parEvalMap` isolate via an inner `handleErrorWith` — one bad vote doesn't abort the chamber.
 */
class VoteProcessor[F[_]: Async](
  houseClient: HouseVotesApiClient[F],
  senateClient: SenateVoteXmlClient[F],
  lisResolver: LisResolver[F],
  houseConverter: HouseVoteConverter[F],
  senateConverter: SenateVoteConverter[F],
  changeDetector: VoteChangeDetector[F],
  persister: VotePersister[F],
  billLookup: BillLookup[F],
  memberLookup: MemberLookup[F],
  eventEmitter: VoteEventEmitter[F],
  amendmentRepo: AmendmentRepository[doobie.ConnectionIO],
  xa: Transactor[F],
  skipCounter: AmendmentPlaceholderSkipCounter,
  findStoredVote: String => F[Option[VoteDO]],
  houseConfig: HouseVotesConfig,
  senateConfig: SenateVoteXmlConfig,
  logger: PipelineLogger[F],
) {

  private val StepName: String = "votes-processing"

  /**
   * Sessions covered for every congress. Both House and Senate organize roll-call votes by session 1 and 2 within a
   * 2-year congress; iterating both is correct in all cases.
   */
  private val Sessions: List[Int] = List(1, 2)

  /**
   * Top-level entry point. Iterates over the supplied congresses (resolved upstream from `VOTES_CONGRESSES` env or the
   * bills table) and, for each one, fans out House + Senate streams across sessions {1, 2}.
   *
   * Per-(congress, session) failures convert to `ProcessingResult.Failed("<chamber>-chamber-<c>-<s>", ...)` via the
   * existing chamber-level `handleErrorWith` so a single 404 (e.g. on an old congress without electronic vote data)
   * does not abort the rest of the run.
   */
  def streamAll(runId: String, congresses: List[Int]): Stream[F, ProcessingResult] = {
    val pairs: List[(Int, Int)] = congresses.flatMap(c => Sessions.map(s => (c, s)))
    Stream.emits(pairs).flatMap {
      case (congress, session) =>
        val houseCtx  = LogContext(runId, StepName + ":house")
        val senateCtx = LogContext(runId, StepName + ":senate")
        processHouseVotes(congress, session, runId, houseCtx)
          .merge(processSenateVotes(congress, session, runId, senateCtx))
    }
  }

  // =========================================================================
  // Chamber streams
  // =========================================================================

  private[pipeline] def processHouseVotes(
    congress: Int,
    session: Int,
    runId: String,
    logCtx: LogContext,
  ): Stream[F, ProcessingResult] =
    Stream
      .evalSeq(houseClient.fetchRecentVotes(congress, session))
      .parEvalMap(houseConfig.parallelism) { listItem =>
        val naturalKey    = VoteNaturalKeys.houseVote(listItem, fallbackSession = session)
        val correlationId = UUID.randomUUID()
        processHouseVote(listItem, correlationId, runId, naturalKey)
          .handleErrorWith { e =>
            logger.error(logCtx, s"House vote $naturalKey failed: ${e.getMessage}", Some(e)) *>
              Async[F].pure(ProcessingResult.Failed(naturalKey, e.getMessage))
          }
      }
      .handleErrorWith { e =>
        Stream.eval(
          logger.error(logCtx, s"House stream failed for $congress/$session: ${e.getMessage}", Some(e))
        ) *> Stream.emit(ProcessingResult.Failed(s"house-chamber-$congress-$session", e.getMessage))
      }

  private[pipeline] def processSenateVotes(
    congress: Int,
    session: Int,
    runId: String,
    logCtx: LogContext,
  ): Stream[F, ProcessingResult] =
    Stream
      .evalSeq(senateClient.fetchVoteIndex(congress, session))
      .parEvalMap(senateConfig.parallelism) { entry =>
        val naturalKey    = VoteNaturalKeys.senateVote(entry, congress, session)
        val correlationId = UUID.randomUUID()
        processSenateVote(entry, congress, session, correlationId, runId, naturalKey)
          .handleErrorWith { e =>
            logger.error(logCtx, s"Senate vote $naturalKey failed: ${e.getMessage}", Some(e)) *>
              Async[F].pure(ProcessingResult.Failed(naturalKey, e.getMessage))
          }
      }
      .handleErrorWith { e =>
        Stream.eval(
          logger.error(logCtx, s"Senate stream failed for $congress/$session: ${e.getMessage}", Some(e))
        ) *> Stream.emit(ProcessingResult.Failed(s"senate-chamber-$congress-$session", e.getMessage))
      }

  // =========================================================================
  // Per-vote flows (House and Senate)
  // =========================================================================

  private[pipeline] def processHouseVote(
    listItem: repcheck.shared.models.congress.dto.vote.VoteListItemDTO,
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
    // House DTO carries congress/sessionNumber inline; sessionNumber is occasionally absent on older list items so
    // we fall back to the listItem's enclosing session via the API call site (House detail endpoint requires both).
    val callSession = listItem.sessionNumber.getOrElse(1)
    houseClient
      .fetchMembersVotePositions(
        congress = listItem.congress,
        session = callSession,
        voteNumber = listItem.rollCallNumber,
      )
      .flatMap {
        case None =>
          // Congress.gov returned `{"houseRollCallVoteMemberVotes": []}` (sentinel "no data" — common
          // on older votes that pre-date the API's member-vote dataset). Treat as Skipped, not Failed:
          // the vote exists per the list endpoint but has no member-position records to ingest.
          logger
            .warn(
              logCtx,
              s"House vote $naturalKey has no member-vote data (Congress.gov returned an empty " +
                "houseRollCallVoteMemberVotes array) — skipping",
            )
            .as(ProcessingResult.Skipped(naturalKey, "no member-vote data available from API"))
        case Some(rawDto) =>
          for {
            // §7.4 House dispatch: amendment-typed `legislationType` (HAMDT/SAMDT/SUAMDT) routes through
            // amendmentRepo.upsertPlaceholder instead of bills-pipeline. The DTO is rewritten to clear the
            // amendment-typed legislationType (shared-models `parseLegislationType` only knows BillType, so
            // leaving HAMDT in place would fail conversion). We retain `legislationNumber` so the vote row
            // still records which amendment was voted on.
            adjustedDto <- handleHouseAmendmentDispatch(rawDto, naturalKey, logCtx)
            conversion  <- houseConverter.convert(adjustedDto, billLookup.forContext(logCtx), logCtx)
            // House arm: resolve each bioguide to members.id (placeholder + lookup).
            memberMap <- memberLookup.resolveAll(conversion.positions, logCtx)
            buildPositions = (voteId: Long) => VotePositionBuilders.house(voteId, conversion.positions, memberMap)
            result <- processVote(
              conversion,
              buildPositions,
              VoteNaturalKeys.houseBill(adjustedDto),
              correlationId,
              logCtx,
            )
          } yield result
      }
  }

  /**
   * House-side amendment dispatch (§7.4). If `dto.legislationType` parses to an [[AmendmentType]]:
   *   - For congress >= 102: upsert an amendment placeholder via [[AmendmentRepository.upsertPlaceholder]] using the
   *     `{congress}-{TYPE}-{number}` natural key. Errors are logged + raised so the per-vote `handleErrorWith` in
   *     `processHouseVotes` records a Failed.
   *   - For congress < 102 (pre-amendments-pipeline range): increment the pre-102 skip counter, log INFO, no repository
   *     write. The vote itself still persists below.
   *
   * Returns a copy of the DTO with `legislationType = None` so the shared-models `parseLegislationType` doesn't fail
   * conversion. `legislationNumber` is retained — the vote row records which amendment number was voted on, just
   * without the now-absent `BillType` enum value (until shared-models bumps to a polymorphic type).
   *
   * Bill-typed and missing `legislationType`s pass through untouched.
   */
  private[pipeline] def handleHouseAmendmentDispatch(
    dto: VoteMembersDTO,
    voteNaturalKey: String,
    logCtx: LogContext,
  ): F[VoteMembersDTO] =
    AmendmentLegislationTypes.parseAmendmentType(dto.legislationType) match {
      case None => Async[F].pure(dto)
      case Some(amendmentType) =>
        dto.legislationNumber match {
          case None =>
            // Congress.gov contract is that legislationType + legislationNumber are populated together.
            // If number is missing, log + persist without amendment linkage rather than raising.
            logger
              .warn(
                logCtx,
                s"House vote $voteNaturalKey has legislationType '${amendmentType.toString}' but no " +
                  "legislationNumber — persisting without amendment placeholder",
              )
              .as(dto.copy(legislationType = None))
          case Some(number) =>
            val amendmentNK = AmendmentLegislationTypes.buildAmendmentNaturalKey(dto.congress, amendmentType, number)
            val placeholderEffect: F[Unit] =
              if (dto.congress < Constants.MinAmendmentCongress) {
                skipCounter.incPre102Skip[F] *>
                  logger.info(
                    logCtx,
                    s"Skipping pre-102 amendment placeholder for $amendmentNK (vote $voteNaturalKey)",
                  )
              } else {
                amendmentRepo.upsertPlaceholder(amendmentNK).transact(xa).handleErrorWith { err =>
                  logger.error(
                    logCtx,
                    s"Amendment placeholder failed for $amendmentNK (vote $voteNaturalKey)",
                    Some(err),
                  ) *> Async[F].raiseError[Unit](err)
                }
              }
            placeholderEffect.as(dto.copy(legislationType = None))
        }
    }

  private[pipeline] def processSenateVote(
    entry: SenateVoteIndexEntry,
    congress: Int,
    session: Int,
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
      envelope   <- senateClient.fetchVote(congress, session, entry.voteNumber)
      lisMap     <- lisResolver.resolve(envelope.dto)
      conversion <- senateConverter.convert(envelope, billLookup.forContext(logCtx), logCtx)
      buildPositions = (voteId: Long) => VotePositionBuilders.senate(voteId, conversion.positions, lisMap)
      result <- processVote(conversion, buildPositions, VoteNaturalKeys.senateBill(envelope.dto), correlationId, logCtx)
    } yield result
  }

  // =========================================================================
  // Common flow: detect change, persist + emit
  // =========================================================================

  /**
   * Common tail that both chamber paths converge on: detect change, persist per the decision matrix, mark stance + emit
   * event. The converter has already populated `conversion.vote.billId` via its `billLookup` callback — the processor
   * trusts that value and does not re-resolve. The caller supplies a `buildPositions: Long => List[VotePositionDO]`
   * factory that materializes position rows given the DB-assigned vote id.
   */
  private[pipeline] def processVote(
    conversion: VoteConversionResult,
    buildPositions: Long => List[VotePositionDO],
    dtoBillNaturalKey: Option[String],
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    // Either chamber may attach a DTO-derived natural key; the shared conversions pathway uses this when House's
    // `legislationType` is known. In practice only one of `dtoBillNaturalKey` / `conversion.billNaturalKey` is `Some`,
    // so `.orElse` just picks whichever side has it for event-emission bookkeeping.
    val billNaturalKey = dtoBillNaturalKey.orElse(conversion.billNaturalKey)
    val voteDo         = conversion.vote
    for {
      stored <- findStoredVote(voteDo.naturalKey)
      // Build the comparison-only position list with the stored row's voteId when one exists (real id → clean
      // structural diff) and a sentinel 0L otherwise (the detector short-circuits to New before the positions are
      // inspected). No VotePositionDO with a fake voteId ever reaches persist.
      comparisonVoteId    = stored.map(_.voteId).getOrElse(0L)
      comparisonPositions = buildPositions(comparisonVoteId)
      report <- changeDetector.detect(voteDo, comparisonPositions, correlationId)
      result <- dispatchOnReport(voteDo, buildPositions, billNaturalKey, stored, report, correlationId, logCtx)
    } yield result
  }

  private def dispatchOnReport(
    voteDo: VoteDO,
    buildPositions: Long => List[VotePositionDO],
    billNaturalKey: Option[String],
    stored: Option[VoteDO],
    report: VoteChangeReport,
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] =
    report match {
      case VoteChangeReport.New =>
        for {
          persisted <- persister.persistNew(voteDo, buildPositions)
          _         <- eventEmitter.emitSuccess(persisted, billNaturalKey, isUpdate = false, correlationId, logCtx)
        } yield ProcessingResult.Succeeded(voteDo.naturalKey, eventEmitted = true)

      case VoteChangeReport.Updated(true, _) =>
        // Stored must be Some here — the detector only returns Updated when a row was found.
        val storedVoteId = stored.map(_.voteId).getOrElse(0L)
        for {
          persisted <- persister.persistUpdate(voteDo, storedVoteId, buildPositions)
          _         <- eventEmitter.emitSuccess(persisted, billNaturalKey, isUpdate = true, correlationId, logCtx)
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

}
