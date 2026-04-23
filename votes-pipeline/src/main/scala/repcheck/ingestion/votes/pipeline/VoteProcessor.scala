package repcheck.ingestion.votes.pipeline

import java.time.ZoneOffset
import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.ingestion.votes.api.HouseVotesApiClient
import repcheck.ingestion.votes.config.{HouseVotesConfig, SenateVoteXmlConfig}
import repcheck.ingestion.votes.errors.{BillResolutionFailed, MemberResolutionFailed, VoteProcessingFailed}
import repcheck.ingestion.votes.lis.LisResolver
import repcheck.ingestion.votes.repo.StanceMaterializationStatusRepository
import repcheck.ingestion.votes.xml.{SenateVoteIndexEntry, SenateVoteXmlClient}
import repcheck.members.common.persistence.MemberRepository
import repcheck.pipeline.models.events.VoteRecordedEvent
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dos.results.{UnresolvedVotePosition, VoteConversionResult}
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}
import repcheck.shared.models.congress.dto.conversions.VoteConversions
import repcheck.shared.models.congress.dto.vote.{SenateVoteXmlDTO, VoteListItemDTO, VoteMembersDTO}

/**
 * End-to-end vote processor: fetches House + Senate roll-call data, converts it, reconciles members and bills, detects
 * what changed, persists the survivors, and emits `VoteRecordedEvent` for every vote with new scoring signal.
 *
 * ==Sequencing: save the vote first, then save its positions==
 *
 * Per the data model and the §6.4 decision matrix, the processor runs the operations in a strict order:
 *   1. Convert DTO → [[VoteConversionResult]] (pure validation + enum parsing, no IO). 2. Resolve the optional bill
 *      natural key → `bills.id` (placeholder + lookup). 3. Resolve member identity for each incoming position — House
 *      bioguides → `members.id`; Senate LIS ids → `lis_members.id` via [[LisResolver]]. 4. Look up the stored vote by
 *      natural key to drive change detection. 5. Run the change detector against stored state. If the detector returns
 *      `Unchanged` or metadata-only `Updated`, stop early. 6. Persist: upsert the vote (archive first on Updated
 *      branches), get the DB-assigned `voteId`, and ONLY THEN build [[VotePositionDO]] rows with the real `voteId` and
 *      real `member_id` / `lis_member_id` values. The persister's factory lambda runs inside the transaction, so
 *      position materialization and replacement are atomic with the vote upsert. 7. If the vote is bill-linked, mark
 *      `stance_materialization_status.has_votes = TRUE` for the bill. 8. Emit `VoteRecordedEvent` (except on the
 *      Unchanged and metadata-only-Updated branches).
 *
 * No [[VotePositionDO]] is ever carried through the pipeline with a fake `voteId = 0L`. The comparison view used by the
 * detector is materialized just-in-time (keyed on the stored row's real `voteId` when a stored row exists), and the
 * persisted rows are built by the lambda inside the persister's transaction.
 *
 * ==Procedural vs. bill-linked votes==
 *
 * A vote's `billId: Option[Long]` is nullable by design. Procedural motions (cloture, motion to recommit, motion to
 * suspend the rules, etc.) are first-class votes with no bill linkage; they still produce `VoteRecordedEvent` with
 * `billNaturalKey = None` and every position persists. The only thing gated on `billId.isDefined` is the
 * `stance_materialization_status.markHasVotes(billId)` call, and that gate is mechanical: the stance table's primary
 * key is `bill_id`, so there is no row shape for a procedural vote. Downstream consumers of `vote-events` decide for
 * themselves whether procedural signal matters for their use case.
 *
 * ==Chamber-level failure isolation==
 *
 * `streamAll` merges `processHouseVotes` and `processSenateVotes` via `Stream.merge`. A chamber-level fatal error
 * (persistent 401, index-decode failure, etc.) is caught at the outer boundary and emitted as a single
 * `ProcessingResult.Failed("house-chamber", …)` / `"senate-chamber"`, and the other chamber continues. Per-vote
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
  stanceRepo: StanceMaterializationStatusRepository,
  eventPublisher: IngestionEventPublisher[F],
  memberRepo: MemberRepository,
  billRepo: BillRepository[doobie.ConnectionIO],
  memberEntityRepo: EntityRepository[F, MemberDO],
  billEntityRepo: EntityRepository[F, BillDO],
  placeholderCreator: PlaceholderCreator[F],
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

  // =========================================================================
  // Chamber streams
  // =========================================================================

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

  // =========================================================================
  // Per-vote flows (House and Senate)
  // =========================================================================

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
      conversion <- houseConverter.convert(dto, logCtx)
      // House arm: resolve each bioguide to members.id (placeholder + lookup). Resulting map drives buildHousePositions.
      memberMap <- resolveHouseMembers(conversion.positions, logCtx)
      buildPositions = (voteId: Long) => buildHousePositions(voteId, conversion.positions, memberMap)
      result <- processVote(conversion, buildPositions, buildHouseBillNaturalKey(dto), correlationId, logCtx)
    } yield result
  }

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
      dto        <- senateClient.fetchVote(congress, session, entry.voteNumber)
      lisMap     <- lisResolver.resolve(dto)
      conversion <- senateConverter.convert(dto, logCtx)
      buildPositions = (voteId: Long) => buildSenatePositions(voteId, conversion.positions, lisMap)
      result <- processVote(conversion, buildPositions, buildSenateBillNaturalKey(dto), correlationId, logCtx)
    } yield result
  }

  // =========================================================================
  // Common flow: resolve bill, detect change, persist + emit
  // =========================================================================

  /**
   * Common tail that both chamber paths converge on: resolve the bill, detect change, persist per the decision matrix,
   * mark stance, emit event. The caller supplies a `buildPositions: Long => List[VotePositionDO]` factory that
   * materializes position rows given the DB-assigned vote id — so no VotePositionDO exists until the vote row is
   * actually persisted.
   */
  private[pipeline] def processVote(
    conversion: VoteConversionResult,
    buildPositions: Long => List[VotePositionDO],
    dtoBillNaturalKey: Option[String],
    correlationId: UUID,
    logCtx: LogContext,
  ): F[ProcessingResult] = {
    // Either chamber may override the conversion's bill natural key with a DTO-derived one; House relies on the
    // shared conversions pathway (which doesn't populate billNaturalKey unless legislationType is known), and Senate
    // relies on its converter's classifyDocument output (already attached to the VoteConversionResult). In practice
    // one of the two is always None for a given vote.
    val billNaturalKey = dtoBillNaturalKey.orElse(conversion.billNaturalKey)

    for {
      billId <- resolveOptionalBillId(billNaturalKey, logCtx)
      voteWithBill = conversion.vote.copy(billId = billId)
      stored <- findStoredVote(voteWithBill.naturalKey)
      // Build the comparison-only position list with the stored row's voteId when one exists (real id → clean
      // structural diff) and a sentinel 0L otherwise (the detector short-circuits to New before the positions are
      // inspected). No VotePositionDO with a fake voteId ever reaches persist.
      comparisonVoteId    = stored.map(_.voteId).getOrElse(0L)
      comparisonPositions = buildPositions(comparisonVoteId)
      report <- changeDetector.detect(voteWithBill, comparisonPositions, correlationId)
      result <- dispatchOnReport(
        voteWithBill,
        buildPositions,
        billNaturalKey,
        stored,
        report,
        correlationId,
        logCtx,
      )
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
          _         <- maybeMarkStance(persisted.billId, logCtx)
          _         <- emitEvent(persisted, billNaturalKey, isUpdate = false, correlationId, logCtx)
        } yield ProcessingResult.Succeeded(voteDo.naturalKey, eventEmitted = true)

      case VoteChangeReport.Updated(true, _) =>
        // Stored must be Some here — the detector only returns Updated when a row was found.
        val storedVoteId = stored.map(_.voteId).getOrElse(0L)
        for {
          persisted <- persister.persistUpdate(voteDo, storedVoteId, buildPositions)
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

  // =========================================================================
  // Bill + member resolution — two-step (ensureExists + lookup) inlined here
  // =========================================================================

  /**
   * Resolve an optional bill natural key to a `bills.id` Long, creating a placeholder bill row first if the bill isn't
   * already present. `None` input short-circuits to `None` output (procedural votes). Raises [[BillResolutionFailed]]
   * only on the pathological "placeholder inserted then immediately absent" path, which indicates another actor deleted
   * the row between our two operations.
   */
  private[pipeline] def resolveOptionalBillId(
    maybeKey: Option[String],
    logCtx: LogContext,
  ): F[Option[Long]] =
    maybeKey match {
      case None => Async[F].pure(None)
      case Some(nk) =>
        for {
          _       <- placeholderCreator.ensureExists[BillDO](nk, billEntityRepo)
          maybeId <- billRepo.findByBillId(nk).map(_.map(_.billId)).transact(xa)
          id <- maybeId match {
            case Some(id) => Async[F].pure(id)
            case None =>
              val err = BillResolutionFailed(nk, "findByBillId returned None after ensureExists")
              logger.error(logCtx, err.getMessage, Some(err)) *> Async[F].raiseError[Long](err)
          }
        } yield Some(id)
    }

  /**
   * Resolve every House bioguide from the conversion result to a `members.id` Long, creating placeholder members when
   * needed. Returns a `Map[bioguide, memberId]` for the caller to thread into position-building. Senate positions use
   * [[buildSenatePositions]] instead and do not go through this path.
   */
  private[pipeline] def resolveHouseMembers(
    positions: List[UnresolvedVotePosition],
    logCtx: LogContext,
  ): F[Map[String, Long]] = {
    val distinctBioguides = positions.iterator
      .flatMap(_.memberSource.left.toOption)
      .filter(_.nonEmpty)
      .toList
      .distinct

    distinctBioguides
      .traverse(bid => resolveMemberId(bid, logCtx).map(id => bid -> id))
      .map(_.toMap)
  }

  private def resolveMemberId(bioguideId: String, logCtx: LogContext): F[Long] =
    for {
      _       <- placeholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)
      maybeId <- memberRepo.findByBioguideId(bioguideId).map(_.map(_.memberId)).transact(xa)
      id <- maybeId match {
        case Some(id) => Async[F].pure(id)
        case None =>
          val err = MemberResolutionFailed(bioguideId, "findByBioguideId returned None after ensureExists")
          logger.error(logCtx, err.getMessage, Some(err)) *> Async[F].raiseError[Long](err)
      }
    } yield id

  // =========================================================================
  // Position builders — pure functions invoked with the real voteId at persist time
  // =========================================================================

  /**
   * Build the House-arm position list with the real `voteId` and the pre-resolved `members.id` for every bioguide.
   * Positions whose bioguide failed to resolve (defensive — shouldn't happen since [[resolveHouseMembers]] raises) are
   * dropped. Positions with `memberSource = Right(_)` are also dropped — the House path never emits Right.
   */
  private[pipeline] def buildHousePositions(
    voteId: Long,
    positions: List[UnresolvedVotePosition],
    memberMap: Map[String, Long],
  ): List[VotePositionDO] =
    positions.flatMap { up =>
      up.memberSource match {
        case Left(bioguide) =>
          memberMap.get(bioguide).map { memberId =>
            VotePositionDO(
              id = 0L,
              voteId = voteId,
              memberId = Some(memberId),
              position = up.voteCast,
              partyAtVote = up.partyAtVote,
              stateAtVote = up.stateAtVote,
              createdAt = None,
              lisMemberId = None,
            )
          }
        case Right(_) => None
      }
    }

  /**
   * Build the Senate-arm position list with the real `voteId` and the pre-resolved `lis_members.id` for every LIS
   * natural key. Positions whose LIS id is missing from `lisMap` are dropped defensively (the LIS resolver should have
   * populated it for every senator seen on the DTO).
   */
  private[pipeline] def buildSenatePositions(
    voteId: Long,
    positions: List[UnresolvedVotePosition],
    lisMap: Map[String, Long],
  ): List[VotePositionDO] =
    positions.flatMap { up =>
      up.memberSource match {
        case Right(lisNaturalKey) =>
          lisMap.get(lisNaturalKey).map { lisMemberId =>
            VotePositionDO(
              id = 0L,
              voteId = voteId,
              memberId = None,
              position = up.voteCast,
              partyAtVote = up.partyAtVote,
              stateAtVote = up.stateAtVote,
              createdAt = None,
              lisMemberId = Some(lisMemberId),
            )
          }
        case Left(_) => None
      }
    }

  // =========================================================================
  // Side effects on success: stance mark + event emit
  // =========================================================================

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

  // =========================================================================
  // Natural key + bill-key helpers
  // =========================================================================

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
   * votes (no legislation metadata), returns `None`.
   */
  private[pipeline] def buildHouseBillNaturalKey(dto: VoteMembersDTO): Option[String] =
    for {
      t <- dto.legislationType
      n <- dto.legislationNumber
    } yield s"${dto.congress.toString}-${t.toUpperCase}-$n"

  /**
   * For Senate votes, the bill natural key is produced by `SenateVoteConverter.classifyDocument` during conversion and
   * attached to the [[VoteConversionResult]]. This method is the no-op hook that keeps the `processVote` signature
   * symmetric with the House path — Senate never overrides the converter's choice.
   */
  private[pipeline] def buildSenateBillNaturalKey(dto: SenateVoteXmlDTO): Option[String] = {
    val _ = dto
    None
  }

}
