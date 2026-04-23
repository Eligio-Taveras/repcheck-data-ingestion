package repcheck.ingestion.votes.app

import cats.effect.{Async, Temporal}

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.DoobieBillRepository
import repcheck.ingestion.common.logging.PipelineLogger
import repcheck.ingestion.common.placeholders.{DefaultPlaceholderCreator, DoobieEntityRepository}
import repcheck.ingestion.votes.api.HouseVotesApiClient
import repcheck.ingestion.votes.lis.LisResolver
import repcheck.ingestion.votes.pipeline.{
  BillLookup,
  HouseVoteConverter,
  MemberLookup,
  SenateVoteConverter,
  VoteChangeDetector,
  VoteEventEmitter,
  VotePersister,
  VoteProcessor,
}
import repcheck.ingestion.votes.repo.{
  DoobieStanceMaterializationStatusRepository,
  DoobieVoteHistoryArchiver,
  DoobieVotePositionRepository,
  DoobieVoteRepository,
  VotePositionRepository,
  VoteRepository,
}
import repcheck.ingestion.votes.xml.SenateVoteXmlClient
import repcheck.members.common.MemberInsertSql
import repcheck.members.common.persistence.{DoobieLisMemberRepository, DoobieMemberRepository, MemberWriteInstances}
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dos.vote.{VoteDO, VotePositionDO}

/**
 * Factory that assembles the full [[VoteProcessor]] dependency graph from a [[VotesPipelineResources.Resources]] bundle
 * plus top-level [[VotesPipeline.AppConfig]]. Pure — no IO, no Resource acquisition — so tests can exercise the entire
 * construction path deterministically by passing a resource bundle backed by mocks.
 *
 * Split out of [[VotesPipeline]] so the wiring remains focused on a single concern (who gets what) independent of the
 * launcher contract and of resource lifecycle.
 *
 * ==What's constructed here, in order==
 *
 *   1. Zero-arg Doobie repositories (votes, vote_positions, vote_history, stance_materialization_status, members,
 *      lis_members, bills). All thread their ConnectionIO through the shared transactor. 2. Placeholder machinery: a
 *      [[DefaultPlaceholderCreator]] plus a `DoobieEntityRepository[F, MemberDO]` keyed by the members table's
 *      `natural_key` column. Bills are handled differently: [[BillLookup]] talks directly to
 *      `BillRepository.upsertPlaceholder` which lives in bills-common (composite-key schema — see
 *      `DoobieBillRepository.upsertPlaceholder`). 3. A no-op `RetryWrapper[F]` for API clients. 4.
 *      [[HouseVotesApiClient]] + [[SenateVoteXmlClient]], each handed its own pre-rate-limited `Client[F]` from
 *      [[VotesPipelineResources.Resources]]. 5. [[LisResolver]] via its `apply` factory. 6. The per-vote collaborators:
 *      [[BillLookup]], [[MemberLookup]], [[HouseVoteConverter]], [[SenateVoteConverter]], [[VotePersister]],
 *      [[VoteEventEmitter]]. 7. Stored-state callbacks that wrap Doobie reads into `F[...]` so [[VoteChangeDetector]]
 *      stays unit-testable. 8. The [[VoteProcessor]] itself — consumes all of the above.
 */
private[votes] object VotesProcessorFactory {

  def build[F[_]: Async](
    config: VotesPipeline.AppConfig,
    resources: VotesPipelineResources.Resources[F],
    logger: PipelineLogger[F],
  ): VoteProcessor[F] = {
    import MemberWriteInstances._ // provides Write[MemberDO] for DoobieEntityRepository

    // Zero-arg Doobie repositories
    val voteRepo      = new DoobieVoteRepository
    val positionRepo  = new DoobieVotePositionRepository
    val historyRepo   = new DoobieVoteHistoryArchiver
    val stanceRepo    = new DoobieStanceMaterializationStatusRepository
    val memberRepo    = new DoobieMemberRepository
    val lisMemberRepo = new DoobieLisMemberRepository
    val billRepo      = new DoobieBillRepository

    // Placeholder machinery. Members use the generic DoobieEntityRepository + MemberInsertSql; bill placeholders are
    // handled directly by BillRepository.upsertPlaceholder (see bills-common) — see BillLookup below.
    val placeholderCreator = new DefaultPlaceholderCreator[F]
    val memberEntityRepo   = new DoobieEntityRepository[F, MemberDO](resources.xa, MemberInsertSql.value)

    // API clients. Each receives a pre-rate-limited Client[F] from `resources` — no client re-wraps internally.
    // The shared `resources.retryWrapper` is used for both (no-op log callback — per-retry logs come from the
    // structured PipelineLogger at each client's boundary instead).
    val houseApiClient = new HouseVotesApiClient[F](
      config = config.congressApi,
      houseConfig = config.pipeline.house,
      client = resources.houseClient,
      retryWrapper = resources.retryWrapper,
      temporalInstance = Temporal[F],
    )
    val senateXmlClient = new SenateVoteXmlClient[F](
      httpClient = resources.senateClient,
      retryWrapper = resources.retryWrapper,
      config = config.pipeline.senate,
      logger = logger,
    )

    // `lisResolver` is the Senate-only LIS identity adapter. `VoteProcessor.processSenateVote` calls
    // `lisResolver.resolve(dto)` once per senate.gov XML vote to upsert one `lis_members` row per senator seen in the
    // DTO (keyed by senate.gov's LIS natural key — e.g. "S428") and returns a `Map[String, Long]` from that natural
    // key to the surrogate `lis_members.id`. The map feeds `VotePositionBuilders.senate` so the resulting
    // `VotePositionDO` rows populate `lis_member_id` (the Senate arm of the dual-identity FK added in migration 023).
    // The resolver never touches `members` or `member_lis_mapping` — bioguide-side mapping is handled by
    // `lis-mapping-refresher`; the House arm uses `memberLookup` below instead.
    val lisResolver = LisResolver[F](lisMemberRepo, resources.xa, logger)

    // Processor collaborators.
    val billLookup = new BillLookup[F](billRepo = billRepo, xa = resources.xa, logger = logger)
    val memberLookup = new MemberLookup[F](
      memberRepo = memberRepo,
      memberEntityRepo = memberEntityRepo,
      placeholderCreator = placeholderCreator,
      xa = resources.xa,
      logger = logger,
    )
    val houseConverter  = new HouseVoteConverter[F](logger)
    val senateConverter = new SenateVoteConverter[F](logger, config.pipeline.senate.baseUrl)
    val persister = new VotePersister[F](
      voteRepo = voteRepo,
      positionRepo = positionRepo,
      historyArchiver = historyRepo,
      xa = resources.xa,
    )
    val eventEmitter = new VoteEventEmitter[F](
      stanceRepo = stanceRepo,
      eventPublisher = resources.eventPublisher,
      xa = resources.xa,
      logger = logger,
    )

    // Stored-state callbacks for VoteChangeDetector and VoteProcessor. Both wrap the Doobie read as a transaction
    // against the shared transactor so detector logic stays unit-testable (it receives an `F[...]`, not a ConnectionIO).
    val findStoredVote      = buildFindStoredVote[F](voteRepo, resources.xa)
    val findStoredPositions = buildFindStoredPositions[F](positionRepo, resources.xa)

    val changeDetector = new VoteChangeDetector[F](findStoredVote, findStoredPositions, logger)

    new VoteProcessor[F](
      houseClient = houseApiClient,
      senateClient = senateXmlClient,
      lisResolver = lisResolver,
      houseConverter = houseConverter,
      senateConverter = senateConverter,
      changeDetector = changeDetector,
      persister = persister,
      billLookup = billLookup,
      memberLookup = memberLookup,
      eventEmitter = eventEmitter,
      findStoredVote = findStoredVote,
      houseConfig = config.pipeline.house,
      senateConfig = config.pipeline.senate,
      congress = config.pipeline.house.congress,
      session = config.pipeline.house.session,
      logger = logger,
    )
  }

  /**
   * Build the `findStoredVote` callback threaded into `VoteChangeDetector` and `VoteProcessor`. Extracted from
   * [[build]] so the callback's behavior is directly unit-testable — tests can invoke it against a mocked
   * [[VoteRepository]] + an in-memory transactor to verify the `findByNaturalKey → transact(xa)` wrapping.
   */
  private[app] def buildFindStoredVote[F[_]: Async](
    voteRepo: VoteRepository,
    xa: Transactor[F],
  ): String => F[Option[VoteDO]] =
    nk => voteRepo.findByNaturalKey(nk).transact(xa)

  /**
   * Build the `findStoredPositions` callback threaded into `VoteChangeDetector`. Extracted from [[build]] for the same
   * testability reason as [[buildFindStoredVote]] — tests invoke it against a mocked [[VotePositionRepository]] + an
   * in-memory transactor to verify `findByVoteId → transact(xa)` wrapping.
   */
  private[app] def buildFindStoredPositions[F[_]: Async](
    positionRepo: VotePositionRepository,
    xa: Transactor[F],
  ): Long => F[List[VotePositionDO]] =
    voteId => positionRepo.findByVoteId(voteId).transact(xa)

}
