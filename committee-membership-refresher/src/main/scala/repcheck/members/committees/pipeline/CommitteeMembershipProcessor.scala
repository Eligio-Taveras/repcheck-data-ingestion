package repcheck.members.committees.pipeline

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{Async, Ref}
import cats.syntax.all._

import doobie.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.api.FetchParams
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.{
  CommitteeMetadataApiClient,
  HouseMemberDataXmlClient,
  SenateCommitteeMembershipXmlClient,
  SenateIdentityXmlClient,
}
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.errors.{CommitteeMemberUpsertFailed, CommitteeUpsertFailed}
import repcheck.members.committees.model._
import repcheck.members.committees.persistence.{CommitteeMemberRepository, CommitteeRepository}
import repcheck.members.common.persistence.MemberRepository

/**
 * Orchestrates the committee membership refresh in four phases:
 *
 * Phase 1: Fetch committee metadata from Congress.gov API (both chambers). Phase 2: Fetch House member data XML,
 * resolve bioguide -> memberId, upsert memberships. Phase 3: Fetch Senate identity XML to build identity map, then
 * fetch per-committee membership XML and upsert memberships. Phase 4: (Future) Emit events for downstream consumers.
 */
class CommitteeMembershipProcessor[F[_]: Async](
  committeeApiClient: CommitteeMetadataApiClient[F],
  houseXmlClient: HouseMemberDataXmlClient[F],
  senateIdentityClient: SenateIdentityXmlClient[F],
  senateCommitteeClient: SenateCommitteeMembershipXmlClient[F],
  committeeRepo: CommitteeRepository,
  committeeMemberRepo: CommitteeMemberRepository,
  memberRepo: MemberRepository,
  xa: Transactor[F],
  config: CommitteeMembershipConfig,
  logger: PipelineLogger[F],
) {

  private val StepName: String = "committee-membership-refresh"

  def refreshAll(runId: Long): F[CommitteeMembershipRefreshResult] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName)

    for {
      _               <- logger.info(logCtx, "Starting committee membership refresh")
      committeesCount <- phase1FetchCommitteeMetadata(logCtx)
      houseCount      <- phase2ProcessHouse(runId, logCtx)
      senateCount     <- phase3ProcessSenate(runId, logCtx)
      membershipCount = houseCount._2 + senateCount._2
      result = CommitteeMembershipRefreshResult(
        committeesUpserted = committeesCount,
        houseMembersProcessed = houseCount._1,
        senateMembersProcessed = senateCount._1,
        membershipRowsUpserted = membershipCount,
      )
      _ <- logger.info(
        logCtx,
        s"Committee membership refresh complete: committees=${result.committeesUpserted.toString} " +
          s"houseMembers=${result.houseMembersProcessed.toString} " +
          s"senateMembers=${result.senateMembersProcessed.toString} " +
          s"membershipRows=${result.membershipRowsUpserted.toString}",
      )
    } yield result
  }

  /** Phase 1: Fetch committee metadata from Congress.gov for both chambers. */
  private[pipeline] def phase1FetchCommitteeMetadata(logCtx: LogContext): F[Int] = {
    val counter = new AtomicInteger(0)
    for {
      _ <- logger.info(logCtx, "Phase 1: Fetching committee metadata from Congress.gov")
      _ <- fetchAndUpsertChamberCommittees("house", logCtx, counter)
      _ <- fetchAndUpsertChamberCommittees("senate", logCtx, counter)
      _ <- fetchAndUpsertChamberCommittees("joint", logCtx, counter)
      count = counter.get()
      _ <- logger.info(logCtx, s"Phase 1 complete: ${count.toString} committees upserted")
    } yield count
  }

  private def fetchAndUpsertChamberCommittees(
    chamber: String,
    logCtx: LogContext,
    counter: AtomicInteger,
  ): F[Unit] = {
    def fetchPage(offset: Int): F[Unit] = {
      val params = FetchParams(offset = offset, pageSize = 250, congress = None)
      for {
        _    <- logger.debug(logCtx, s"Fetching $chamber committees page offset=$offset")
        page <- committeeApiClient.fetchPageForChamber(chamber, params)
        _ <- page.items.traverse_ { item =>
          val committeeCode = CommitteeCodeNormalizer.fromCongressGov(item.systemCode, chamber)
          val parentCode =
            item.parent.flatMap(_.systemCode).map(pc => CommitteeCodeNormalizer.fromCongressGov(pc, chamber))
          val committeeType = item.committeeTypeCode.getOrElse(
            if (parentCode.isDefined) "Subcommittee" else "Standing"
          )
          val committeeDO = CommitteeDO(
            committeeCode = committeeCode,
            name = item.name,
            chamber = chamber.capitalize,
            committeeType = committeeType,
            parentCommitteeCode = parentCode,
            isCurrent = true,
            updateDate = item.updateDate.map(d => Instant.parse(s"${d}T00:00:00Z")).orElse(Some(Instant.now())),
            createdAt = None,
            updatedAt = None,
          )
          committeeRepo
            .upsert(committeeDO)
            .transact(xa)
            .handleErrorWith { error =>
              Async[F].raiseError(
                CommitteeUpsertFailed(committeeCode, Option(error.getMessage).getOrElse("unknown"), Some(error))
              )
            }
            .map(_ => counter.incrementAndGet())
        }
        _ <- page.nextOffset match {
          case Some(next) => fetchPage(next)
          case None       => Async[F].unit
        }
      } yield ()
    }

    fetchPage(0)
  }

  /** Phase 2: Process House member data XML. */
  private[pipeline] def phase2ProcessHouse(runId: Long, logCtx: LogContext): F[(Int, Int)] = {
    val memberCounter     = new AtomicInteger(0)
    val membershipCounter = new AtomicInteger(0)

    for {
      _ <- logger.info(logCtx, "Phase 2: Processing House member committee data")
      _ <- houseXmlClient
        .fetchMembers(runId)
        .evalMap(dto => resolveAndUpsertHouseMember(dto, logCtx, memberCounter, membershipCounter))
        .compile
        .drain
      mc  = memberCounter.get()
      msc = membershipCounter.get()
      _ <- logger.info(logCtx, s"Phase 2 complete: ${mc.toString} House members, ${msc.toString} membership rows")
    } yield (mc, msc)
  }

  private def resolveAndUpsertHouseMember(
    dto: HouseMemberDataXmlDTO,
    logCtx: LogContext,
    memberCounter: AtomicInteger,
    membershipCounter: AtomicInteger,
  ): F[Unit] = {
    val itemCtx = logCtx.copy(entityId = Some(dto.bioguideId))

    for {
      maybeMember <- memberRepo.findByBioguideId(dto.bioguideId).transact(xa)
      _ <- maybeMember match {
        case Some(member) =>
          val _ = memberCounter.incrementAndGet()
          dto.committees.traverse_ { assignment =>
            val committeeMemberDO = CommitteeMemberDO(
              id = 0L,
              committeeCode = assignment.committeeCode,
              memberId = member.memberId,
              position = None,
              side = assignment.side,
              rank = assignment.rank,
              congress = config.currentCongress,
              beginDate = None,
              endDate = None,
              createdAt = None,
              updatedAt = None,
            )
            for {
              _ <- committeeRepo.upsertPlaceholder(assignment.committeeCode, "House").transact(xa)
              _ <- committeeMemberRepo.upsert(committeeMemberDO).transact(xa).handleErrorWith { error =>
                Async[F].raiseError(
                  CommitteeMemberUpsertFailed(
                    assignment.committeeCode,
                    member.memberId,
                    Option(error.getMessage).getOrElse("unknown"),
                    Some(error),
                  )
                )
              }
            } yield {
              val _ = membershipCounter.incrementAndGet()
            }
          }
        case None =>
          logger.debug(itemCtx, s"Skipping House member ${dto.bioguideId} — not in members table")
      }
    } yield ()
  }

  /** Phase 3: Process Senate committee membership. Two passes: identity map, then per-committee. */
  private[pipeline] def phase3ProcessSenate(runId: Long, logCtx: LogContext): F[(Int, Int)] = {
    val memberCounter     = new AtomicInteger(0)
    val membershipCounter = new AtomicInteger(0)

    for {
      _ <- logger.info(logCtx, "Phase 3: Processing Senate committee membership")
      // Pass A: Build identity map from cvc_member_data.xml
      identityMap <- buildSenateIdentityMap(runId, logCtx)
      // Pass B: Fetch per-committee XML and resolve members
      parentCodes <- committeeRepo.findAllSenateParentCodes().transact(xa)
      _           <- logger.info(logCtx, s"Found ${parentCodes.size.toString} Senate parent committees to process")
      _ <- parentCodes.traverse_ { code =>
        senateCommitteeClient
          .fetchCommitteeMembers(code, runId)
          .evalMap { senMember =>
            resolveSenateCommitteeMember(senMember, identityMap, logCtx, memberCounter, membershipCounter)
          }
          .compile
          .drain
      }
      mc  = memberCounter.get()
      msc = membershipCounter.get()
      _ <- logger.info(logCtx, s"Phase 3 complete: ${mc.toString} Senate members, ${msc.toString} membership rows")
    } yield (mc, msc)
  }

  private[pipeline] def buildSenateIdentityMap(
    runId: Long,
    logCtx: LogContext,
  ): F[Map[(String, String, String), (String, Long)]] =
    for {
      ref <- Ref.of[F, Map[(String, String, String), (String, Long)]](Map.empty)
      _ <- senateIdentityClient
        .fetchIdentities(runId)
        .evalMap { identity =>
          memberRepo.findByBioguideId(identity.bioguideId).transact(xa).flatMap {
            case Some(member) =>
              val key = (identity.firstName.toLowerCase, identity.lastName.toLowerCase, identity.state.toUpperCase)
              ref.update(_.updated(key, (identity.bioguideId, member.memberId)))
            case None =>
              logger.debug(
                logCtx,
                s"Senate identity ${identity.bioguideId} not in members table, skipping identity map entry",
              )
          }
        }
        .compile
        .drain
      result <- ref.get
      _      <- logger.info(logCtx, s"Built Senate identity map with ${result.size.toString} entries")
    } yield result

  private def resolveSenateCommitteeMember(
    senMember: SenateCommitteeMemberXmlDTO,
    identityMap: Map[(String, String, String), (String, Long)],
    logCtx: LogContext,
    memberCounter: AtomicInteger,
    membershipCounter: AtomicInteger,
  ): F[Unit] = {
    // Try to resolve via bioguideId first, then by identity map
    val resolvedMemberId: F[Option[Long]] = senMember.bioguideId match {
      case Some(bio) =>
        memberRepo.findByBioguideId(bio).transact(xa).map(_.map(_.memberId))
      case None =>
        val key = (senMember.firstName.toLowerCase, senMember.lastName.toLowerCase, senMember.state.toUpperCase)
        Async[F].pure(identityMap.get(key).map(_._2))
    }

    for {
      maybeMemberId <- resolvedMemberId
      _ <- maybeMemberId match {
        case Some(memberId) =>
          val _ = memberCounter.incrementAndGet()
          val committeeMemberDO = CommitteeMemberDO(
            id = 0L,
            committeeCode = senMember.committeeCode,
            memberId = memberId,
            position = senMember.position,
            side = None,
            rank = senMember.rank,
            congress = config.currentCongress,
            beginDate = None,
            endDate = None,
            createdAt = None,
            updatedAt = None,
          )
          for {
            _ <- committeeRepo.upsertPlaceholder(senMember.committeeCode, "Senate").transact(xa)
            _ <- committeeMemberRepo.upsert(committeeMemberDO).transact(xa).handleErrorWith { error =>
              Async[F].raiseError(
                CommitteeMemberUpsertFailed(
                  senMember.committeeCode,
                  memberId,
                  Option(error.getMessage).getOrElse("unknown"),
                  Some(error),
                )
              )
            }
          } yield {
            val _ = membershipCounter.incrementAndGet()
          }
        case None =>
          logger.debug(
            logCtx,
            s"Could not resolve Senate member ${senMember.firstName} ${senMember.lastName} " +
              s"(${senMember.state}) for committee ${senMember.committeeCode}",
          )
      }
    } yield ()
  }

}
