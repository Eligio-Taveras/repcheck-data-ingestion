package com.repcheck.bills.metadata.pipeline

import java.time.LocalDate

import cats.effect.Async
import cats.syntax.all._

import doobie.ConnectionIO
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO}
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.bill.CoSponsorDTO

import com.repcheck.bills.common.persistence.{MemberLookupRepository, TransactionRunner}

/**
 * Resolves Congress.gov bioguide IDs to internal member IDs, creating placeholder member rows when needed. Handles both
 * bill sponsors and cosponsors.
 */
private[pipeline] class MemberResolver[F[_]: Async](
  memberLookupRepo: MemberLookupRepository[ConnectionIO],
  placeholderCreator: PlaceholderCreator[F],
  memberEntityRepo: EntityRepository[F, MemberDO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
) {

  /**
   * Ensures a placeholder member row exists for the bill's sponsor, then looks up the internal member ID. Returns a
   * copy of the bill with `sponsorMemberId` set (or `None` if the member could not be resolved).
   */
  private[pipeline] def ensureSponsorPlaceholder(
    billDO: BillDO,
    detail: repcheck.shared.models.congress.dto.bill.BillDetailDTO,
    logCtx: LogContext,
  ): F[BillDO] = {
    val sponsorBioguideId = detail.sponsors.flatMap(_.headOption).map(_.bioguideId)
    sponsorBioguideId match {
      case Some(bioguideId) =>
        for {
          _        <- placeholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)
          memberId <- TransactionRunner.run(xa)(memberLookupRepo.findIdByNaturalKey(bioguideId))
          _ <-
            if (memberId.isEmpty) {
              logger.warn(logCtx, s"Sponsor member ID not found after placeholder creation for $bioguideId")
            } else {
              Async[F].unit
            }
        } yield billDO.copy(sponsorMemberId = memberId)
      case None =>
        Async[F].pure(billDO)
    }
  }

  /**
   * Converts a list of cosponsor DTOs to domain objects by ensuring placeholder members exist, looking up their
   * internal IDs, and filtering out any that could not be resolved.
   */
  private[pipeline] def buildCosponsorDOs(
    cosponsorDTOs: List[CoSponsorDTO],
    logCtx: LogContext,
  ): F[List[BillCosponsorDO]] =
    cosponsorDTOs.traverseFilter { dto =>
      for {
        _        <- placeholderCreator.ensureExists[MemberDO](dto.bioguideId, memberEntityRepo)
        memberId <- TransactionRunner.run(xa)(memberLookupRepo.findIdByNaturalKey(dto.bioguideId))
        result <- memberId match {
          case Some(mid) =>
            Async[F].pure(
              Some(
                BillCosponsorDO(
                  billId = 0L,
                  memberId = mid,
                  isOriginalCosponsor = dto.isOriginalCosponsor,
                  sponsorshipDate = dto.sponsorshipDate.map(LocalDate.parse),
                )
              )
            )
          case None =>
            logger.warn(logCtx, s"Cosponsor member ID not found for ${dto.bioguideId}") *>
              Async[F].pure(Option.empty[BillCosponsorDO])
        }
      } yield result
    }

}
