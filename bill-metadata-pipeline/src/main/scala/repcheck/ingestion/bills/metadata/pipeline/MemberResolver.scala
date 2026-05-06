package repcheck.ingestion.bills.metadata.pipeline

import java.time.LocalDate

import cats.effect.Async
import cats.syntax.all._

import doobie.util.transactor.Transactor

import repcheck.ingestion.bills.common.persistence.TransactionRunner
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.members.common.persistence.MemberRepository
import repcheck.shared.models.congress.dos.bill.{BillCosponsorDO, BillDO}
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.bill.CoSponsorDTO

private[pipeline] class MemberResolver[F[_]: Async](
  memberRepo: MemberRepository,
  placeholderCreator: PlaceholderCreator[F],
  memberEntityRepo: EntityRepository[F, MemberDO],
  xa: Transactor[F],
  logger: PipelineLogger[F],
) {

  private[pipeline] def ensureSponsorPlaceholder(
    billDO: BillDO,
    detail: repcheck.shared.models.congress.dto.bill.BillDetailDTO,
    logCtx: LogContext,
  ): F[BillDO] = {
    val sponsorBioguideId = detail.sponsors.flatMap(_.headOption).map(_.bioguideId)
    sponsorBioguideId match {
      case Some(bioguideId) =>
        for {
          _ <- logger.debug(
            logCtx,
            s"ensureSponsorPlaceholder.start bioguideId=$bioguideId sponsorListSize=${detail.sponsors.fold(0)(_.size).toString}",
          )
          _        <- logger.debug(logCtx, s"ensureSponsorPlaceholder.placeholder.start bioguideId=$bioguideId")
          _        <- placeholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)
          _        <- logger.debug(logCtx, s"ensureSponsorPlaceholder.placeholder.done bioguideId=$bioguideId")
          _        <- logger.debug(logCtx, s"ensureSponsorPlaceholder.lookup.start bioguideId=$bioguideId")
          memberId <- TransactionRunner.run(xa)(memberRepo.findByBioguideId(bioguideId).map(_.map(_.memberId)))
          _ <- logger.debug(
            logCtx,
            s"ensureSponsorPlaceholder.lookup.done bioguideId=$bioguideId memberId=${memberId.fold("none")(_.toString)}",
          )
          _ <-
            if (memberId.isEmpty) {
              logger.warn(logCtx, s"Sponsor member ID not found after placeholder creation for $bioguideId")
            } else {
              Async[F].unit
            }
        } yield billDO.copy(sponsorMemberId = memberId)
      case None =>
        logger.debug(logCtx, s"ensureSponsorPlaceholder.skip (no sponsor in detail)") *>
          Async[F].pure(billDO)
    }
  }

  private[pipeline] def buildCosponsorDOs(
    cosponsorDTOs: List[CoSponsorDTO],
    logCtx: LogContext,
  ): F[List[BillCosponsorDO]] = {
    val deduped        = cosponsorDTOs.distinctBy(_.bioguideId)
    val duplicateCount = cosponsorDTOs.size - deduped.size

    for {
      _ <- logger.debug(
        logCtx,
        s"buildCosponsorDOs.start raw=${cosponsorDTOs.size.toString} " +
          s"deduped=${deduped.size.toString} duplicates=${duplicateCount.toString}",
      )
      _ <-
        if (duplicateCount > 0) {
          val dupeIds = cosponsorDTOs
            .groupBy(_.bioguideId)
            .collect { case (id, list) if list.size > 1 => id }
            .toList
            .sorted
          logger.warn(
            logCtx,
            s"Congress.gov returned $duplicateCount duplicate cosponsor(s) for ${dupeIds.mkString(", ")} — deduping to first occurrence",
          )
        } else {
          Async[F].unit
        }
      result <- deduped.zipWithIndex.traverseFilter {
        case (dto, idx) =>
          for {
            _ <- logger.debug(
              logCtx,
              s"buildCosponsorDOs.cosponsor[${idx.toString}/${deduped.size.toString}].start " +
                s"bioguideId=${dto.bioguideId} sponsorshipDate=${dto.sponsorshipDate.fold("none")(identity)}",
            )
            _        <- placeholderCreator.ensureExists[MemberDO](dto.bioguideId, memberEntityRepo)
            _        <- logger.debug(logCtx, s"buildCosponsorDOs.cosponsor[${idx.toString}].placeholder.done")
            memberId <- TransactionRunner.run(xa)(memberRepo.findByBioguideId(dto.bioguideId).map(_.map(_.memberId)))
            _ <- logger.debug(
              logCtx,
              s"buildCosponsorDOs.cosponsor[${idx.toString}].lookup.done memberId=${memberId.fold("none")(_.toString)}",
            )
            maybeDO <- memberId match {
              case Some(mid) =>
                Async[F].pure[Option[BillCosponsorDO]](
                  Some(
                    BillCosponsorDO(
                      billId = 0L,
                      memberId = mid,
                      isOriginalCosponsor = dto.isOriginalCosponsor,
                      sponsorshipDate = dto.sponsorshipDate.filter(_.nonEmpty).map(LocalDate.parse),
                    )
                  )
                )
              case None =>
                logger.warn(logCtx, s"Cosponsor member ID not found for ${dto.bioguideId}") *>
                  Async[F].pure(Option.empty[BillCosponsorDO])
            }
          } yield maybeDO
      }
      _ <- logger.debug(
        logCtx,
        s"buildCosponsorDOs.done resolved=${result.size.toString}/${deduped.size.toString}",
      )
    } yield result
  }

}
