package repcheck.ingestion.bills.metadata.pipeline

import java.time.Instant

import cats.effect.Async
import cats.syntax.all._

import repcheck.ingestion.bills.metadata.api.BillsApiClient
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.shared.models.congress.dos.bill.BillSubjectDO
import repcheck.shared.models.congress.dto.bill.{BillDetailDTO, CoSponsorDTO}

/**
 * Retrieves a bill's API sub-resources — cosponsors (from the detail's cosponsors ref url) and legislative subjects
 * (from the `/subjects` sub-endpoint, by coordinate) — mapping subjects to DOs. Extracted from
 * [[BillMetadataProcessor]] so the processor owns orchestration (fetch detail → resolve members → persist) and this
 * owns sub-resource fetching.
 */
class BillSubResourceFetcher[F[_]: Async](
  apiClient: BillsApiClient[F],
  logger: PipelineLogger[F],
) {

  /** Cosponsors come from the detail's `cosponsors.url` ref; absent ref → empty. */
  def fetchCosponsors(detail: BillDetailDTO, logCtx: LogContext): F[List[CoSponsorDTO]] =
    detail.cosponsors.flatMap(_.url) match {
      case Some(url) =>
        for {
          _      <- logger.debug(logCtx, s"fetchCosponsors.url=$url")
          result <- apiClient.fetchCosponsors(url)
          _      <- logger.debug(logCtx, s"fetchCosponsors.done count=${result.size.toString}")
        } yield result
      case None =>
        logger.debug(logCtx, "fetchCosponsors.skip (no cosponsors URL on detail)") *>
          Async[F].pure(List.empty[CoSponsorDTO])
    }

  /**
   * Subjects come from the `/subjects` sub-endpoint (the detail carries only a `{count,url}` ref that decodes empty).
   * `billId` is a 0L placeholder rewritten by `BillPersister` before `replaceAll`; embeddings are populated later
   * (D16).
   */
  def fetchSubjects(detail: BillDetailDTO, logCtx: LogContext): F[List[BillSubjectDO]] =
    for {
      _ <- logger.debug(logCtx, s"fetchSubjects.bill=${detail.congress.toString}/${detail.billType}/${detail.number}")
      dtos <- apiClient.fetchSubjects(detail.congress, detail.billType, detail.number)
      _    <- logger.debug(logCtx, s"fetchSubjects.done count=${dtos.size.toString}")
    } yield dtos.map(d => BillSubjectDO(0L, d.name, None, d.updateDate.flatMap(parseInstant)))

  private def parseInstant(dateStr: String): Option[Instant] =
    scala.util.Try(Instant.parse(dateStr)).toOption

}
