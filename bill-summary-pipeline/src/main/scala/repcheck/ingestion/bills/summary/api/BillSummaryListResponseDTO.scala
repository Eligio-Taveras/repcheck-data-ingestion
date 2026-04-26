package repcheck.ingestion.bills.summary.api

import io.circe.Decoder

import repcheck.ingestion.common.api.PagedObject
import repcheck.shared.models.congress.dto.bill.BillSummaryDTO
import repcheck.shared.models.congress.dto.common.PaginationInfoDTO

/**
 * Wrapper for Congress.gov `/summaries[/{congress}]` paginated responses. The body shape is `{ summaries: [...],
 * pagination: {...} }` — semi-auto Circe wouldn't pick up the `summaries` field name automatically, so we provide a
 * manual decoder that downs-into `summaries`.
 *
 * Mirrors the pattern used by `BillListResponseDTO` (which downs into `bills` or falls back to `items`).
 */
final case class BillSummaryListResponseDTO(
  items: List[BillSummaryDTO],
  pagination: Option[PaginationInfoDTO],
) extends PagedObject[BillSummaryDTO]

object BillSummaryListResponseDTO {

  implicit val decoder: Decoder[BillSummaryListResponseDTO] = Decoder.instance { c =>
    for {
      items      <- c.downField("summaries").as[List[BillSummaryDTO]]
      pagination <- c.downField("pagination").as[Option[PaginationInfoDTO]]
    } yield BillSummaryListResponseDTO(items = items, pagination = pagination)
  }

}
