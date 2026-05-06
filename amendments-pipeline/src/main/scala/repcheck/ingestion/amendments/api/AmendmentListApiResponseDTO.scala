package repcheck.ingestion.amendments.api

import io.circe.Decoder

import repcheck.shared.models.congress.dto.amendment.AmendmentListItemDTO
import repcheck.shared.models.congress.dto.common.{PagedObject, PaginationInfoDTO}

/**
 * Wrapper for Congress.gov `/v3/amendment` paginated responses. The body shape is `{ amendments: [...], pagination:
 * {...} }` — semi-auto Circe wouldn't pick up the `amendments` field name automatically (the `AmendmentListResponseDTO`
 * in `repchecksharedmodels` uses a generic `items` field for cross-endpoint reuse), so we provide a manual decoder that
 * downs into `amendments`.
 *
 * Mirrors `BillSummaryListResponseDTO` in `bill-summary-pipeline`.
 */
final case class AmendmentListApiResponseDTO(
  items: List[AmendmentListItemDTO],
  pagination: Option[PaginationInfoDTO],
) extends PagedObject[AmendmentListItemDTO]

object AmendmentListApiResponseDTO {

  implicit val decoder: Decoder[AmendmentListApiResponseDTO] = Decoder.instance { c =>
    for {
      items      <- c.downField("amendments").as[List[AmendmentListItemDTO]]
      pagination <- c.downField("pagination").as[Option[PaginationInfoDTO]]
    } yield AmendmentListApiResponseDTO(items = items, pagination = pagination)
  }

}
