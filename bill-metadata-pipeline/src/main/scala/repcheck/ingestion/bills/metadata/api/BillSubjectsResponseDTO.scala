package repcheck.ingestion.bills.metadata.api

import io.circe.Decoder

import repcheck.shared.models.congress.dto.bill.LegislativeSubjectDTO
import repcheck.shared.models.congress.dto.common.PaginationInfoDTO

/**
 * Wire envelope for the Congress.gov `/bill/{congress}/{type}/{number}/subjects` sub-endpoint: `{ "subjects": {
 * "legislativeSubjects": [...], "policyArea": {...} }, "pagination": {...} }`.
 *
 * Local to the metadata client (the domain DTO `LegislativeSubjectDTO` lives in shared-models; this is only the
 * paginated response shape, mirroring `CosponsorListResponseDTO`). The bill DETAIL returns `subjects` as a
 * `{count,url}` ref, so the actual list must be fetched here. The decoder reads `subjects.legislativeSubjects` directly
 * and ignores `policyArea` (Congress.gov returns it as an object, not a string — we only need the subject list).
 */
final case class BillSubjectsResponseDTO(
  legislativeSubjects: List[LegislativeSubjectDTO],
  pagination: Option[PaginationInfoDTO],
)

object BillSubjectsResponseDTO {

  implicit val decoder: Decoder[BillSubjectsResponseDTO] = Decoder.instance { c =>
    for {
      subjects   <- c.downField("subjects").downField("legislativeSubjects").as[Option[List[LegislativeSubjectDTO]]]
      pagination <- c.downField("pagination").as[Option[PaginationInfoDTO]]
    } yield BillSubjectsResponseDTO(subjects.getOrElse(List.empty), pagination)
  }

}
