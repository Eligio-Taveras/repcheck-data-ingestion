package repcheck.ingestion.amendments.api

import io.circe.Decoder

import repcheck.shared.models.congress.dto.amendment.AmendmentDetailDTO

/**
 * Wrapper for Congress.gov `/v3/amendment/{c}/{t}/{n}` responses. The body shape is `{ amendment: {...}, request: {...}
 * }` — we down into `amendment` to get the detail object.
 */
final case class AmendmentDetailApiResponseDTO(amendment: AmendmentDetailDTO)

object AmendmentDetailApiResponseDTO {

  implicit val decoder: Decoder[AmendmentDetailApiResponseDTO] = Decoder.instance { c =>
    c.downField("amendment").as[AmendmentDetailDTO].map(AmendmentDetailApiResponseDTO(_))
  }

}
