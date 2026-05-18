package repcheck.members.committees.client

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

import repcheck.members.committees.model.{CommitteeListItemDTO, CommitteeParentDTO, CommitteeSubcommitteeDTO}
import repcheck.shared.models.congress.dto.common.PaginationInfoDTO

final private[client] case class CommitteeListWrapper(
  committees: List[CommitteeListItemDTO],
  pagination: Option[PaginationInfoDTO],
)

private[client] object CommitteeListWrapper {

  implicit val parentDecoder: Decoder[CommitteeParentDTO]             = deriveDecoder[CommitteeParentDTO]
  implicit val subcommitteeDecoder: Decoder[CommitteeSubcommitteeDTO] = deriveDecoder[CommitteeSubcommitteeDTO]
  implicit val committeeDecoder: Decoder[CommitteeListItemDTO]        = deriveDecoder[CommitteeListItemDTO]
  implicit val decoder: Decoder[CommitteeListWrapper]                 = deriveDecoder[CommitteeListWrapper]

}
