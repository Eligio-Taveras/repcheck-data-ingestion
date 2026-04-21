package repcheck.ingestion.members.profile.api

import java.util.UUID

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal
import cats.syntax.all._

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.headers.Accept
import org.http4s.{MediaType, Uri}

import repcheck.ingestion.common.api.{CongressGovClientConfig, CongressGovPaginatedClient, FetchParams, PagedResponse}
import repcheck.ingestion.members.profile.errors.{MemberFetchFailed, MembersApiErrorClassifier, MembersApiHttpError}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.common.PaginationInfoDTO
import repcheck.shared.models.congress.dto.member.{
  MemberDepictionDTO,
  MemberDetailDTO,
  MemberListItemDTO,
  MemberTermSummaryDTO,
}

class MembersApiClient[F[_]](
  config: CongressGovClientConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
  temporalInstance: Temporal[F],
) extends CongressGovPaginatedClient[F, MemberListItemDTO] {

  override protected def pageDelay: FiniteDuration = config.pageDelay

  implicit override protected def temporal: Temporal[F] = temporalInstance

  private def raiseApiError[A](response: org.http4s.Response[F]): F[A] =
    response
      .as[String]
      .recover { case _ => response.status.reason }
      .flatMap(body => temporal.raiseError[A](MembersApiHttpError(response.status.code, body)))

  private def parseUri(raw: String): F[Uri] =
    Uri.fromString(raw) match {
      case Right(uri) => temporal.pure(uri)
      case Left(err)  => temporal.raiseError(MemberFetchFailed(None, err.sanitized, Some(err)))
    }

  override def fetchPage(params: FetchParams): F[PagedResponse[MemberListItemDTO]] =
    params.congress match {
      case None           => temporal.raiseError(MemberFetchFailed(None, "congress is required for member fetch"))
      case Some(congress) => fetchPageForCongress(congress, params)
    }

  // The Congress.gov member list endpoint is scoped by congress: `/v3/member/congress/{congress}`. Per acceptance
  // criteria 5.1, this client only supports the by-congress form — there is no "all members across all congresses"
  // endpoint we target. Back-filling older congresses is a pipeline concern: the orchestrator iterates the configured
  // congress range (e.g., 118, 119) and invokes the client once per congress, not a single unfiltered call.
  private def fetchPageForCongress(congress: Int, params: FetchParams): F[PagedResponse[MemberListItemDTO]] =
    parseUri(s"${config.baseUrl}/member/congress/$congress").flatMap { baseUri =>
      val uri = baseUri
        .withQueryParam("api_key", config.apiKey)
        .withQueryParam("format", "json")
        .withQueryParam("offset", params.offset)
        .withQueryParam("limit", params.pageSize)

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        if (response.status.isSuccess) {
          response.as[MembersListWrapper].map { listResponse =>
            PagedResponse(
              items = listResponse.members,
              totalCount = listResponse.pagination.flatMap(_.count).getOrElse(listResponse.members.size),
              nextOffset = if (listResponse.members.size < params.pageSize) { None }
              else { Some(params.offset + params.pageSize) },
            )
          }
        } else {
          raiseApiError(response)
        }
      }

      retryWrapper.withRetry(
        operation = operation,
        config = config.retry,
        classifier = MembersApiErrorClassifier,
        errorFactory = (msg, cause) =>
          MemberFetchFailed(
            bioguideId = None,
            detail = s"${uri.renderString}: $msg",
            cause = Option(cause),
          ),
        correlationId = UUID.randomUUID(),
      )
    }

  def fetchDetail(detailUrl: String): F[MemberDetailDTO] =
    parseUri(detailUrl).flatMap { baseUri =>
      val uri = baseUri.withQueryParam("api_key", config.apiKey).withQueryParam("format", "json")

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        if (response.status.isSuccess) {
          response.as[MemberDetailWrapper].map(_.member)
        } else {
          raiseApiError(response)
        }
      }

      retryWrapper.withRetry(
        operation = operation,
        config = config.retry,
        classifier = MembersApiErrorClassifier,
        errorFactory = (msg, cause) =>
          MemberFetchFailed(
            bioguideId = None,
            detail = s"${uri.renderString}: $msg",
            cause = Option(cause),
          ),
        correlationId = UUID.randomUUID(),
      )
    }

}

object MembersApiClient {

  def apply[F[_]: Temporal](
    config: CongressGovClientConfig,
    client: Client[F],
    retryWrapper: RetryWrapper[F],
  ): MembersApiClient[F] =
    new MembersApiClient[F](config, client, retryWrapper, Temporal[F])

}

/** Congress.gov wraps the member list in `{"members": [...], "pagination": {...}}`. */
final private[api] case class MembersListWrapper(
  members: List[MemberListItemDTO],
  pagination: Option[PaginationInfoDTO],
)

private[api] object MembersListWrapper {

  // The Congress.gov `/member/congress/{congress}` response wraps nested lists in a `{"item": [...]}` envelope:
  //   "terms": { "item": [ {"chamber": "House", "startYear": 2021} ] }
  // The shared-models `MemberListItemDTO.decoder` (deriveDecoder) expects a plain array, so we shadow it
  // here with a custom decoder that unwraps `terms.item` before delegating everything else.
  implicit private val memberListItemDecoder: Decoder[MemberListItemDTO] = Decoder.instance { c =>
    for {
      bioguideId <- c.downField("bioguideId").as[String]
      name       <- c.downField("name").as[Option[String]]
      partyName  <- c.downField("partyName").as[Option[String]]
      state      <- c.downField("state").as[Option[String]]
      depiction  <- c.downField("depiction").as[Option[MemberDepictionDTO]]
      terms      <- c.downField("terms").downField("item").as[Option[List[MemberTermSummaryDTO]]]
      updateDate <- c.downField("updateDate").as[Option[String]]
      url        <- c.downField("url").as[Option[String]]
    } yield MemberListItemDTO(bioguideId, name, partyName, state, depiction, terms, updateDate, url)
  }

  implicit val decoder: Decoder[MembersListWrapper] = deriveDecoder[MembersListWrapper]
}

/** Congress.gov wraps the member detail in `{"member": {...}}`. */
final private[api] case class MemberDetailWrapper(member: MemberDetailDTO)

private[api] object MemberDetailWrapper {

  // The Congress.gov member detail endpoint uses plain JSON arrays for nested collections
  // (e.g., "terms": [...], "partyHistory": [...]), so the shared-models deriveDecoder for
  // MemberDetailDTO handles them correctly without any custom unwrapping.
  implicit val decoder: Decoder[MemberDetailWrapper] = Decoder.instance { c =>
    c.downField("member").as[MemberDetailDTO].map(MemberDetailWrapper.apply)
  }

}
