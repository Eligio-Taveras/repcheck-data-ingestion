package repcheck.members.committees.client

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
import repcheck.members.committees.errors.{CommitteeApiHttpError, CommitteeFetchFailed}
import repcheck.members.committees.model.{CommitteeListItemDTO, CommitteeParentDTO, CommitteeSubcommitteeDTO}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.common.PaginationInfoDTO

class CommitteeMetadataApiClient[F[_]](
  config: CongressGovClientConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
  temporalInstance: Temporal[F],
) extends CongressGovPaginatedClient[F, CommitteeListItemDTO] {

  override protected def pageDelay: FiniteDuration = config.pageDelay

  implicit override protected def temporal: Temporal[F] = temporalInstance

  private def raiseApiError[A](response: org.http4s.Response[F]): F[A] =
    response
      .as[String]
      .recover { case _ => response.status.reason }
      .flatMap(body => temporal.raiseError[A](CommitteeApiHttpError(response.status.code, body)))

  private def parseUri(raw: String): F[Uri] =
    Uri.fromString(raw) match {
      case Right(uri) => temporal.pure(uri)
      case Left(err)  => temporal.raiseError(CommitteeFetchFailed(err.sanitized, Some(err)))
    }

  override def fetchPage(params: FetchParams): F[PagedResponse[CommitteeListItemDTO]] = {
    val chamber = params.congress match {
      case Some(c) => c.toString
      case None    => "house"
    }
    fetchPageForChamber(chamber, params)
  }

  def fetchPageForChamber(chamber: String, params: FetchParams): F[PagedResponse[CommitteeListItemDTO]] =
    parseUri(s"${config.baseUrl}/committee/$chamber").flatMap { baseUri =>
      val uri = baseUri
        .withQueryParam("api_key", config.apiKey)
        .withQueryParam("format", "json")
        .withQueryParam("offset", params.offset)
        .withQueryParam("limit", params.pageSize)

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        if (response.status.isSuccess) {
          response.as[CommitteeListWrapper].map { listResponse =>
            PagedResponse(
              items = listResponse.committees,
              totalCount = listResponse.pagination.flatMap(_.count).getOrElse(listResponse.committees.size),
              nextOffset = if (listResponse.committees.size < params.pageSize) { None }
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
        classifier = CommitteeApiErrorClassifier,
        errorFactory = (msg, cause) =>
          CommitteeFetchFailed(
            detail = s"${uri.removeQueryParam("api_key").renderString}: $msg",
            cause = Option(cause),
          ),
        correlationId = UUID.randomUUID(),
      )
    }

}

object CommitteeMetadataApiClient {

  def apply[F[_]: Temporal](
    config: CongressGovClientConfig,
    client: Client[F],
    retryWrapper: RetryWrapper[F],
  ): CommitteeMetadataApiClient[F] =
    new CommitteeMetadataApiClient[F](config, client, retryWrapper, Temporal[F])

}

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

private[client] object CommitteeApiErrorClassifier extends repcheck.pipeline.models.errors.ErrorClassifier {

  import repcheck.pipeline.models.errors.ErrorClass

  override def classify(error: Throwable): ErrorClass = error match {
    case e: CommitteeApiHttpError if isTransient(e.statusCode) => ErrorClass.Transient
    case _: java.net.ConnectException                          => ErrorClass.Transient
    case _: java.net.SocketTimeoutException                    => ErrorClass.Transient
    case _: org.http4s.InvalidMessageBodyFailure               => ErrorClass.Systemic
    case _                                                     => ErrorClass.Systemic
  }

  private def isTransient(status: Int): Boolean =
    status == 429 || status == 500 || status == 502 || status == 503 || status == 504

}
