package repcheck.ingestion.amendments.textcheck.api

import java.util.UUID

import cats.effect.Temporal
import cats.syntax.all._

import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.headers.Accept
import org.http4s.{MediaType, Status, Uri}

import repcheck.ingestion.amendments.textcheck.errors.{
  AmendmentTextCheckErrorClassifier,
  AmendmentTextCheckFailed,
  AmendmentTextCheckHttpError,
}
import repcheck.ingestion.common.api.CongressGovClientConfig
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.dto.amendment.{AmendmentTextItemDTO, AmendmentTextResponseDTO}

/**
 * Single-shot GET against Congress.gov's `/v3/amendment/{congress}/{type}/{number}/text` endpoint. Mirrors the
 * `BillTextApiClient` shape: builds the URL with `api_key` + `format=json`, decodes the
 * `{"textVersions":[...],"pagination":{...}}` envelope into `List[AmendmentTextItemDTO]`, and wraps the call with
 * [[RetryWrapper]] + [[AmendmentTextCheckErrorClassifier]].
 *
 * URL casing: path lowercase (`/amendment/{c}/{type-lower}/{number}/text`); query parameters camelCase. `api_key` is
 * passed as a query parameter (Congress.gov's documented auth scheme).
 *
 * 404 maps to `Right(Nil)` — many House amendments have no text granules upstream, so a 404 is a normal "no text
 * available" signal, not an error. Other non-success responses raise [[AmendmentTextCheckHttpError]] which the
 * classifier maps to Transient (5xx/429) or Systemic (everything else); on retry exhaustion the wrapper rewraps as
 * [[AmendmentTextCheckFailed]].
 */
class AmendmentTextApiClient[F[_]: Temporal](
  config: CongressGovClientConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
) {

  private def parseUri(raw: String, naturalKey: String): F[Uri] =
    Uri.fromString(raw) match {
      case Right(uri) => Temporal[F].pure(uri)
      case Left(err) =>
        Temporal[F].raiseError(
          AmendmentTextCheckFailed(
            naturalKey = naturalKey,
            detail = s"Invalid URI: ${err.sanitized}",
            cause = new IllegalArgumentException(err.sanitized),
          )
        )
    }

  private def raiseApiError[A](response: org.http4s.Response[F]): F[A] =
    response
      .as[String]
      .recover { case _ => response.status.reason }
      .flatMap(body => Temporal[F].raiseError[A](AmendmentTextCheckHttpError(response.status.code, body, attempt = 1)))

  /**
   * Fetch all text versions for the given amendment. Returns `Nil` on 404 (amendment has no text granules).
   *
   * @param correlationId
   *   Per-amendment correlation ID propagated from the calling pipeline so retry-wrapper logs share context with
   *   surrounding work for that amendment. Do NOT mint a fresh UUID here.
   */
  def fetchTextVersions(
    congress: Int,
    amendmentType: AmendmentType,
    number: String,
    correlationId: UUID,
  ): F[List[AmendmentTextItemDTO]] = {
    val naturalKey = s"$congress-${amendmentType.apiValue.toUpperCase}-$number"
    val typePath   = amendmentType.apiValue.toLowerCase
    val rawUrl     = s"${config.baseUrl}/amendment/$congress/$typePath/$number/text"

    parseUri(rawUrl, naturalKey).flatMap { baseUri =>
      val uri = baseUri
        .withQueryParam("api_key", config.apiKey)
        .withQueryParam("format", "json")

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        response.status match {
          case Status.NotFound =>
            Temporal[F].pure(List.empty[AmendmentTextItemDTO])
          case status if status.isSuccess =>
            response.as[AmendmentTextResponseDTO].map(_.textVersions)
          case _ =>
            raiseApiError(response)
        }
      }

      retryWrapper.withRetry(
        operation = operation,
        config = config.retry,
        classifier = AmendmentTextCheckErrorClassifier,
        errorFactory = (msg, cause) =>
          AmendmentTextCheckFailed(
            naturalKey = naturalKey,
            detail = msg,
            cause = cause,
          ),
        correlationId = correlationId,
      )
    }
  }

}
