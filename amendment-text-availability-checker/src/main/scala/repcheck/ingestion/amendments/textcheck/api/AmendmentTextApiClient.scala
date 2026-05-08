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
 * Single-shot GET against Congress.gov's `/v3/amendment/{congress}/{type}/{amendmentNumber}/text` endpoint. Builds the
 * URL with `api_key` + `format=json`, decodes the `{"textVersions":[...],"pagination":{...}}` envelope into
 * `List[AmendmentTextItemDTO]`, and wraps the call with [[RetryWrapper]] + [[AmendmentTextCheckErrorClassifier]].
 *
 * Casing convention (consistent with the rest of the codebase, including `AmendmentsApiClient` and
 * `BillTextApiClient`):
 *   - URL path uses LOWERCASE amendment-type code (`samdt`, `hamdt`) — Congress.gov accepts only lowercase in path
 *     segments.
 *   - Natural key uses UPPERCASE amendment-type code (`SAMDT`, `HAMDT`) to match the values stored in the
 *     `amendment_type_enum` PostgreSQL enum (so logs and error messages line up with what queries against the DB
 *     produce).
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
   * @param amendmentNumber
   *   Congress.gov-assigned amendment number (string because upstream emits it as a string, e.g. `"2137"`). This is the
   *   `{number}` path segment in the upstream URL — not a synthetic ID.
   * @param correlationId
   *   Per-amendment correlation ID minted by the calling pipeline (`AmendmentTextAvailabilityChecker.checkAll`
   *   generates one UUID per amendment in `parEvalMap`). The same UUID is propagated end-to-end: retry-wrapper logs
   *   here, the `AmendmentTextAvailableEvent` payload, and the downstream `amendment-text-pipeline` consumer that
   *   processes the event. This gives a single correlation thread per amendment across the availability checker AND the
   *   text-ingestion service. Do NOT mint a fresh UUID here.
   */
  def fetchTextVersions(
    congress: Int,
    amendmentType: AmendmentType,
    amendmentNumber: String,
    correlationId: UUID,
  ): F[List[AmendmentTextItemDTO]] = {
    // UPPERCASE for natural key (matches amendment_type_enum DB values); LOWERCASE for URL path (Congress.gov
    // accepts only lowercase in `/amendment/{type}` path segments).
    val naturalKey = s"$congress-${amendmentType.apiValue.toUpperCase}-$amendmentNumber"
    val typePath   = amendmentType.apiValue.toLowerCase
    val rawUrl     = s"${config.baseUrl}/amendment/$congress/$typePath/$amendmentNumber/text"

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
