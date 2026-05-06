package repcheck.ingestion.amendments.api

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal
import cats.syntax.all._

import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.headers.Accept
import org.http4s.{MediaType, Uri}

import repcheck.ingestion.amendments.errors.{AmendmentFetchFailed, AmendmentsApiErrorClassifier, AmendmentsApiHttpError}
import repcheck.ingestion.common.api.{CongressGovClientConfig, CongressGovPaginatedClient, FetchParams, PagedResponse}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.amendment.{AmendmentDetailDTO, AmendmentListItemDTO}

/**
 * Paginated client for Congress.gov's `/v3/amendment` endpoints. Each list page yields up to `config.pageSize`
 * [[AmendmentListItemDTO]] entries; each entry carries a `url` pointing at the per-amendment detail endpoint, which the
 * caller hands back to [[fetchDetail]] to get the full [[AmendmentDetailDTO]].
 *
 * `fetchAll` (inherited from [[CongressGovPaginatedClient]]) drains every page across the configured
 * `fromDateTime`/`toDateTime` window. Termination is driven by the response's `pagination.count` (totalCount), NOT by
 * `items.size < pageSize` — the base class explicitly retries anomalously-short pages mid-stream up to
 * `MaxShortPageRetries` (10) times with a `pageDelay * 4` backoff before either giving up and continuing or terminating
 * via the totalCount-bounded check.
 *
 * URL casing: the path segment is lowercase (`/v3/amendment`) per Congress.gov's documented convention; query parameter
 * names follow the camelCase form they use in their own examples (`fromDateTime`, `toDateTime`, `api_key`).
 *
 * `api_key` is passed as a query parameter (Congress.gov's documented auth scheme); error messages strip it via
 * `Uri.removeQueryParam("api_key")` so it never lands in logs.
 */
class AmendmentsApiClient[F[_]](
  config: CongressGovClientConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
  temporalInstance: Temporal[F],
) extends CongressGovPaginatedClient[F, AmendmentListItemDTO] {

  override protected def pageDelay: FiniteDuration = config.pageDelay

  implicit override protected def temporal: Temporal[F] = temporalInstance

  override protected def maxShortPageRetries: Int = config.shortPageRetries

  private val isoFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC)

  /**
   * Per-call attempt is captured on the body-decode side via the inbound HTTP `attempt` is not surfaced by the
   * RetryWrapper API (it tracks attempts internally, then synthesizes a Systemic at the end). The attempt slot on
   * `AmendmentsApiHttpError` reflects the body-decode side: `1` for the first non-success response we observe per call.
   * When the wrapper retries, it constructs a fresh response → fresh `1`. Per-error attempt accounting therefore lives
   * on the wrapper logs, not the exception field — the field is here for forward compat with a future processor-level
   * retry that wants the count visible in the exception.
   */
  private def raiseApiError[A](response: org.http4s.Response[F]): F[A] =
    response
      .as[String]
      .recover { case _ => response.status.reason }
      .flatMap(body => temporal.raiseError[A](AmendmentsApiHttpError(response.status.code, body, attempt = 1)))

  private def parseUri(raw: String, naturalKey: String): F[Uri] =
    Uri.fromString(raw) match {
      case Right(uri) => temporal.pure(uri)
      case Left(err) =>
        temporal.raiseError(AmendmentFetchFailed(naturalKey, new IllegalArgumentException(err.sanitized)))
    }

  /**
   * One page of amendments from `/v3/amendment[/{congress}]`. Builds the URL with `api_key`, `format=json`, `offset`,
   * `limit`, `sort`, and the optional `congress` / `fromDateTime` / `toDateTime` filters. Decodes the
   * `{"amendments":[...],"pagination":{...}}` envelope via [[AmendmentListApiResponseDTO]] and surfaces it as a
   * [[PagedResponse]].
   *
   * `nextOffset` is left `None` here: the parent's `attemptFetch` advances offsets independently. Setting a misleading
   * "stop on short page" value would obscure the actual contract — see `BillSummariesApiClient` for the same rationale.
   */
  override def fetchPage(params: FetchParams): F[PagedResponse[AmendmentListItemDTO]] = {
    val basePath = params.congress.fold(s"${config.baseUrl}/amendment")(c => s"${config.baseUrl}/amendment/$c")
    parseUri(basePath, naturalKeyForList(params)).flatMap { baseUri =>
      val uri = {
        val withBase = baseUri
          .withQueryParam("api_key", config.apiKey)
          .withQueryParam("format", "json")
          .withQueryParam("offset", params.offset)
          .withQueryParam("limit", params.pageSize)
          .withQueryParam("sort", params.sort.queryValue)

        val withFrom = params.fromDateTime.fold(withBase) { dt =>
          withBase.withQueryParam("fromDateTime", isoFormatter.format(dt))
        }

        params.toDateTime.fold(withFrom)(dt => withFrom.withQueryParam("toDateTime", isoFormatter.format(dt)))
      }

      val operation =
        client.run(org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))).use { response =>
          if (response.status.isSuccess) {
            response.as[AmendmentListApiResponseDTO].map { listResponse =>
              PagedResponse(
                items = listResponse.items,
                totalCount = listResponse.pagination.flatMap(_.count).getOrElse(listResponse.items.size),
                nextOffset = None,
              )
            }
          } else {
            raiseApiError(response)
          }
        }

      retryWrapper.withRetry(
        operation = operation,
        config = config.retry,
        classifier = AmendmentsApiErrorClassifier,
        errorFactory = (_, cause) =>
          AmendmentFetchFailed(
            naturalKey = naturalKeyForList(params),
            cause = cause,
          ),
        correlationId = UUID.randomUUID(),
      )
    }
  }

  /**
   * Single-shot fetch of a per-amendment detail endpoint. The list endpoint hands us a fully-qualified URL (which may
   * already include `format=`) — strip any pre-existing `format` and `api_key` and re-add them to keep the call
   * deterministic.
   *
   * Decodes the `{"amendment": {...}}` envelope into [[AmendmentDetailDTO]] via [[AmendmentDetailApiResponseDTO]]. 4xx
   * (other than 429) is classified as Systemic and propagates as `AmendmentFetchFailed`; the processor (§7.3) decides
   * whether to skip-and-warn (404) or halt (auth errors).
   *
   * The caller threads its amendment-level `correlationId` through this method so detail-fetch logs (and any retry
   * wrapper diagnostics) share the same ID as the surrounding list-page → detail-fetch → recursion → upsert work for
   * that amendment. Do NOT mint a fresh UUID here.
   */
  def fetchDetail(detailUrl: String, correlationId: UUID): F[AmendmentDetailDTO] =
    parseUri(detailUrl, detailUrl).flatMap { rawUri =>
      val uri = rawUri
        .removeQueryParam("format")
        .removeQueryParam("api_key")
        .withQueryParam("format", "json")
        .withQueryParam("api_key", config.apiKey)

      val operation =
        client.run(org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))).use { response =>
          if (response.status.isSuccess) {
            response.as[AmendmentDetailApiResponseDTO].map(_.amendment)
          } else {
            raiseApiError[AmendmentDetailDTO](response)
          }
        }

      retryWrapper.withRetry(
        operation = operation,
        config = config.retry,
        classifier = AmendmentsApiErrorClassifier,
        errorFactory = (_, cause) =>
          AmendmentFetchFailed(
            naturalKey = uri.removeQueryParam("api_key").renderString,
            cause = cause,
          ),
        correlationId = correlationId,
      )
    }

  /**
   * Stable identifier for list-call diagnostics. Prefer the `{congress}` tag when present; otherwise the global feed
   * tag.
   */
  private def naturalKeyForList(params: FetchParams): String =
    params.congress.fold("amendment")(c => s"amendment/$c")

}

object AmendmentsApiClient {

  def apply[F[_]: Temporal](
    config: CongressGovClientConfig,
    client: Client[F],
    retryWrapper: RetryWrapper[F],
  ): AmendmentsApiClient[F] =
    new AmendmentsApiClient[F](config, client, retryWrapper, Temporal[F])

}
