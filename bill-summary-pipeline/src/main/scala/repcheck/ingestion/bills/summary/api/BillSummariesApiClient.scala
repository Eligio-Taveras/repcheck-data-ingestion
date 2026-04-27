package repcheck.ingestion.bills.summary.api

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

import repcheck.ingestion.bills.summary.errors.{
  BillSummariesApiErrorClassifier,
  BillSummariesApiHttpError,
  BillSummaryFetchFailed,
}
import repcheck.ingestion.common.api.{CongressGovClientConfig, CongressGovPaginatedClient, FetchParams, PagedResponse}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.bill.BillSummaryDTO

/**
 * Paginated client for Congress.gov's `/summaries[/{congress}]` endpoints. Each page yields up to `config.pageSize`
 * [[BillSummaryDTO]] entries, each carrying an embedded `bill: BillReferenceDTO` we use to look the bill up by natural
 * key in the processor.
 *
 * `fetchAll` (inherited from [[CongressGovPaginatedClient]]) drains every page across the configured
 * `fromDateTime`/`toDateTime` window. Termination is driven by the response's `pagination.count` (totalCount), NOT by
 * `items.size < pageSize` — the base class explicitly retries anomalously-short pages mid-stream up to
 * `MaxShortPageRetries` (10) times with a `pageDelay * 4` backoff before either giving up and continuing or terminating
 * via the totalCount-bounded check. See `CongressGovPaginatedClient.fetchAll` scaladoc for the full termination logic.
 *
 * The `nextOffset` field in [[PagedResponse]] is therefore unused by `fetchAll` and is set to `None` here (the parent's
 * `attemptFetch` advances offsets independently). Leaving it `None` instead of computing a misleading short-page-stop
 * value avoids future confusion about whether this client tries to short-circuit pagination — it does not.
 *
 * `api_key` is passed as a query parameter (Congress.gov's documented auth scheme); error messages strip it via
 * `Uri.removeQueryParam("api_key")` so it never lands in logs.
 */
class BillSummariesApiClient[F[_]](
  config: CongressGovClientConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
  temporalInstance: Temporal[F],
) extends CongressGovPaginatedClient[F, BillSummaryDTO] {

  override protected def pageDelay: FiniteDuration = config.pageDelay

  implicit override protected def temporal: Temporal[F] = temporalInstance

  private val isoFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC)

  private def raiseApiError[A](response: org.http4s.Response[F]): F[A] =
    response
      .as[String]
      .recover { case _ => response.status.reason }
      .flatMap(body => temporal.raiseError[A](BillSummariesApiHttpError(response.status.code, body)))

  private def parseUri(raw: String): F[Uri] =
    Uri.fromString(raw) match {
      case Right(uri) => temporal.pure(uri)
      case Left(err)  => temporal.raiseError(BillSummaryFetchFailed(raw, 0, err.sanitized, err))
    }

  override def fetchPage(params: FetchParams): F[PagedResponse[BillSummaryDTO]] = {
    val basePath = params.congress.fold(s"${config.baseUrl}/summaries")(c => s"${config.baseUrl}/summaries/$c")
    parseUri(basePath).flatMap { baseUri =>
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

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        if (response.status.isSuccess) {
          response.as[BillSummaryListResponseDTO].map { listResponse =>
            // `nextOffset` is unused by CongressGovPaginatedClient.fetchAll; the parent advances
            // offsets via `currentParams.offset + currentParams.pageSize` and terminates on
            // totalCount or empty pages. Setting it to `None` here makes that contract explicit
            // and avoids the misleading "stop on short page" pattern that previously lived here.
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
        classifier = BillSummariesApiErrorClassifier,
        errorFactory = (msg, cause) =>
          BillSummaryFetchFailed(
            endpoint = uri.removeQueryParam("api_key").renderString,
            statusCode = 0,
            detail = msg,
            cause = cause,
          ),
        correlationId = UUID.randomUUID(),
      )
    }
  }

}

object BillSummariesApiClient {

  def apply[F[_]: Temporal](
    config: CongressGovClientConfig,
    client: Client[F],
    retryWrapper: RetryWrapper[F],
  ): BillSummariesApiClient[F] =
    new BillSummariesApiClient[F](config, client, retryWrapper, Temporal[F])

}
