package repcheck.ingestion.bills.metadata.api

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal
import cats.syntax.all._

import io.circe.Decoder

import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.headers.Accept
import org.http4s.{MediaType, Uri}

import repcheck.ingestion.bills.metadata.errors.{BillFetchFailed, BillsApiErrorClassifier, BillsApiHttpError}
import repcheck.ingestion.common.api.{CongressGovClientConfig, CongressGovPaginatedClient, FetchParams, PagedResponse}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.shared.models.congress.dto.bill.{
  BillDetailDTO,
  BillListItemDTO,
  BillListResponseDTO,
  CoSponsorDTO,
  CosponsorListResponseDTO,
  LegislativeSubjectDTO,
}
import repcheck.shared.models.congress.dto.common.PaginationInfoDTO

import com.repcheck.utils.errors.RetryWrapper

class BillsApiClient[F[_]](
  config: CongressGovClientConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
  logger: PipelineLogger[F],
  temporalInstance: Temporal[F],
) extends CongressGovPaginatedClient[F, BillListItemDTO] {

  // Logs from the API layer aren't tied to a specific run ID (the run ID is owned by the pipeline
  // entry point) — but every log line still carries a stepName so operators can grep an operational
  // dimension. "bill-metadata-api" distinguishes API-layer events from the in-pipeline
  // "bill-metadata" stepName used by BillMetadataProcessor.
  private val apiLogCtx = LogContext("0", "bill-metadata-api")

  override protected def pageDelay: FiniteDuration = config.pageDelay

  implicit override protected def temporal: Temporal[F] = temporalInstance

  private val isoFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC)

  private def raiseApiError[A](response: org.http4s.Response[F]): F[A] =
    response
      .as[String]
      .recover { case _ => response.status.reason }
      .flatMap(body => temporal.raiseError[A](BillsApiHttpError(response.status.code, body)))

  private def parseUri(raw: String): F[Uri] =
    Uri.fromString(raw) match {
      case Right(uri) => temporal.pure(uri)
      case Left(err)  => temporal.raiseError(BillFetchFailed(raw, 0, err.sanitized, err))
    }

  override def fetchPage(params: FetchParams): F[PagedResponse[BillListItemDTO]] =
    parseUri(s"${config.baseUrl}/bill").flatMap { baseUri =>
      val uri = {
        val withBase = baseUri
          .withQueryParam("api_key", config.apiKey)
          .withQueryParam("format", "json")
          .withQueryParam("offset", params.offset)
          .withQueryParam("limit", params.pageSize)
          .withQueryParam("sort", params.sort.queryValue)

        val withCongress = params.congress.fold(withBase)(c => withBase.withQueryParam("congress", c))

        val withFrom = params.fromDateTime.fold(withCongress) { dt =>
          withCongress.withQueryParam("fromDateTime", isoFormatter.format(dt))
        }

        params.toDateTime.fold(withFrom)(dt => withFrom.withQueryParam("toDateTime", isoFormatter.format(dt)))
      }
      val sanitizedUri = uri.removeQueryParam("api_key").renderString

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        if (response.status.isSuccess) {
          response.as[BillListResponseDTO].map { listResponse =>
            PagedResponse(
              items = listResponse.items,
              totalCount = listResponse.pagination.flatMap(_.count).getOrElse(listResponse.items.size),
              nextOffset = if (listResponse.items.size < params.pageSize) { None }
              else { Some(params.offset + params.pageSize) },
            )
          }
        } else {
          raiseApiError(response)
        }
      }

      val instrumentedOperation =
        for {
          _ <- logger.info(
            apiLogCtx,
            s"Fetching bill list page: offset=${params.offset.toString}, limit=${params.pageSize.toString}",
          )
          start <- temporal.realTime
          result <- retryWrapper.withRetry(
            operation = operation,
            config = config.retry,
            classifier = BillsApiErrorClassifier,
            errorFactory = (msg, cause) =>
              BillFetchFailed(
                endpoint = sanitizedUri,
                statusCode = 0,
                detail = msg,
                cause = cause,
              ),
            correlationId = UUID.randomUUID(),
          )
          end <- temporal.realTime
          _ <- logger.info(
            apiLogCtx,
            s"Fetched bill list page: offset=${params.offset.toString}, " +
              s"items=${result.items.size.toString}, totalCount=${result.totalCount.toString}, " +
              s"elapsed=${(end - start).toMillis.toString}ms",
          )
        } yield result

      instrumentedOperation
    }

  def fetchDetail(detailUrl: String): F[BillDetailDTO] =
    parseUri(detailUrl).flatMap { baseUri =>
      val uri          = baseUri.withQueryParam("api_key", config.apiKey).withQueryParam("format", "json")
      val sanitizedUri = uri.removeQueryParam("api_key").renderString

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        if (response.status.isSuccess) {
          response.as[BillDetailWrapper].map(_.bill)
        } else {
          raiseApiError(response)
        }
      }

      for {
        _     <- logger.info(apiLogCtx, s"Fetching bill detail: $sanitizedUri")
        start <- temporal.realTime
        result <- retryWrapper.withRetry(
          operation = operation,
          config = config.retry,
          classifier = BillsApiErrorClassifier,
          errorFactory = (msg, cause) =>
            BillFetchFailed(
              endpoint = sanitizedUri,
              statusCode = 0,
              detail = msg,
              cause = cause,
            ),
          correlationId = UUID.randomUUID(),
        )
        end <- temporal.realTime
        _ <- logger.info(
          apiLogCtx,
          s"Fetched bill detail: $sanitizedUri (elapsed=${(end - start).toMillis.toString}ms)",
        )
      } yield result
    }

  def fetchCosponsors(cosponsorUrl: String): F[List[CoSponsorDTO]] =
    for {
      _     <- logger.info(apiLogCtx, s"Fetching cosponsors: $cosponsorUrl")
      start <- temporal.realTime
      result <- fetchPaginated[CosponsorListResponseDTO, CoSponsorDTO](
        cosponsorUrl,
        List.empty,
        config.pageSize,
        "cosponsor",
        _.cosponsors,
        _.pagination,
      )
      end <- temporal.realTime
      _ <- logger.info(
        apiLogCtx,
        s"Fetched ${result.size.toString} cosponsors total: $cosponsorUrl " +
          s"(elapsed=${(end - start).toMillis.toString}ms)",
      )
    } yield result

  /**
   * Follow a Congress.gov url-paginated sub-resource (cosponsors, subjects, …) to completion: fetch a page through the
   * standard retry/backoff, accumulate its items, and recurse via `pagination.url` until a short page or no next url.
   * `items`/`pagination` project the per-resource response; `resourceName` only labels the per-page debug log.
   */
  private def fetchPaginated[R: Decoder, A](
    url: String,
    accumulated: List[A],
    pageSize: Int,
    resourceName: String,
    items: R => List[A],
    pagination: R => Option[PaginationInfoDTO],
  ): F[List[A]] =
    parseUri(url).flatMap { baseUri =>
      val uri = baseUri
        .withQueryParam("api_key", config.apiKey)
        .withQueryParam("format", "json")
        .withQueryParam("limit", pageSize)
      val sanitizedUri = uri.removeQueryParam("api_key").renderString

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        if (response.status.isSuccess) {
          response.as[R]
        } else {
          raiseApiError[R](response)
        }
      }

      val pageStart = temporal.flatMap(logger.debug(apiLogCtx, s"Fetching $resourceName page: $sanitizedUri")) { _ =>
        retryWrapper.withRetry(
          operation = operation,
          config = config.retry,
          classifier = BillsApiErrorClassifier,
          errorFactory = (msg, cause) =>
            BillFetchFailed(
              endpoint = sanitizedUri,
              statusCode = 0,
              detail = msg,
              cause = cause,
            ),
          correlationId = UUID.randomUUID(),
        )
      }

      pageStart.flatMap { page =>
        val pageItems = items(page)
        val all       = accumulated ++ pageItems
        val nextUrl   = pagination(page).flatMap(_.url)
        if (pageItems.size < pageSize || nextUrl.isEmpty) {
          temporal.pure(all)
        } else {
          temporal.flatMap(temporal.sleep(pageDelay))(_ =>
            fetchPaginated(nextUrl.getOrElse(url), all, pageSize, resourceName, items, pagination)
          )
        }
      }
    }

  /**
   * Fetch a bill's legislative subjects. Unlike cosponsors (whose sub-resource url the detail provides), the bill
   * detail carries `subjects` only as a `{count,url}` ref that decodes empty, so we construct the `/subjects` url from
   * the bill's own coordinates against `config.baseUrl` — robust regardless of whether the detail carries a `url`.
   */
  def fetchSubjects(congress: Int, billType: String, number: String): F[List[LegislativeSubjectDTO]] = {
    val subjectsUrl = s"${config.baseUrl}/bill/${congress.toString}/${billType.toLowerCase}/$number/subjects"
    for {
      _ <- logger.info(apiLogCtx, s"Fetching subjects: $subjectsUrl")
      result <- fetchPaginated[BillSubjectsResponseDTO, LegislativeSubjectDTO](
        subjectsUrl,
        List.empty,
        config.pageSize,
        "subject",
        _.legislativeSubjects,
        _.pagination,
      )
      _ <- logger.info(apiLogCtx, s"Fetched ${result.size.toString} subjects total: $subjectsUrl")
    } yield result
  }

}

object BillsApiClient {

  def apply[F[_]: Temporal](
    config: CongressGovClientConfig,
    client: Client[F],
    retryWrapper: RetryWrapper[F],
    logger: PipelineLogger[F],
  ): BillsApiClient[F] =
    new BillsApiClient[F](config, client, retryWrapper, logger, Temporal[F])

}

/** Congress.gov wraps the bill detail in a `{"bill": {...}}` envelope. */
final private[api] case class BillDetailWrapper(bill: BillDetailDTO)

private[api] object BillDetailWrapper {

  implicit val decoder: Decoder[BillDetailWrapper] = Decoder.instance { c =>
    c.downField("bill").as[BillDetailDTO].map(BillDetailWrapper.apply)
  }

}
