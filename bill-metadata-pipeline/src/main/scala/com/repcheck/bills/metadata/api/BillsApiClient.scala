package com.repcheck.bills.metadata.api

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal
import cats.syntax.all._

import io.circe.Decoder

import org.http4s.Uri
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client

import repcheck.ingestion.common.api.{
  CongressGovApiException,
  CongressGovClientConfig,
  CongressGovErrorClassifier,
  CongressGovPaginatedClient,
  FetchParams,
  PagedResponse,
}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.bill.{BillDetailDTO, BillListItemDTO, BillListResponseDTO}

import com.repcheck.bills.metadata.errors.BillFetchFailed

class BillsApiClient[F[_]](
  config: CongressGovClientConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
  temporalInstance: Temporal[F],
) extends CongressGovPaginatedClient[F, BillListItemDTO] {

  override protected def pageDelay: FiniteDuration = config.pageDelay

  implicit override protected def temporal: Temporal[F] = temporalInstance

  private val isoFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC)

  override def fetchPage(params: FetchParams): F[PagedResponse[BillListItemDTO]] = {
    val baseUri = Uri
      .unsafeFromString(s"${config.baseUrl}/bill")
      .withQueryParam("api_key", config.apiKey)
      .withQueryParam("offset", params.offset)
      .withQueryParam("limit", params.pageSize)
      .withQueryParam("sort", params.sort.queryValue)

    val withCongress = params.congress.fold(baseUri)(c => baseUri.withQueryParam("congress", c))

    val withFrom = params.fromDateTime.fold(withCongress) { dt =>
      withCongress.withQueryParam("fromDateTime", isoFormatter.format(dt))
    }

    val uri = params.toDateTime.fold(withFrom)(dt => withFrom.withQueryParam("toDateTime", isoFormatter.format(dt)))

    val operation = client.run(org.http4s.Request[F](uri = uri)).use { response =>
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
        org.http4s.EntityDecoder
          .text[F](using temporal)
          .decode(response, strict = false)
          .value
          .flatMap {
            case Right(body) =>
              temporal.raiseError(CongressGovApiException(response.status.code, body))
            case Left(_) =>
              temporal.raiseError(
                CongressGovApiException(response.status.code, response.status.reason)
              )
          }
      }
    }

    retryWrapper.withRetry(
      operation = operation,
      config = config.retry,
      classifier = CongressGovErrorClassifier,
      errorFactory = (msg, cause) =>
        BillFetchFailed(
          endpoint = uri.renderString,
          statusCode = 0,
          detail = msg,
          cause = cause,
        ),
      correlationId = UUID.randomUUID(),
    )
  }

  def fetchDetail(detailUrl: String): F[BillDetailDTO] = {
    val uri = Uri
      .unsafeFromString(detailUrl)
      .withQueryParam("api_key", config.apiKey)

    val operation = client.run(org.http4s.Request[F](uri = uri)).use { response =>
      if (response.status.isSuccess) {
        response.as[BillDetailWrapper].map(_.bill)
      } else {
        org.http4s.EntityDecoder
          .text[F](using temporal)
          .decode(response, strict = false)
          .value
          .flatMap {
            case Right(body) =>
              temporal.raiseError(CongressGovApiException(response.status.code, body))
            case Left(_) =>
              temporal.raiseError(
                CongressGovApiException(response.status.code, response.status.reason)
              )
          }
      }
    }

    retryWrapper.withRetry(
      operation = operation,
      config = config.retry,
      classifier = CongressGovErrorClassifier,
      errorFactory = (msg, cause) =>
        BillFetchFailed(
          endpoint = uri.renderString,
          statusCode = 0,
          detail = msg,
          cause = cause,
        ),
      correlationId = UUID.randomUUID(),
    )
  }

}

object BillsApiClient {

  def apply[F[_]: Temporal](
    config: CongressGovClientConfig,
    client: Client[F],
    retryWrapper: RetryWrapper[F],
  ): BillsApiClient[F] =
    new BillsApiClient[F](config, client, retryWrapper, Temporal[F])

}

/** Congress.gov wraps the bill detail in a `{"bill": {...}}` envelope. */
final private[api] case class BillDetailWrapper(bill: BillDetailDTO)

private[api] object BillDetailWrapper {

  implicit val decoder: Decoder[BillDetailWrapper] = Decoder.instance { c =>
    c.downField("bill").as[BillDetailDTO].map(BillDetailWrapper.apply)
  }

}
