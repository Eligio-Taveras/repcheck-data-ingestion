package com.repcheck.bills.textcheck.api

import java.util.UUID

import cats.effect.Temporal
import cats.syntax.all._

import io.circe.Decoder

import org.http4s.circe.jsonOf
import org.http4s.client.Client
import org.http4s.{EntityDecoder, Status, Uri}

import repcheck.ingestion.common.api.{CongressGovApiException, CongressGovClientConfig, CongressGovErrorClassifier}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.bill.TextVersionDTO

import com.repcheck.bills.textcheck.errors.BillTextCheckFailed

class BillTextApiClient[F[_]: Temporal](
  config: CongressGovClientConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
) {

  implicit private val textVersionsResponseDecoder: EntityDecoder[F, TextVersionsResponse] =
    jsonOf[F, TextVersionsResponse]

  def fetchTextVersions(
    congress: Int,
    billType: String,
    number: String,
  ): F[List[TextVersionDTO]] = {
    val billId = s"$congress-${billType.toUpperCase}-$number"
    val uri = Uri
      .unsafeFromString(s"${config.baseUrl}/bill/$congress/$billType/$number/text")
      .withQueryParam("api_key", config.apiKey)

    val operation = client.run(org.http4s.Request[F](uri = uri)).use { response =>
      response.status match {
        case Status.NotFound =>
          Temporal[F].pure(List.empty[TextVersionDTO])
        case status if status.isSuccess =>
          response.as[TextVersionsResponse].map(_.textVersions)
        case status =>
          response.as[String].flatMap { body =>
            Temporal[F].raiseError[List[TextVersionDTO]](
              CongressGovApiException(status.code, body)
            )
          }
      }
    }

    retryWrapper.withRetry(
      operation = operation,
      config = config.retry,
      classifier = CongressGovErrorClassifier,
      errorFactory = (msg, cause) =>
        BillTextCheckFailed(
          billId = billId,
          detail = msg,
          cause = cause,
        ),
      correlationId = UUID.randomUUID(),
    )
  }

}

/** Congress.gov wraps text versions in a `{"textVersions": [...]}` envelope. */
final private[api] case class TextVersionsResponse(textVersions: List[TextVersionDTO])

private[api] object TextVersionsResponse {

  implicit val decoder: Decoder[TextVersionsResponse] = Decoder.instance { c =>
    c.downField("textVersions")
      .as[List[TextVersionDTO]]
      .map(TextVersionsResponse.apply)
  }

}
