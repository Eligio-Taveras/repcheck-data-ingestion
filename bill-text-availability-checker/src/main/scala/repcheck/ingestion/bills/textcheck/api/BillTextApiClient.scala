package repcheck.ingestion.bills.textcheck.api

import java.util.UUID

import cats.effect.Temporal
import cats.syntax.all._

import io.circe.Decoder

import org.http4s.circe.jsonOf
import org.http4s.client.Client
import org.http4s.headers.Accept
import org.http4s.{EntityDecoder, MediaType, Status, Uri}

import repcheck.ingestion.bills.textcheck.errors.BillTextCheckFailed
import repcheck.ingestion.common.api.{CongressGovApiException, CongressGovClientConfig, CongressGovErrorClassifier}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.bill.TextVersionDTO

class BillTextApiClient[F[_]: Temporal](
  config: CongressGovClientConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
) {

  implicit private val textVersionsResponseDecoder: EntityDecoder[F, TextVersionsResponse] =
    jsonOf[F, TextVersionsResponse]

  private def raiseApiError[A](response: org.http4s.Response[F]): F[A] =
    response
      .as[String]
      .recover { case _ => response.status.reason }
      .flatMap(body => Temporal[F].raiseError[A](CongressGovApiException(response.status.code, body)))

  private def parseUri(raw: String): F[Uri] =
    Uri.fromString(raw) match {
      case Right(uri) => Temporal[F].pure(uri)
      case Left(err)  => Temporal[F].raiseError(err)
    }

  def fetchTextVersions(
    congress: Int,
    billType: String,
    number: String,
  ): F[List[TextVersionDTO]] = {
    val billId = s"$congress-${billType.toUpperCase}-$number"

    parseUri(s"${config.baseUrl}/bill/$congress/$billType/$number/text").flatMap { baseUri =>
      val uri = baseUri.withQueryParam("api_key", config.apiKey).withQueryParam("format", "json")

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        response.status match {
          case Status.NotFound =>
            Temporal[F].pure(List.empty[TextVersionDTO])
          case status if status.isSuccess =>
            response.as[TextVersionsResponse].map(_.textVersions)
          case _ =>
            raiseApiError(response)
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
