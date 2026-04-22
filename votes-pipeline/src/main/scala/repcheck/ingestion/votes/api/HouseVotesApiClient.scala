package repcheck.ingestion.votes.api

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal
import cats.syntax.all._

import io.circe.Decoder

import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.headers.Accept
import org.http4s.{MediaType, Uri}

import repcheck.ingestion.common.api.{CongressGovClientConfig, CongressGovPaginatedClient, FetchParams, PagedResponse}
import repcheck.ingestion.votes.config.HouseVotesConfig
import repcheck.ingestion.votes.errors.{HouseVoteApiErrorClassifier, HouseVoteApiHttpError, HouseVoteFetchFailed}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.vote.{VoteListItemDTO, VoteMembersDTO, VoteResultDTO}

/**
 * Congress.gov beta `/house-vote` API client. Extends [[CongressGovPaginatedClient]] so `fetchAll` can walk the
 * paginated list endpoint; also exposes [[fetchMembersVotePositions]] for the per-vote member-positions endpoint.
 *
 * ==URL construction==
 * Both URLs match the official Congress.gov OpenAPI spec (see `congress-gov-api.yaml` in the votr docs repo):
 *   - List: `{baseUrl}/house-vote/{congress}/{session}?format=json&offset={o}&limit={n}&api_key={k}` — confirmed
 *     against `/house-vote/{congress}/{session}` at yaml lines 1061-1084. Only `format` / `offset` / `limit` are in the
 *     spec's parameter list (plus `api_key` added as a global query param).
 *   - Members: `{baseUrl}/house-vote/{congress}/{session}/{voteNumber}/members?format=json&api_key={k}` — confirmed
 *     against `/house-vote/{congress}/{session}/{voteNumber}/members` at yaml lines 1110-1134. The spec allows `offset`
 *     / `limit` too but the response is a single non-paginated object so we omit them.
 *
 * The beta endpoint does NOT accept `fromDateTime` / `toDateTime` / `sort` — those produce HTTP 400. `lookbackDays` is
 * applied client-side after pagination (see [[fetchRecentVotes]]).
 *
 * ==Client-side lookback==
 * `fetchRecentVotes` paginates ALL pages for the configured congress/session, sorts DESC by `updateDate`, then filters
 * by `now - lookbackDays`. We do NOT short-circuit via `takeWhile` because the API does not guarantee ordering.
 *
 * ==Constructor contract==
 * The passed-in `Client[F]` must already be wrapped by `VotesPipeline.rateLimitedClient` — this class does NOT apply
 * its own rate limiter. `pageDelay` in the base trait is read from `CongressGovClientConfig` for the sake of the
 * `fetchAll` default implementation, but the real inter-request pacing is enforced by the wrapper's semaphore.
 */
class HouseVotesApiClient[F[_]](
  config: CongressGovClientConfig,
  houseConfig: HouseVotesConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
  temporalInstance: Temporal[F],
) extends CongressGovPaginatedClient[F, VoteListItemDTO] {

  override protected def pageDelay: FiniteDuration = config.pageDelay

  implicit override protected def temporal: Temporal[F] = temporalInstance

  /** Decoder-scoping: keep the API-shape wrappers lexically next to the client that uses them. */
  import HouseVotesApiClient._

  override def fetchPage(params: FetchParams): F[PagedResponse[VoteListItemDTO]] =
    parseUri(s"${config.baseUrl}/house-vote/${houseConfig.congress}/${houseConfig.session}", None).flatMap { baseUri =>
      val uri = baseUri
        .withQueryParam("format", "json")
        .withQueryParam("offset", params.offset)
        .withQueryParam("limit", params.pageSize)
        .withQueryParam("api_key", config.apiKey)

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        if (response.status.isSuccess) {
          response.as[HouseVoteListEnvelope].map { envelope =>
            PagedResponse(
              items = envelope.items,
              totalCount = envelope.pagination.flatMap(_.count).getOrElse(envelope.items.size),
              nextOffset = if (envelope.items.size < params.pageSize) { None }
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
        classifier = HouseVoteApiErrorClassifier,
        errorFactory = (msg, cause) =>
          HouseVoteFetchFailed(
            congress = houseConfig.congress,
            session = houseConfig.session,
            voteNumber = None,
            detail = msg,
            cause = cause,
          ),
        correlationId = UUID.randomUUID(),
      )
    }

  /**
   * Fetch member positions for a specific vote. Different return type (`VoteMembersDTO`) than the list endpoint, so
   * this method does NOT extend the paginated protocol — it's a direct, single-request call.
   *
   * Uses [[HouseVoteApiErrorClassifier]] for transient-vs-systemic classification just like `fetchPage`; a 404 for a
   * specific vote is classified as Systemic (not retried) and surfaces as [[HouseVoteFetchFailed]] with the caller's
   * `voteNumber` preserved so the log entry identifies exactly which vote was missing.
   */
  def fetchMembersVotePositions(congress: Int, session: Int, voteNumber: Int): F[VoteMembersDTO] = {
    val url = s"${config.baseUrl}/house-vote/$congress/$session/$voteNumber/members"
    parseUri(url, Some(voteNumber)).flatMap { baseUri =>
      val uri = baseUri
        .withQueryParam("format", "json")
        .withQueryParam("api_key", config.apiKey)

      val request = org.http4s.Request[F](uri = uri).putHeaders(Accept(MediaType.application.json))
      val operation = client.run(request).use { response =>
        if (response.status.isSuccess) {
          response.as[HouseVoteMembersEnvelope].map(_.data)
        } else {
          raiseApiError(response)
        }
      }

      retryWrapper.withRetry(
        operation = operation,
        config = config.retry,
        classifier = HouseVoteApiErrorClassifier,
        errorFactory = (msg, cause) =>
          HouseVoteFetchFailed(
            congress = congress,
            session = session,
            voteNumber = Some(voteNumber),
            detail = msg,
            cause = cause,
          ),
        correlationId = UUID.randomUUID(),
      )
    }
  }

  /**
   * Fetch every vote for the configured congress/session, sort newest-first, and drop anything older than the lookback
   * cutoff. Calls [[fetchAll]] under the hood so pagination uses the base trait's stream-of-pages implementation.
   *
   * Memory note: one session's vote list fits in a single in-memory list (House has ≤ ~600 roll calls/year). No
   * streaming gymnastics required here — the expensive fan-out happens downstream on [[fetchMembersVotePositions]].
   *
   * A `lookbackDays` of `0` or negative keeps every item regardless of `updateDate`, which is useful for back-fill
   * runs.
   */
  def fetchRecentVotes: F[List[VoteListItemDTO]] = {
    val params = FetchParams(pageSize = config.pageSize)
    fetchAll(params).compile.toList.map(all => filterByLookback(all, houseConfig.lookbackDays))
  }

  /**
   * Parse a URL string into an http4s `Uri`, raising [[HouseVoteFetchFailed]] with our context on malformed input.
   * `voteNumber` is `None` for list calls (where the failure context is just congress/session) and `Some(n)` for
   * members calls (where we want to surface the specific vote that triggered the bad URL).
   */
  private[api] def parseUri(raw: String, voteNumber: Option[Int]): F[Uri] =
    Uri.fromString(raw) match {
      case Right(uri) => temporal.pure(uri)
      case Left(err) =>
        temporal.raiseError(
          HouseVoteFetchFailed(
            congress = houseConfig.congress,
            session = houseConfig.session,
            voteNumber = voteNumber,
            detail = s"Invalid URL: ${err.sanitized}",
            cause = err,
          )
        )
    }

  private def raiseApiError[A](response: org.http4s.Response[F]): F[A] =
    response
      .as[String]
      .recover { case _ => response.status.reason }
      .flatMap { body =>
        val err = HouseVoteApiHttpError(response.status.code, body)
        temporal.raiseError[A](err)
      }

}

object HouseVotesApiClient {

  def apply[F[_]: Temporal](
    config: CongressGovClientConfig,
    houseConfig: HouseVotesConfig,
    client: Client[F],
    retryWrapper: RetryWrapper[F],
  ): HouseVotesApiClient[F] =
    new HouseVotesApiClient[F](config, houseConfig, client, retryWrapper, Temporal[F])

  /**
   * Apply client-side lookback filter: sort DESC by `updateDate`, then keep items within the configured window.
   *
   * Broken out as a pure function (and `private[api]`) so the sort/filter logic can be unit-tested independently of the
   * HTTP stack. Items with no `updateDate` (API omits it) sort last and are dropped whenever the filter is active,
   * since we can't tell if they're inside the window.
   *
   * A `lookbackDays <= 0` disables the filter — keeps everything, still sorted DESC.
   */
  private[api] def filterByLookback(
    items: List[VoteListItemDTO],
    lookbackDays: Int,
  ): List[VoteListItemDTO] =
    filterByLookback(items, lookbackDays, Instant.now())

  private[api] def filterByLookback(
    items: List[VoteListItemDTO],
    lookbackDays: Int,
    now: Instant,
  ): List[VoteListItemDTO] = {
    val parsedPairs: List[(VoteListItemDTO, Option[Instant])] =
      items.map { item =>
        val parsed = item.updateDate.flatMap(parseUpdateDate)
        (item, parsed)
      }

    val sortedDesc = parsedPairs.sortWith { (leftPair, rightPair) =>
      val (_, leftUpdatedAt)  = leftPair
      val (_, rightUpdatedAt) = rightPair
      (leftUpdatedAt, rightUpdatedAt) match {
        case (Some(leftTime), Some(rightTime)) => leftTime.isAfter(rightTime)
        case (Some(_), None)                   => true
        case (None, Some(_))                   => false
        case (None, None)                      => false
      }
    }

    if (lookbackDays <= 0) { sortedDesc.map(_._1) }
    else {
      val cutoff = now.minus(lookbackDays.toLong, ChronoUnit.DAYS)
      sortedDesc.collect {
        case (item, Some(ts)) if !ts.isBefore(cutoff) => item
      }
    }
  }

  /**
   * Parse Congress.gov `updateDate` strings. The API emits ISO-8601 with offset (e.g., `2025-09-09T18:53:19-04:00`);
   * `Instant.parse` handles that directly via `DateTimeFormatter.ISO_OFFSET_DATE_TIME` under the hood. Returns `None`
   * for malformed input so we fall through to "sort last / drop when filter active" rather than fail the whole fetch
   * because of one bad row.
   */
  private[api] def parseUpdateDate(raw: String): Option[Instant] =
    scala.util
      .Try(java.time.OffsetDateTime.parse(raw).toInstant)
      .toOption
      .orElse(scala.util.Try(Instant.parse(raw)).toOption)

  /**
   * API list response envelope: `{"houseRollCallVotes": [ ... ], "pagination": {...}}`. Also handles the case where the
   * API returns an unusual wrapper for empty/edge-case pages by falling back to "no items".
   *
   * The API response field names don't match the shared-models `VoteListItemDTO` field names in three places
   * (`sourceDataURL` vs `sourceDataUrl`; `identifier` is an integer not a string; no `chamber` field). We supply a
   * custom decoder so the client can consume the real-world shape without waiting for another shared-models bump.
   */
  final private[api] case class HouseVoteListEnvelope(
    items: List[VoteListItemDTO],
    pagination: Option[repcheck.shared.models.congress.dto.common.PaginationInfoDTO],
  )

  private[api] object HouseVoteListEnvelope {

    implicit private val voteListItemDecoder: Decoder[VoteListItemDTO] = Decoder.instance { c =>
      for {
        congress          <- c.downField("congress").as[Int]
        rollCallNumber    <- c.downField("rollCallNumber").as[Int]
        sessionNumber     <- c.downField("sessionNumber").as[Option[Int]]
        startDate         <- c.downField("startDate").as[Option[String]]
        updateDate        <- c.downField("updateDate").as[Option[String]]
        result            <- c.downField("result").as[Option[String]]
        voteType          <- c.downField("voteType").as[Option[String]]
        legislationNumber <- c.downField("legislationNumber").as[Option[String]]
        legislationType   <- c.downField("legislationType").as[Option[String]]
        legislationUrl    <- c.downField("legislationUrl").as[Option[String]]
        url               <- c.downField("url").as[Option[String]]
        identifier        <- decodeIdentifier(c)
        sourceDataUrl     <- decodeSourceDataUrl(c)
      } yield VoteListItemDTO(
        congress = congress,
        chamber = "House",
        rollCallNumber = rollCallNumber,
        sessionNumber = sessionNumber,
        startDate = startDate,
        updateDate = updateDate,
        result = result,
        voteType = voteType,
        legislationNumber = legislationNumber,
        legislationType = legislationType,
        legislationUrl = legislationUrl,
        url = url,
        identifier = identifier,
        sourceDataUrl = sourceDataUrl,
      )
    }

    implicit val decoder: Decoder[HouseVoteListEnvelope] = Decoder.instance { c =>
      for {
        items <- c
          .downField("houseRollCallVotes")
          .as[Option[List[VoteListItemDTO]]]
          .map(_.getOrElse(List.empty[VoteListItemDTO]))
        pagination <- c
          .downField("pagination")
          .as[Option[repcheck.shared.models.congress.dto.common.PaginationInfoDTO]]
      } yield HouseVoteListEnvelope(items, pagination)
    }

  }

  /**
   * API members response envelope: `{"houseRollCallVoteMemberVotes": {...}}`. The inner object is a `HouseVoteMembers`
   * — same fields as the detail envelope plus `voteQuestion` + `results`. The `results` array uses
   * `bioguideID`/`voteParty`/`voteState`, which we map into the shared-models `VoteResultDTO` fields
   * (`memberId`/`party`/`state`) inside a custom member-result decoder.
   */
  final private[api] case class HouseVoteMembersEnvelope(data: VoteMembersDTO)

  private[api] object HouseVoteMembersEnvelope {

    implicit private val voteResultDecoder: Decoder[VoteResultDTO] = Decoder.instance { c =>
      for {
        bioguideId <- c.downField("bioguideID").as[Option[String]]
        firstName  <- c.downField("firstName").as[Option[String]]
        lastName   <- c.downField("lastName").as[Option[String]]
        voteCast   <- c.downField("voteCast").as[Option[String]]
        voteParty  <- c.downField("voteParty").as[Option[String]]
        voteState  <- c.downField("voteState").as[Option[String]]
      } yield VoteResultDTO(
        memberId = bioguideId,
        firstName = firstName,
        lastName = lastName,
        voteCast = voteCast,
        party = voteParty,
        state = voteState,
      )
    }

    implicit private val voteMembersDecoder: Decoder[VoteMembersDTO] = Decoder.instance { c =>
      for {
        congress          <- c.downField("congress").as[Int]
        rollCallNumber    <- c.downField("rollCallNumber").as[Int]
        sessionNumber     <- c.downField("sessionNumber").as[Option[Int]]
        startDate         <- c.downField("startDate").as[Option[String]]
        updateDate        <- c.downField("updateDate").as[Option[String]]
        result            <- c.downField("result").as[Option[String]]
        voteType          <- c.downField("voteType").as[Option[String]]
        legislationNumber <- c.downField("legislationNumber").as[Option[String]]
        legislationType   <- c.downField("legislationType").as[Option[String]]
        legislationUrl    <- c.downField("legislationUrl").as[Option[String]]
        url               <- c.downField("url").as[Option[String]]
        identifier        <- decodeIdentifier(c)
        sourceDataUrl     <- decodeSourceDataUrl(c)
        voteQuestion      <- c.downField("voteQuestion").as[Option[String]]
        results           <- c.downField("results").as[Option[List[VoteResultDTO]]]
      } yield VoteMembersDTO(
        congress = congress,
        chamber = "House",
        rollCallNumber = rollCallNumber,
        sessionNumber = sessionNumber,
        startDate = startDate,
        updateDate = updateDate,
        result = result,
        voteType = voteType,
        legislationNumber = legislationNumber,
        legislationType = legislationType,
        legislationUrl = legislationUrl,
        url = url,
        identifier = identifier,
        sourceDataUrl = sourceDataUrl,
        voteQuestion = voteQuestion,
        results = Some(results.getOrElse(List.empty)),
      )
    }

    implicit val decoder: Decoder[HouseVoteMembersEnvelope] = Decoder.instance { c =>
      c.downField("houseRollCallVoteMemberVotes").as[VoteMembersDTO].map(HouseVoteMembersEnvelope.apply)
    }

  }

  /**
   * Decode the API's integer `identifier` (e.g., `1191202517`) while keeping the shared-models DTO field typed as
   * `Option[String]`. The field is omitted on some edge cases in the API, so we tolerate absence. We accept strings
   * too, in case the API toggles types between integer and string across versions — both coerce to `Option[String]`.
   */
  private def decodeIdentifier(c: io.circe.HCursor): Decoder.Result[Option[String]] =
    c.downField("identifier").as[Option[Long]] match {
      case Right(Some(l)) => Right(Some(l.toString))
      case Right(None)    => c.downField("identifier").as[Option[String]]
      case Left(_)        => c.downField("identifier").as[Option[String]]
    }

  /**
   * The API field is `sourceDataURL` (fully uppercase URL) but the shared-models DTO uses `sourceDataUrl` (lowercase
   * trailing letters). Circe semi-auto derivation is case-sensitive, so we read from the actual API key and write into
   * the DTO field in a single step.
   */
  private def decodeSourceDataUrl(c: io.circe.HCursor): Decoder.Result[Option[String]] =
    c.downField("sourceDataURL").as[Option[String]].flatMap {
      case s @ Some(_) => Right(s)
      case None        => c.downField("sourceDataUrl").as[Option[String]]
    }

}
