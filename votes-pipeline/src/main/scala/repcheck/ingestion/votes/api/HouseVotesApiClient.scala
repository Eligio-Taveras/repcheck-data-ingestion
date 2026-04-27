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

import fs2.Stream

import repcheck.ingestion.common.api.{CongressGovClientConfig, FetchParams, PagedResponse}
import repcheck.ingestion.votes.config.HouseVotesConfig
import repcheck.ingestion.votes.errors.{HouseVoteApiErrorClassifier, HouseVoteApiHttpError, HouseVoteFetchFailed}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.vote.{VoteListItemDTO, VoteMembersDTO, VoteResultDTO}

/**
 * Congress.gov beta `/house-vote` API client.
 *
 * Per P6.H5 the (congress, session) tuple is NOT bound at construction time — every list/detail call accepts them as
 * explicit arguments. The pipeline iterates over the resolved congresses list (env or DB-derived) and calls
 * `fetchRecentVotes(congress, session)` once per pair. Pagination, lookback filtering, and retries all sit inside each
 * call.
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
 * The passed-in `Client[F]` must already be wrapped by `RateLimitedHttpClient.make` (per app-level wiring in
 * `VotesPipelineResources.build`) — this class does NOT apply its own rate limiter. `pageDelay` in the base trait is
 * read from `CongressGovClientConfig` for the sake of the `fetchAll` default implementation, but the real inter-request
 * pacing is enforced by the wrapper's semaphore.
 */
class HouseVotesApiClient[F[_]](
  config: CongressGovClientConfig,
  houseConfig: HouseVotesConfig,
  client: Client[F],
  retryWrapper: RetryWrapper[F],
  temporalInstance: Temporal[F],
) {

  protected def pageDelay: FiniteDuration = config.pageDelay

  implicit protected def temporal: Temporal[F] = temporalInstance

  /** Decoder-scoping: keep the API-shape wrappers lexically next to the client that uses them. */
  import HouseVotesApiClient._

  /**
   * Fetch a single page of the list endpoint for the given (congress, session). Internal — used by [[fetchAllPages]] to
   * walk pagination.
   */
  private def fetchPage(congress: Int, session: Int, params: FetchParams): F[PagedResponse[VoteListItemDTO]] =
    parseUri(s"${config.baseUrl}/house-vote/$congress/$session", congress, session, None).flatMap { baseUri =>
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
            congress = congress,
            session = session,
            voteNumber = None,
            detail = msg,
            cause = cause,
          ),
        correlationId = UUID.randomUUID(),
      )
    }

  /**
   * Walk all pages for the given (congress, session). Mirrors the unfoldEval pattern from the previous
   * `CongressGovPaginatedClient.fetchAll` base default — inlined here so the per-call (congress, session) closes over
   * the inner `fetchPage` invocation.
   */
  private def fetchAllPages(congress: Int, session: Int, params: FetchParams): Stream[F, VoteListItemDTO] = {
    val F = temporal
    Stream
      .unfoldEval[F, Option[FetchParams], List[VoteListItemDTO]](Some(params)) {
        case None => F.pure(None)
        case Some(currentParams) =>
          F.flatMap(fetchPage(congress, session, currentParams)) { response =>
            val items = response.items
            if (items.size < currentParams.pageSize) {
              F.pure(Some((items, None)))
            } else {
              val nextParams = currentParams.copy(offset = currentParams.offset + currentParams.pageSize)
              F.as(F.sleep(pageDelay), Some((items, Some(nextParams))))
            }
          }
      }
      .flatMap(Stream.emits)
  }

  /**
   * Fetch member positions for a specific vote. Different return type (`VoteMembersDTO`) than the list endpoint, so
   * this method does NOT extend the paginated protocol — it's a direct, single-request call.
   *
   * Uses [[HouseVoteApiErrorClassifier]] for transient-vs-systemic classification just like `fetchPage`; a 404 for a
   * specific vote is classified as Systemic (not retried) and surfaces as [[HouseVoteFetchFailed]] with the caller's
   * `voteNumber` preserved so the log entry identifies exactly which vote was missing.
   */
  /**
   * Fetch the members + their cast votes for a specific House roll-call.
   *
   * Returns `None` when Congress.gov reports no member-vote data for the vote — older votes (early 117th-Congress
   * roll-calls and before) sometimes return `{"houseRollCallVoteMemberVotes": []}` (empty array) instead of the normal
   * `{...}` object. Callers should treat None as `ProcessingResult.Skipped` rather than failure: the vote exists per
   * the list endpoint but has no member-position records to ingest.
   */
  def fetchMembersVotePositions(congress: Int, session: Int, voteNumber: Int): F[Option[VoteMembersDTO]] = {
    val url = s"${config.baseUrl}/house-vote/$congress/$session/$voteNumber/members"
    parseUri(url, congress, session, Some(voteNumber)).flatMap { baseUri =>
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
   * Fetch every vote for the given (congress, session), sort newest-first, and drop anything older than the lookback
   * cutoff. Pagination + lookback filtering all run inside this call.
   *
   * Memory note: one session's vote list fits in a single in-memory list (House has ≤ ~600 roll calls/year). No
   * streaming gymnastics required here — the expensive fan-out happens downstream on [[fetchMembersVotePositions]].
   *
   * A `lookbackDays` of `0` or negative keeps every item regardless of `updateDate`, which is useful for back-fill
   * runs.
   */
  def fetchRecentVotes(congress: Int, session: Int): F[List[VoteListItemDTO]] = {
    val params = FetchParams(pageSize = config.pageSize)
    val operation = fetchAllPages(congress, session, params).compile.toList
      .map(all => filterByLookback(all, houseConfig.lookbackDays))

    // A 404 on the list endpoint means "no votes published for this (congress, session)" — not an error.
    // Common for future sessions that haven't started yet, or for old congresses pre-electronic-archive.
    // Recover to an empty list so per-(c,s) iteration in the pipeline continues cleanly instead of
    // marking the chamber as Failed.
    operation.recoverWith {
      case e: HouseVoteFetchFailed if isHttp404(e.cause) =>
        temporal.pure(List.empty[VoteListItemDTO])
    }
  }

  private def isHttp404(cause: Throwable): Boolean = cause match {
    case h: HouseVoteApiHttpError => h.statusCode == 404
    case _                        => false
  }

  /**
   * Parse a URL string into an http4s `Uri`, raising [[HouseVoteFetchFailed]] with our context on malformed input.
   * `voteNumber` is `None` for list calls (where the failure context is just congress/session) and `Some(n)` for
   * members calls (where we want to surface the specific vote that triggered the bad URL).
   */
  private[api] def parseUri(raw: String, congress: Int, session: Int, voteNumber: Option[Int]): F[Uri] =
    Uri.fromString(raw) match {
      case Right(uri) => temporal.pure(uri)
      case Left(err) =>
        temporal.raiseError(
          HouseVoteFetchFailed(
            congress = congress,
            session = session,
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
   * API members response envelope: `{"houseRollCallVoteMemberVotes": {...}}`. The inner field is normally an object (a
   * `HouseVoteMembers` body — same fields as the detail envelope plus `voteQuestion` + `results`). For older votes
   * where Congress.gov has no member-vote data the API returns an EMPTY ARRAY there instead —
   * `{"houseRollCallVoteMemberVotes": []}` — which we represent as `data = None`. Callers detect None and treat the
   * vote as Skipped.
   *
   * The `results` array uses `bioguideID`/`voteParty`/`voteState`, which we map into the shared-models `VoteResultDTO`
   * fields (`memberId`/`party`/`state`) inside a custom member-result decoder.
   */
  final private[api] case class HouseVoteMembersEnvelope(data: Option[VoteMembersDTO])

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
      val field   = c.downField("houseRollCallVoteMemberVotes")
      val isEmpty = field.focus.flatMap(_.asArray).exists(_.isEmpty)
      if (isEmpty) {
        Right(HouseVoteMembersEnvelope(None))
      } else {
        field.as[VoteMembersDTO].map(dto => HouseVoteMembersEnvelope(Some(dto)))
      }
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
