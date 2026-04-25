package repcheck.ingestion.votes.xml

import java.util.UUID

import scala.xml.Elem

import cats.effect.Async
import cats.syntax.all._

import org.http4s.client.Client

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.ingestion.votes.config.SenateVoteXmlConfig
import repcheck.ingestion.votes.errors.{
  SenateVoteFetchFailed,
  SenateVoteNumberOutOfRange,
  SenateVoteXmlErrorClassifier,
  SenateVoteXmlHttpError,
}
import repcheck.pipeline.models.errors.RetryWrapper
import repcheck.shared.models.congress.dto.vote.SenateVoteXmlDTO

/**
 * Fetches and decodes Senate roll-call XML from senate.gov.
 *
 * The client has two public entry points:
 *   - `fetchVote(congress, session, voteNumber)` — retrieves a single vote's `<roll_call_vote>` document and decodes it
 *     into a [[SenateVoteXmlDTO]].
 *   - `fetchVoteIndex(congress, session)` — retrieves the session's `vote_menu_{congress}_{session}.xml` index and
 *     decodes it into a list of [[SenateVoteIndexEntry]]s so the processor can iterate votes in order.
 *
 * URL construction is delegated to [[SenateVoteUrls]] so both this client and
 * [[repcheck.ingestion.votes.pipeline.SenateVoteConverter]] (when deriving `sourceDataUrl`) share a single source of
 * truth. Senate.gov uses TWO sub-paths under `{baseUrl}`:
 *   - Index: `{baseUrl}/roll_call_lists/vote_menu_{congress}_{session}.xml`
 *   - Vote: `{baseUrl}/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{voteNumber:05d}.xml`
 *
 * Vote numbers are zero-padded to 5 digits via `f"%05d"` (plan decision 13). Numbers ≥ 100,000 overflow the 5-digit
 * field and cannot be fetched from senate.gov; the client rejects them up-front with [[SenateVoteFetchFailed]] rather
 * than issuing a malformed request.
 *
 * ==Error handling==
 *   - Underlying HTTP failures from [[XmlFeedClient]] (non-2xx status, network errors) bubble up and are classified via
 *     [[SenateVoteXmlErrorClassifier]]. Transient statuses (429/500/502/503/504) are retried per `config.retry`;
 *     exhausted retries wrap the cause in [[SenateVoteFetchFailed]].
 *   - Decoder failures from [[SenateVoteXmlDecoder]] ([[XmlParseFailed]]) are wrapped in [[SenateVoteFetchFailed]] with
 *     `cause = XmlParseFailed` so operators see both layers in logs.
 *
 * ==Rate limiting==
 * The constructor receives a pre-wrapped `Client[F]` — the votes-pipeline wires the `rateLimitedClient` helper from the
 * P1.1 scaffold (`VotesPipeline.rateLimitedClient`) to hold a one-permit semaphore with `config.requestDelay` between
 * requests. The client here does NOT construct or re-wrap the HTTP client; pacing is handled upstream.
 */
class SenateVoteXmlClient[F[_]: Async](
  httpClient: Client[F],
  retryWrapper: RetryWrapper[F],
  config: SenateVoteXmlConfig,
  logger: PipelineLogger[F],
) {

  private val stepName: String = "senate-vote-xml-fetch"

  /**
   * Internal [[XmlFeedClient]] adapter. The shared client runs its own retry loop, but this client layers the
   * project-standard [[RetryWrapper]] on top for transient/systemic classification via
   * [[SenateVoteXmlErrorClassifier]]. To avoid double-retrying (shared client × outer wrapper = N × M attempts), we
   * hand the shared client a zero-retry `RetryConfig` and let the outer wrapper be the single source of retry
   * decisions. Other shared-client fields (`initialBackoffMs`, `maxBackoffMs`, `backoffMultiplier`) are irrelevant when
   * `maxRetries = 0`.
   */
  private val xmlFeedClient: XmlFeedClient[F] = {
    implicit val log4cats: Logger[F] = SenateVoteXmlClient.noOpLogger[F]
    val zeroRetry                    = config.retry.copy(maxRetries = 0)
    XmlFeedClient.make[F](httpClient, zeroRetry)
  }

  /**
   * Fetch a single Senate roll-call vote and decode it into a [[SenateVoteXmlDTO]].
   *
   * `voteNumber` must satisfy `1 <= voteNumber < 100000` (senate.gov's 5-digit URL segment). Values outside that range
   * fail fast with [[SenateVoteFetchFailed]] before any HTTP call is made.
   */
  def fetchVote(congress: Int, session: Int, voteNumber: Int): F[SenateVoteXmlDTO] =
    SenateVoteXmlClient.validateVoteNumber(voteNumber) match {
      case Left(err) =>
        Async[F].raiseError[SenateVoteXmlDTO](
          SenateVoteFetchFailed(
            congress = congress,
            session = session,
            voteNumber = Some(voteNumber),
            detail = err,
            cause = SenateVoteNumberOutOfRange(voteNumber, err),
          )
        )
      case Right(_) =>
        val url = buildVoteUrl(congress, session, voteNumber)
        val logCtx =
          LogContext(runId = "", stepName = stepName, additional = Map("url" -> url))
        val correlationId = UUID.randomUUID()

        val operation: F[SenateVoteXmlDTO] =
          logger.info(logCtx, s"Fetching Senate vote XML from $url") *>
            fetchAndDecodeVote(url, congress, session, voteNumber)

        retryWrapper.withRetry(
          operation = operation,
          config = config.retry,
          classifier = SenateVoteXmlErrorClassifier,
          errorFactory = (msg, cause) =>
            SenateVoteFetchFailed(
              congress = congress,
              session = session,
              voteNumber = Some(voteNumber),
              detail = msg,
              cause = cause,
            ),
          correlationId = correlationId,
        )
    }

  /**
   * Fetch the roll-call vote index for a congress/session and decode it into a list of [[SenateVoteIndexEntry]]s.
   */
  def fetchVoteIndex(congress: Int, session: Int): F[List[SenateVoteIndexEntry]] = {
    val url = buildIndexUrl(congress, session)
    val logCtx =
      LogContext(runId = "", stepName = stepName, additional = Map("url" -> url))
    val correlationId = UUID.randomUUID()

    val operation: F[List[SenateVoteIndexEntry]] =
      logger.info(logCtx, s"Fetching Senate vote index from $url") *>
        fetchAndDecodeIndex(url, congress, session)

    val withRetry = retryWrapper.withRetry(
      operation = operation,
      config = config.retry,
      classifier = SenateVoteXmlErrorClassifier,
      errorFactory = (msg, cause) =>
        SenateVoteFetchFailed(
          congress = congress,
          session = session,
          voteNumber = None,
          detail = msg,
          cause = cause,
        ),
      correlationId = correlationId,
    )

    // A 404 on the index endpoint means "no votes published for this (congress, session)" — not an error.
    // Common for future sessions that haven't started yet, or for old congresses where senate.gov never
    // hosted electronic vote indexes. Recover to an empty list so per-(c,s) iteration in the pipeline
    // continues cleanly to the next pair instead of marking the chamber as Failed. Non-404 errors
    // re-raise unchanged so the chamber-level handler in VoteProcessor still observes them.
    withRetry.handleErrorWith {
      case e: SenateVoteFetchFailed if e.statusCode == 404 =>
        logger
          .info(logCtx, s"Senate vote index 404 for $congress/$session — no votes for this session, treating as empty")
          .as(List.empty[SenateVoteIndexEntry])
      case other => Async[F].raiseError(other)
    }
  }

  private def fetchAndDecodeVote(
    url: String,
    congress: Int,
    session: Int,
    voteNumber: Int,
  ): F[SenateVoteXmlDTO] =
    fetchElem(url, congress, session, Some(voteNumber))
      .flatMap { elem =>
        SenateVoteXmlDecoder.decodeVote(elem) match {
          case Right(dto) => Async[F].pure(dto)
          case Left(pf) =>
            Async[F].raiseError[SenateVoteXmlDTO](
              SenateVoteFetchFailed(
                congress = congress,
                session = session,
                voteNumber = Some(voteNumber),
                detail = pf.getMessage,
                cause = pf,
              )
            )
        }
      }

  private def fetchAndDecodeIndex(
    url: String,
    congress: Int,
    session: Int,
  ): F[List[SenateVoteIndexEntry]] =
    fetchElem(url, congress, session, None)
      .flatMap { elem =>
        SenateVoteXmlDecoder.decodeIndex(elem) match {
          case Right(entries) => Async[F].pure(entries)
          case Left(pf) =>
            Async[F].raiseError[List[SenateVoteIndexEntry]](
              SenateVoteFetchFailed(
                congress = congress,
                session = session,
                voteNumber = None,
                detail = pf.getMessage,
                cause = pf,
              )
            )
        }
      }

  /**
   * Wraps [[XmlFeedClient#fetchXml]] so that any [[repcheck.ingestion.common.errors.XmlParseFailed]] from the shared
   * client is promoted into a [[SenateVoteXmlHttpError]] when the underlying cause is an HTTP status failure. The
   * shared client adapts all throwables into its own `XmlParseFailed` (including `UnexpectedStatus` raised by http4s
   * when a non-2xx status is returned); unwrapping the cause lets the votes-pipeline classifier distinguish HTTP status
   * failures from genuine decode errors.
   */
  private def fetchElem(
    url: String,
    congress: Int,
    session: Int,
    voteNumber: Option[Int],
  ): F[Elem] =
    xmlFeedClient.fetchXml(url).adaptError {
      case sharedPF: repcheck.ingestion.common.errors.XmlParseFailed =>
        SenateVoteXmlClient.unwrapHttpStatus(Option(sharedPF.cause)) match {
          case Some(statusCode) =>
            SenateVoteXmlHttpError(
              statusCode,
              s"from $url (congress=${congress.toString} session=${session.toString} voteNumber=${voteNumber.fold("-")(_.toString)})",
            )
          case None =>
            sharedPF
        }
    }

  private def buildVoteUrl(congress: Int, session: Int, voteNumber: Int): String =
    SenateVoteUrls.voteXmlUrl(config.baseUrl, congress, session, voteNumber)

  private def buildIndexUrl(congress: Int, session: Int): String =
    SenateVoteUrls.voteIndexUrl(config.baseUrl, congress, session)

}

object SenateVoteXmlClient {

  /** Minimum accepted vote number. senate.gov's URL scheme uses 5-digit left-padded roll-call numbers. */
  val MinVoteNumber: Int = 1

  /**
   * Upper bound (exclusive) on vote numbers. senate.gov's `%05d` URL segment cannot represent six-digit roll-call
   * numbers, so 100000 and above are rejected up-front.
   */
  val VoteNumberUpperBoundExclusive: Int = 100000

  private[xml] def validateVoteNumber(voteNumber: Int): Either[String, Unit] =
    if (voteNumber < MinVoteNumber) {
      Left(s"voteNumber must be >= $MinVoteNumber but was ${voteNumber.toString}")
    } else if (voteNumber >= VoteNumberUpperBoundExclusive) {
      Left(
        s"voteNumber must fit in 5 digits (< $VoteNumberUpperBoundExclusive) but was ${voteNumber.toString}"
      )
    } else {
      Right(())
    }

  /**
   * Slf4j-backed no-op-ish `Logger[F]` instance that [[XmlFeedClient]] requires internally. Retry-loop warnings from
   * the shared client are best-effort diagnostics; the votes-pipeline's [[PipelineLogger]] is the primary logging
   * channel and is invoked directly at the boundary of this client.
   */
  private[xml] def noOpLogger[F[_]: cats.effect.Sync]: Logger[F] =
    Slf4jLogger.getLoggerFromName[F]("SenateVoteXmlClient")

  /**
   * Walk an exception chain in search of an http4s `UnexpectedStatus`. Returns the HTTP status code if found, or `None`
   * if the chain does not carry an HTTP status failure. Defensive against missing causes (`Option(..)` at the entry
   * point rules out a null root cause) and cycles (`inner ne cause` prevents infinite recursion).
   */
  private[xml] def unwrapHttpStatus(causeOpt: Option[Throwable]): Option[Int] =
    causeOpt.flatMap { cause =>
      cause match {
        case us: org.http4s.client.UnexpectedStatus => Some(us.status.code)
        case other =>
          Option(other.getCause) match {
            case Some(inner) if inner ne other => unwrapHttpStatus(Some(inner))
            case _                             => None
          }
      }
    }

}
