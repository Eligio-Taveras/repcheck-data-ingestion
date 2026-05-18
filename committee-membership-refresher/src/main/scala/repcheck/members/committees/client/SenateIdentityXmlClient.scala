package repcheck.members.committees.client

import scala.xml.{Node, NodeSeq}

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.model.SenatorIdentityXmlDTO

/**
 * Streams `cvc_member_data.xml` from senate.gov. Emits one [[SenatorIdentityXmlDTO]] per `<senator>` for building the
 * identity map: `(firstName, lastName, state) -> (bioguideId, memberId)`.
 */
class SenateIdentityXmlClient[F[_]: Async](
  xmlFeedClient: XmlFeedClient[F],
  config: CommitteeMembershipConfig,
  logger: PipelineLogger[F],
) {

  private val StepName: String = "senate-identity-xml-fetch"

  def fetchIdentities(runId: Long): Stream[F, SenatorIdentityXmlDTO] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName)

    Stream
      .eval(
        logger.info(logCtx, s"Fetching Senate identity XML from ${config.senateIdentityUrl}") *>
          xmlFeedClient.fetchXml(config.senateIdentityUrl)
      )
      .flatMap { elem =>
        val parsed = SenateIdentityXmlParser.parse(elem)
        Stream
          .eval(logger.info(logCtx, s"Parsed ${parsed.size.toString} Senate identities"))
          .flatMap(_ => Stream.emits(parsed))
      }
  }

}

private[client] object SenateIdentityXmlParser {

  def parse(elem: scala.xml.Elem): List[SenatorIdentityXmlDTO] =
    (elem \\ "senator").toList.flatMap(parseSenator)

  private[client] def parseSenator(node: Node): Option[SenatorIdentityXmlDTO] = {
    val bioguide    = textOption(node \ "bioguide_id").orElse(textOption(node \ "bioguideId"))
    val lisMemberId = textOption(node \ "lis_member_id").orElse(textOption(node \ "lisMemberId"))
    val firstName   = textOption(node \ "first_name").orElse(textOption(node \ "firstname"))
    val lastName    = textOption(node \ "last_name").orElse(textOption(node \ "lastname"))
    val party       = textOption(node \ "party")
    val state       = textOption(node \ "state")

    for {
      bio <- bioguide
      lis <- lisMemberId
      fn  <- firstName
      ln  <- lastName
      p   <- party
      st  <- state
    } yield SenatorIdentityXmlDTO(
      bioguideId = bio,
      lisMemberId = lis,
      firstName = fn,
      lastName = ln,
      party = p,
      state = st,
    )
  }

  private def textOption(ns: NodeSeq): Option[String] = {
    val raw = ns.text.trim
    if (raw.isEmpty) None else Some(raw)
  }

}
