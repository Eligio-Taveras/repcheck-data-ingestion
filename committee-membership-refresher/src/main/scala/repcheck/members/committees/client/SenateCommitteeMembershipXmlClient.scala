package repcheck.members.committees.client

import scala.util.Try
import scala.xml.{Node, NodeSeq}

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.model.{CommitteeCodeNormalizer, SenateCommitteeMemberXmlDTO}

/**
 * Fetches per-committee XML from senate.gov by committee code. Returns a Stream of member entries including both parent
 * committee and subcommittee members. Detects soft-404 (HTML response returned for codes that have no roster).
 */
class SenateCommitteeMembershipXmlClient[F[_]: Async](
  xmlFeedClient: XmlFeedClient[F],
  config: CommitteeMembershipConfig,
  logger: PipelineLogger[F],
) {

  private val StepName: String = "senate-committee-xml-fetch"

  def fetchCommitteeMembers(
    committeeCode: String,
    runId: Long,
  ): Stream[F, SenateCommitteeMemberXmlDTO] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName, entityId = Some(committeeCode))
    val url    = s"${config.senateCommitteeBaseUrl}/$committeeCode.xml"

    Stream
      .eval(
        logger.info(logCtx, s"Fetching Senate committee XML for $committeeCode from $url") *>
          xmlFeedClient.fetchXml(url)
      )
      .flatMap { elem =>
        if (isSoft404(elem)) {
          Stream
            .eval(
              logger.warn(logCtx, s"Senate committee $committeeCode returned HTML (soft-404), skipping")
            )
            .drain
        } else {
          val parentMembers = SenateCommitteeMemberXmlParser.parseMembers(elem, committeeCode, isSubcommittee = false)
          val subMembers    = SenateCommitteeMemberXmlParser.parseSubcommitteeMembers(elem, committeeCode)
          val all           = parentMembers ++ subMembers
          Stream
            .eval(logger.info(logCtx, s"Parsed ${all.size.toString} members for Senate committee $committeeCode"))
            .flatMap(_ => Stream.emits(all))
        }
      }
      .handleErrorWith { e =>
        Stream.eval(
          logger.warn(logCtx, s"Failed to fetch committee $committeeCode: ${e.getMessage}")
        ) >> Stream.empty
      }
  }

  private def isSoft404(elem: scala.xml.Elem): Boolean =
    elem.label.equalsIgnoreCase("html") || elem.label.equalsIgnoreCase("HTML")

}

private[client] object SenateCommitteeMemberXmlParser {

  def parseMembers(
    elem: scala.xml.Elem,
    parentCommitteeCode: String,
    isSubcommittee: Boolean,
  ): List[SenateCommitteeMemberXmlDTO] = {
    // Use direct child navigation (\ not \\) to avoid picking up subcommittee members
    val directMembers = (elem \ "members" \ "member").toList
    directMembers.flatMap(parseMember(_, parentCommitteeCode, isSubcommittee))
  }

  def parseSubcommitteeMembers(
    elem: scala.xml.Elem,
    parentCommitteeCode: String,
  ): List[SenateCommitteeMemberXmlDTO] =
    (elem \\ "subcommittee").toList.flatMap { subNode =>
      val subCode = textOption(subNode \@ "code")
        .map(CommitteeCodeNormalizer.fromSenateXml)
        .getOrElse(parentCommitteeCode)
      (subNode \ "members" \ "member").toList.flatMap(parseMember(_, subCode, isSubcommittee = true))
    }

  private[client] def parseMember(
    node: Node,
    committeeCode: String,
    isSubcommittee: Boolean,
  ): Option[SenateCommitteeMemberXmlDTO] = {
    val firstName = textOption(node \ "first_name").orElse(textOption(node \ "firstname"))
    val lastName  = textOption(node \ "last_name").orElse(textOption(node \ "lastname"))
    val state     = textOption(node \ "state")
    val party     = textOption(node \ "party")
    val position  = textOption(node \ "position")
    val rank      = textOption(node \ "rank").flatMap(r => Try(r.toInt).toOption)
    val bioguide  = textOption(node \ "bioguide_id").orElse(textOption(node \ "bioguideId"))

    for {
      fn <- firstName
      ln <- lastName
      st <- state
      p  <- party
    } yield SenateCommitteeMemberXmlDTO(
      bioguideId = bioguide,
      firstName = fn,
      lastName = ln,
      state = st,
      party = p,
      position = position,
      rank = rank,
      committeeCode = committeeCode,
      isSubcommittee = isSubcommittee,
    )
  }

  private def textOption(s: String): Option[String] = {
    val trimmed = s.trim
    if (trimmed.isEmpty) None else Some(trimmed)
  }

  private def textOption(ns: NodeSeq): Option[String] = {
    val raw = ns.text.trim
    if (raw.isEmpty) None else Some(raw)
  }

}
