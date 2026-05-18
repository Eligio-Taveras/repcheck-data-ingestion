package repcheck.members.committees.client

import scala.util.Try
import scala.xml.{Node, NodeSeq}

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.members.committees.config.CommitteeMembershipConfig
import repcheck.members.committees.model.{
  CommitteeCodeNormalizer,
  HouseCommitteeAssignmentXmlDTO,
  HouseMemberDataXmlDTO,
}

/**
 * Streams `MemberData.xml` from House clerk and emits one [[HouseMemberDataXmlDTO]] per `<member>`. Members missing a
 * bioguideID are silently skipped.
 */
class HouseMemberDataXmlClient[F[_]: Async](
  xmlFeedClient: XmlFeedClient[F],
  config: CommitteeMembershipConfig,
  logger: PipelineLogger[F],
) {

  private val StepName: String = "house-member-data-xml-fetch"

  def fetchMembers(runId: Long): Stream[F, HouseMemberDataXmlDTO] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName)

    Stream
      .eval(
        logger.info(logCtx, s"Fetching House MemberData.xml from ${config.houseMemberDataUrl}") *>
          xmlFeedClient.fetchXml(config.houseMemberDataUrl)
      )
      .flatMap { elem =>
        val parsed = HouseMemberDataXmlParser.parse(elem)
        Stream
          .eval(logger.info(logCtx, s"Parsed ${parsed.size.toString} House members with committee data"))
          .flatMap(_ => Stream.emits(parsed))
      }
  }

}

private[client] object HouseMemberDataXmlParser {

  def parse(elem: scala.xml.Elem): List[HouseMemberDataXmlDTO] =
    (elem \\ "member").toList.flatMap(parseMember)

  private[client] def parseMember(node: Node): Option[HouseMemberDataXmlDTO] = {
    val memberInfo = node \ "member-info"
    val bioguide   = textOption(memberInfo \ "bioguideID")
    val firstName  = textOption(memberInfo \ "firstname").orElse(textOption(memberInfo \ "first-name"))
    val lastName   = textOption(memberInfo \ "lastname").orElse(textOption(memberInfo \ "last-name"))
    val party      = textOption(memberInfo \ "party")
    val state = textOption(memberInfo \ "state" \ "state-fullname")
      .orElse(textOption(memberInfo \ "state"))
    val district = textOption(memberInfo \ "district").flatMap(d => Try(d.toInt).toOption)

    val committees = parseCommitteeAssignments(node \ "committee-assignments")

    for {
      bio <- bioguide
      fn  <- firstName
      ln  <- lastName
      p   <- party
      st  <- state
    } yield HouseMemberDataXmlDTO(
      bioguideId = bio,
      firstName = fn,
      lastName = ln,
      party = p,
      state = st,
      district = district,
      committees = committees,
    )
  }

  private def parseCommitteeAssignments(assignments: NodeSeq): List[HouseCommitteeAssignmentXmlDTO] = {
    val committees    = (assignments \ "committee").toList.flatMap(parseAssignment)
    val subcommittees = (assignments \ "subcommittee").toList.flatMap(parseAssignment)
    committees ++ subcommittees
  }

  private def parseAssignment(node: Node): Option[HouseCommitteeAssignmentXmlDTO] = {
    val comcode = textOption(node \@ "comcode").orElse(
      textOption(node \@ "subcomcode")
    )
    val name = textOption(node \@ "name")
      .orElse(
        textOption(node \@ "subcomname")
      )
      .getOrElse("")
    val rank = Try((node \@ "rank").trim.toInt).toOption
    val side = textOption(node \@ "side")

    comcode.map { code =>
      HouseCommitteeAssignmentXmlDTO(
        committeeCode = CommitteeCodeNormalizer.fromHouseClerk(code),
        committeeName = name,
        rank = rank,
        side = side,
      )
    }
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
