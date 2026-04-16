package repcheck.members.lismapping.client

import scala.util.Try
import scala.xml.{Elem, Node, NodeSeq}

import repcheck.shared.models.congress.dto.vote.{SenatorLookupXmlDTO, ServicePeriodDTO}

/**
 * Pure helper that parses the senate.gov `senators_cfm.xml` feed into [[SenatorLookupXmlDTO]]s.
 *
 * Assumed schema (matches publicly documented senate.gov feed shape):
 *
 * {{{
 *   <contact_information>
 *     <member>
 *       <lis_member_id>S123</lis_member_id>
 *       <bioguide_id>S000148</bioguide_id>
 *       <first_name>Jane</first_name>
 *       <last_name>Doe</last_name>
 *       <party>D</party>
 *       <state>NY</state>
 *       <class>1</class>
 *       <is_current>true</is_current>
 *       <service_dates>
 *         <service>
 *           <congress>118</congress>
 *           <start_date>2023-01-03</start_date>
 *           <end_date>2025-01-03</end_date>
 *         </service>
 *       </service_dates>
 *     </member>
 *   </contact_information>
 * }}}
 *
 * Resilience: an individual `<member>` entry that fails to parse (missing required field or malformed numeric value)
 * is skipped rather than aborting the whole feed. Required fields for a senator to be kept are `lisId` (from
 * `<lis_member_id>`), `bioguideId` (from `<bioguide_id>`), `firstName`, `lastName`, `party`, and `state`. Everything
 * else is optional.
 */
object SenatorXmlParser {

  /**
   * Parse a senators_cfm.xml document into a list of DTOs. Malformed entries are skipped silently. Caller may log the
   * delta between `(root \\ "member").size` and the returned list size if it wants to report dropped entries.
   */
  def parse(elem: Elem): List[SenatorLookupXmlDTO] =
    (elem \\ "member").toList.flatMap(parseMember)

  private def parseMember(node: Node): Option[SenatorLookupXmlDTO] = {
    val lisId      = textOption(node \ "lis_member_id")
    val bioguideId = textOption(node \ "bioguide_id")
    val firstName  = textOption(node \ "first_name")
    val lastName   = textOption(node \ "last_name")
    val party      = textOption(node \ "party")
    val state      = textOption(node \ "state")

    val senateClass  = textOption(node \ "class").flatMap(s => Try(s.toInt).toOption)
    val isCurrent    = parseBoolean(textOption(node \ "is_current"))
    val serviceDates = parseServiceDates(node \ "service_dates")

    for {
      lis  <- lisId
      bio  <- bioguideId
      fn   <- firstName
      ln   <- lastName
      p    <- party
      st   <- state
    } yield SenatorLookupXmlDTO(
      lisId        = lis,
      bioguideId   = bio,
      firstName    = fn,
      lastName     = ln,
      party        = p,
      state        = st,
      senateClass  = senateClass,
      serviceDates = serviceDates,
      isCurrent    = isCurrent,
    )
  }

  private def parseServiceDates(node: NodeSeq): List[ServicePeriodDTO] = {
    val services = node \ "service"
    if (services.isEmpty) {
      // Fall back to a single <service_dates> element directly carrying start/end/congress sub-fields.
      val flattened = parseServicePeriod(node)
      flattened.toList
    } else {
      services.toList.flatMap(parseServicePeriod)
    }
  }

  private def parseServicePeriod(node: NodeSeq): Option[ServicePeriodDTO] = {
    val congress  = textOption(node \ "congress").flatMap(s => Try(s.toInt).toOption)
    val startDate = textOption(node \ "start_date")
    val endDate   = textOption(node \ "end_date")
    if (congress.isEmpty && startDate.isEmpty && endDate.isEmpty) {
      None
    } else {
      Some(ServicePeriodDTO(congress = congress, startDate = startDate, endDate = endDate))
    }
  }

  private def textOption(ns: NodeSeq): Option[String] = {
    val raw = ns.text.trim
    if (raw.isEmpty) None else Some(raw)
  }

  private def parseBoolean(raw: Option[String]): Boolean =
    raw.map(_.trim.toLowerCase).exists {
      case "true" | "yes" | "y" | "1" => true
      case _                          => false
    }

}
