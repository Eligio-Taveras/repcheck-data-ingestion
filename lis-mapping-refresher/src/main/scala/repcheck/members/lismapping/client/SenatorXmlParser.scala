package repcheck.members.lismapping.client

import scala.util.Try
import scala.xml.{Elem, Node, NodeSeq}

import repcheck.shared.models.congress.dto.vote.{SenatorLookupXmlDTO, ServicePeriodDTO}

/**
 * Pure helper that parses the senate.gov `senator-lookup.xml` feed into [[SenatorLookupXmlDTO]]s.
 *
 * Real feed schema (`https://www.senate.gov/about/senator-lookup.xml`):
 *
 * {{{
 *   <senators>
 *     <senator>
 *       <full_name>
 *         <first_name>Tammy</first_name>
 *         <middle_name/>
 *         <last_name>Baldwin</last_name>
 *       </full_name>
 *       <party>D</party>
 *       <state>WI</state>
 *       <bioguide>B001230</bioguide>
 *       <lisid>S354</lisid>
 *       <congresses>
 *         <congress>112</congress>
 *         <congress>119</congress>
 *       </congresses>
 *       <service_dates>
 *         <service_date class="1">
 *           <begin_date congress="112" day="03" month="01" year="2013"/>
 *           <end_date day="" month="" year=""/>  <!-- empty year = currently serving -->
 *         </service_date>
 *       </service_dates>
 *     </senator>
 *   </senators>
 * }}}
 *
 * Key field mappings vs. original spec assumptions:
 *   - LIS ID: `<lisid>` (spec assumed `<lis_member_id>`)
 *   - Bioguide: `<bioguide>` (spec assumed `<bioguide_id>`)
 *   - Names: nested under `<full_name>` (spec assumed flat siblings)
 *   - Current: derived from empty `<end_date year="">` (spec assumed `<is_current>`)
 *   - Congresses: `<congresses><congress>` numbers (spec assumed `<service_dates>`)
 *
 * Resilience: individual `<senator>` entries missing required fields (`lisid`, `bioguide`, `first_name`, `last_name`,
 * `party`, `state`) are skipped silently.
 */
object SenatorXmlParser {

  /**
   * Parse a `senator-lookup.xml` document into a list of DTOs. Entries missing required fields are skipped silently.
   */
  def parse(elem: Elem): List[SenatorLookupXmlDTO] =
    (elem \\ "senator").toList.flatMap(parseSenator)

  private def parseSenator(node: Node): Option[SenatorLookupXmlDTO] = {
    val lisId      = textOption(node \ "lisid")
    val bioguideId = textOption(node \ "bioguide")
    val firstName  = textOption((node \ "full_name") \ "first_name")
    val lastName   = textOption((node \ "full_name") \ "last_name")
    val party      = textOption(node \ "party")
    val state      = textOption(node \ "state")

    val senateClass  = latestServiceClass(node \ "service_dates" \ "service_date")
    val isCurrent    = isCurrentlyServing(node \ "service_dates" \ "service_date")
    val serviceDates = parseCongresses(node \ "congresses" \ "congress")

    for {
      lis <- lisId
      bio <- bioguideId
      fn  <- firstName
      ln  <- lastName
      p   <- party
      st  <- state
    } yield SenatorLookupXmlDTO(
      lisId = lis,
      bioguideId = bio,
      firstName = fn,
      lastName = ln,
      party = p,
      state = st,
      senateClass = senateClass,
      serviceDates = serviceDates,
      isCurrent = isCurrent,
    )
  }

  /**
   * A senator is currently serving if any of their `<service_date>` entries has an `<end_date>` with an empty `year`
   * attribute (senate.gov convention for open-ended service).
   */
  private def isCurrentlyServing(serviceDates: NodeSeq): Boolean =
    serviceDates.exists { sd =>
      val endYear = sd \ "end_date" \@ "year"
      endYear.trim.isEmpty
    }

  /**
   * Return the senate class (1, 2, or 3) from the most recently started `<service_date class="N">` entry. Class is an
   * attribute on `<service_date>`, not a child element.
   */
  private def latestServiceClass(serviceDates: NodeSeq): Option[Int] =
    serviceDates.toList
      .flatMap(sd => Try((sd \@ "class").trim.toInt).toOption)
      .lastOption

  /**
   * Map `<congress>N</congress>` elements to [[ServicePeriodDTO]]s (congress number only; start/end dates are not
   * needed for lookback-window filtering).
   */
  private def parseCongresses(congresses: NodeSeq): List[ServicePeriodDTO] =
    congresses.toList.flatMap { cn =>
      Try(cn.text.trim.toInt).toOption.map(n => ServicePeriodDTO(congress = Some(n), startDate = None, endDate = None))
    }

  private def textOption(ns: NodeSeq): Option[String] = {
    val raw = ns.text.trim
    if (raw.isEmpty) None else Some(raw)
  }

}
